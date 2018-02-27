
defmodule Caspaxos.Acceptor do
  use GenServer

  def group_init do
    :pg2.create(__MODULE__)
  end

  def start_link do
    GenServer.start_link(__MODULE__, [])
  end

  def init(_) do
    :pg2.join(__MODULE__, self())
    {:ok, {nil, nil, nil}}
  end

  defp is_gt?(nil, _), do: false
  defp is_gt?({lballot, lid}, {rballot, rid}) do
    lballot > rballot or (lballot == rballot && lid > rid)
  end

  def handle_call({:prepare, ballot}, _, {_, _, nil}) do
    new_state = {ballot, nil, nil}
    {:reply, {:ok, nil}, new_state}
  end
  def handle_call({:prepare, ballot}, _, {promise, max_ballot, value} = state) do
    cond do
      is_gt?(promise, ballot) -> 
        {:reply, {:error, {:conflict, promise}}, state}
      is_gt?(max_ballot, ballot) -> 
        {:reply, {:error, {:conflict, max_ballot}}, state}
      true ->
        new_state = {ballot, max_ballot, value}
        {:reply, {:ok, {value, max_ballot}}, new_state}
    end
  end

  def handle_call({:accept, ballot, result}, _, {promise, max_ballot, _value} = state) do
    cond do
      is_gt?(promise, ballot) -> 
        {:reply, {:error, {:conflict, promise}}, state}
      is_gt?(max_ballot, ballot) -> 
        {:reply, {:error, {:conflict, max_ballot}}, state}
      true ->
        new_state = {nil, ballot, result}
        {:reply, :ok, new_state}
    end
  end

  defp members(), do: :pg2.get_members(__MODULE__)

  def prepare(ballot) do
    members()
    |> Enum.map(fn pid ->
      GenServer.call(pid, {:prepare, ballot})
    end)    
  end

  def accept(ballot, result) do
    members()
    |> Enum.map(fn pid ->
      GenServer.call(pid, {:accept, ballot, result})
    end)
    |> Enum.reduce(nil, fn
      :ok, _ -> :ok
      {:error, _} = e, _ -> e
    end)
  end

end
