defmodule Caspaxos.Acceptor do
  use GenServer
  require Logger

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
        Logger.debug(inspect {:prepare, value})
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
        Logger.debug(inspect {:accept, result})
        {:reply, :ok, new_state}
    end
  end

  defp members(), do: :pg2.get_members(__MODULE__)

  defp f_plus_1_call(msg, f) do
    ref = make_ref()
    owner = self()

    Enum.each(members(), fn pid ->
      spawn_link(fn ->
        result = GenServer.call(pid, msg)
        send(owner, {:result, result, ref})
      end)
    end)

    gather_replies(ref, 0, f + 1)
  end

  defp gather_replies(_, f_plus_1, f_plus_1), do: []
  defp gather_replies(ref, current, target) do
    receive do
      {:result, result, ^ref} -> 
        [result | gather_replies(ref, current + 1, target)]
    after
      2000 ->
        raise RuntimeError, message: "Timeout: failed to gather_replies #{target} replies"
    end
  end

  def prepare(ballot, f) do
    f_plus_1_call({:prepare, ballot}, f)
  end

  def accept(ballot, result, f) do
    f_plus_1_call({:accept, ballot, result}, f)
    |> Enum.reduce(nil, fn
      :ok, _ -> :ok
      {:error, _} = e, _ -> e
    end)
  end

end
