defmodule Caspaxos.Proposer do
  use GenServer
  alias Caspaxos.Acceptor

  def group_init do
    :pg2.create(__MODULE__)
  end

  def start_link(id) do
    :pg2.create(__MODULE__)
    GenServer.start_link(__MODULE__, [id])
  end

  def init([id]) do
    :pg2.join(__MODULE__, self())
    {:ok, {id, 0}}
  end

  def handle_call({:submit, fchange}, _, {id, bnum}) do
    ballot = {id, bnum + 1}
    
    {state, _} = Acceptor.prepare(ballot)
    |> Enum.reduce({nil, 0}, fn
      {:ok, {value, ballot}}, {_, acc_ballot} when ballot > acc_ballot ->
        {value, ballot}
      {:ok, {_, _} = result}, {nil, 0} -> result
      _, acc -> acc
    end)

    {result, changed_state} = fchange.(state)
    IO.inspect {:state_is, state, "-->", changed_state}

    case Acceptor.accept(ballot, changed_state) do
      :ok -> 
        # Cool
        {:reply, {:ok, result}, ballot}
      {:error, {:conflict, {_, ballot_num}}} ->
        # Fast forward
        {:reply, {:error, :conflict}, {id, ballot_num}}
    end
  end

  defp get() do 
    case :pg2.get_members(__MODULE__) do
      [] -> {:error, :no_proposer}
      members -> {:ok, Enum.random(members)}
    end
  end

  def submit(fchange) do
    with {:ok, proposer_pid} <- get() do
      GenServer.call(proposer_pid, {:submit, fchange})
    end
  end
end