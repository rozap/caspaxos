defmodule Caspaxos.Proposer do
  use GenServer
  alias Caspaxos.Acceptor
  require Logger

  def group_init do
    :pg2.create(__MODULE__)
  end

  def start_link(id) do
    :pg2.create(__MODULE__)
    GenServer.start_link(__MODULE__, [id])
  end

  def init([id]) do
    :pg2.join(__MODULE__, self())
    {:ok, {0, id}}
  end

  def handle_call({:submit, fchange, f}, _, {bnum, id}) do
    ballot = {bnum + 1, id}

    Logger.debug("Prepare with ballot #{inspect ballot}")
    
    {state, _} = Acceptor.prepare(ballot, f)
    |> Enum.reduce({nil, 0}, fn
      {:ok, {value, ballot}}, {_, acc_ballot} when ballot > acc_ballot ->
        {value, ballot}
      {:ok, {_, _} = result}, {nil, 0} -> result
      _, acc -> acc
    end)

    {result, changed_state} = fchange.(state)

    case Acceptor.accept(ballot, changed_state, f) do
      :ok -> 
        # Cool
        {:reply, {:ok, result}, ballot}
      {:error, {:conflict, {ballot_num, _}}} ->
        # Fast forward
        new_ballot = {ballot_num, id}
        Logger.debug("Got conflict, fastfwd to #{inspect new_ballot}")
        {:reply, {:error, :conflict}, new_ballot}
    end
  end

  # The other nodes we didn't wait for are sending us stuff
  def handle_info({:result, _, _}, state), do: {:noreply, state}
  # yea this should be stateful - probably pg2 isn't ideal here
  # also this lookup map is super goofy but HashRing only does binaries
  def closest(key) do 
    lookup = :pg2.get_members(__MODULE__) 
    |> Enum.map(fn pid -> {:erlang.term_to_binary(pid), pid} end)
    |> Enum.into(%{})

    idx = Enum.reduce(lookup, HashRing.new, fn {bin, _}, ring -> 
      {:ok, ring} = HashRing.add_node(ring, bin)
      ring
    end)
    |> HashRing.find_node(key)

    Map.get(lookup, idx)
  end

  defp do_submit(_, _, _, max_attempts, max_attempts) do
    {:error, :reached_max_submit_attempts}
  end
  defp do_submit(pid, fchange, f, attempt, max_attempts) do
    case GenServer.call(pid, {:submit, fchange, f}) do
      {:error, :conflict} -> 
        do_submit(pid, fchange, f, attempt + 1, max_attempts)
      res -> 
        res
    end
  end

  def submit(proposer_pid, fchange, f) do
    do_submit(proposer_pid, fchange, f, 0, 5)
  end
end