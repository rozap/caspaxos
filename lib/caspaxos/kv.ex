defmodule Caspaxos.KV do
  alias Caspaxos.{Proposer, Acceptor}

  def start(f) do
    Proposer.group_init()
    Acceptor.group_init()

    count = (2 * f) + 1

    Proposer.start_link(0)
    Proposer.start_link(1)
    Proposer.start_link(2)


    Enum.each(0..count, fn _ ->
      Acceptor.start_link()
    end)

    :ok
  end

  def put(key, val) do
    Proposer.submit(fn 
      nil ->   {:ok, Map.put(%{}, key, val)}
      state -> {:ok, Map.put(state, key, val)}
    end)
  end

  def get(key) do
    Proposer.submit(fn
      nil ->   {nil, nil}
      state -> {Map.get(state, key), state}
    end)
  end

  def toy() do
    start(3)
    put(:foo, :bar) |> IO.inspect
    put(:biz, :buzz) |> IO.inspect
    get(:foo) |> IO.inspect
  end
end