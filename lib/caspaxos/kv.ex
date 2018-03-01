defmodule Caspaxos.KV do
  alias Caspaxos.{Proposer, Acceptor}
  require Logger

  defmodule Store do
    defstruct [:f]
  end


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

    %Store{f: f}
  end

  def put(%Store{f: f}, key, val) do
    Logger.debug(inspect {key, val})
    result = Proposer.closest(key)
    |> Proposer.submit(fn 
      nil ->   {:ok, Map.put(%{}, key, val)}
      state -> {:ok, Map.put(state, key, val)}
    end, f)
    Logger.debug(inspect {:result, result})
    result
  end

  def get(%Store{f: f}, key) do
    Logger.debug(key)

    result = Proposer.closest(key)
    |> Proposer.submit(fn
      nil ->   {nil, nil}
      state -> {Map.get(state, key), state}
    end, f) 
    Logger.debug(inspect {:result, result})
    result
  end

  def toy() do
    store = start(2)
    put(store, :foo, :bar)
    # put(:foo, :wat)
    put(store, :biz, :buzz)
    get(store, :foo)
  end
end