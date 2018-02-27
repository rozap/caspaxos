defmodule CaspaxosTest do
  use ExUnit.Case
  doctest Caspaxos

  test "greets the world" do
    assert Caspaxos.hello() == :world
  end
end
