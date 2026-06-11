defmodule SynSupervisor.Test.Support.SlowWorker do
  @moduledoc false
  use GenServer

  def start_link({sleep_ms, init_arg}) do
    :timer.sleep(sleep_ms)
    GenServer.start_link(__MODULE__, init_arg)
  end

  def child_spec(sleep_ms, init_arg) do
    default = %{
      id: {__MODULE__, init_arg},
      start: {__MODULE__, :start_link, [{sleep_ms, init_arg}]}
    }

    Supervisor.child_spec(default, [])
  end

  @impl true
  def init(init_arg) do
    {:ok, init_arg}
  end
end
