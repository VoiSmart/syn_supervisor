defmodule SynSupervisor do
  @moduledoc ~S"""
  A distributed, dynamic supervisor.
  Heavily derived from Elixir DynamicSupervisor.

  A supervisor optimized to only start children dynamically.

  The `Supervisor` module was designed to handle mostly static children
  that are started in the given order when the supervisor starts. A
  `SynSupervisor` starts with no children. Instead, children are
  started on demand via `start_child/2` and there is no ordering between
  children. This allows the `SynSupervisor` to hold millions of
  children by using efficient data structures and to execute certain
  operations, such as shutting down, concurrently.

  ## Examples

  A dynamic supervisor is started with no children and often a name:

      children = [
        {SynSupervisor, name: MyApp.SynSupervisor, strategy: :one_for_one}
      ]

      Supervisor.start_link(children, strategy: :one_for_one)

  The options given in the child specification are documented in `start_link/1`.

  Once the dynamic supervisor is running, we can use it to start children
  on demand. Given this sample `GenServer`:

      defmodule Counter do
        use GenServer

        def start_link(initial) do
          GenServer.start_link(__MODULE__, initial)
        end

        def inc(pid) do
          GenServer.call(pid, :inc)
        end

        def init(initial) do
          {:ok, initial}
        end

        def handle_call(:inc, _, count) do
          {:reply, count, count + 1}
        end
      end

  We can use `start_child/2` with a child specification to start a `Counter`
  server:

      {:ok, counter1} = SynSupervisor.start_child(MyApp.SynSupervisor, {Counter, 0})
      Counter.inc(counter1)
      #=> 0

      {:ok, counter2} = SynSupervisor.start_child(MyApp.SynSupervisor, {Counter, 10})
      Counter.inc(counter2)
      #=> 10

      SynSupervisor.count_children(MyApp.SynSupervisor)
      #=> %{active: 2, specs: 2, supervisors: 0, workers: 2}

  ## Scalability and partitioning

  The `SynSupervisor` is a single process responsible for starting
  other processes. In some applications, the `SynSupervisor` may
  become a bottleneck. To address this, you can start multiple instances
  of the `SynSupervisor` and then pick a "random" instance to start
  the child on.

  Instead of:

      children = [
        {SynSupervisor, name: MyApp.SynSupervisor}
      ]

  and:

      SynSupervisor.start_child(MyApp.SynSupervisor, {Counter, 0})

  You can do this:

      children = [
        {PartitionSupervisor,
         child_spec: SynSupervisor,
         name: MyApp.SynSupervisors}
      ]

  and then:

      SynSupervisor.start_child(
        {:via, PartitionSupervisor, {MyApp.SynSupervisors, self()}},
        {Counter, 0}
      )

  In the code above, we start a partition supervisor that will by default
  start a dynamic supervisor for each core in your machine. Then, instead
  of calling the `SynSupervisor` by name, you call it through the
  partition supervisor, using `self()` as the routing key. This means each
  process will be assigned one of the existing dynamic supervisors.
  Read the `PartitionSupervisor` docs for more information.

  ## Module-based supervisors

  Similar to `Supervisor`, dynamic supervisors also support module-based
  supervisors.

      defmodule MyApp.SynSupervisor do
        # Automatically defines child_spec/1
        use SynSupervisor

        def start_link(init_arg) do
          SynSupervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
        end

        @impl true
        def init(_init_arg) do
          SynSupervisor.init(strategy: :one_for_one)
        end
      end

  See the `Supervisor` docs for a discussion of when you may want to use
  module-based supervisors. A `@doc` annotation immediately preceding
  `use SynSupervisor` will be attached to the generated `child_spec/1`
  function.

  > #### `use SynSupervisor` {: .info}
  >
  > When you `use SynSupervisor`, the `SynSupervisor` module will
  > set `@behaviour SynSupervisor` and define a `child_spec/1`
  > function, so your module can be used as a child in a supervision tree.

  ## Name registration

  A supervisor is bound to the same name registration rules as a `GenServer`.
  Read more about these rules in the documentation for `GenServer`.

  """

  alias SynSupervisor.Distribution
  alias SynSupervisor.Distribution.Child

  @behaviour GenServer

  @default_sync_interval 5 * 60_000
  @sync_delay_on_topology_change 5_000

  @doc """
  Callback invoked to start the supervisor and during hot code upgrades.

  Developers typically invoke `SynSupervisor.init/1` at the end of
  their init callback to return the proper supervision flags.
  """
  @callback init(init_arg :: term) :: {:ok, sup_flags()} | :ignore

  @typedoc "The supervisor flags returned on init"
  @type sup_flags() :: %{
          strategy: strategy(),
          intensity: non_neg_integer(),
          period: pos_integer(),
          max_children: non_neg_integer() | :infinity,
          extra_arguments: [term()],
          sync_interval: non_neg_integer(),
          sync_delay_on_topology_change: non_neg_integer(),
          scope: atom()
        }

  @typedoc "Options given to `start_link` functions"
  @type option :: GenServer.option()

  @typedoc "Options given to `start_link` and `init/1` functions"
  @type init_option ::
          {:strategy, strategy()}
          | {:max_restarts, non_neg_integer()}
          | {:max_seconds, pos_integer()}
          | {:max_children, non_neg_integer() | :infinity}
          | {:extra_arguments, [term()]}
          | {:scope, atom()}
          | {:sync_interval, non_neg_integer()}
          | {:sync_delay_on_topology_change, non_neg_integer()}

  @typedoc "Supported strategies"
  @type strategy :: :one_for_one

  @typedoc "Return values of `start_child` functions"
  @type on_start_child ::
          {:ok, pid}
          | {:ok, pid, info :: term}
          | :ignore
          | {:error, {:already_started, pid} | :max_children | term}

  # In this struct, `args` refers to the arguments passed to init/1 (the `init_arg`).
  defstruct [
    :args,
    :extra_arguments,
    :mod,
    :name,
    :strategy,
    :max_children,
    :max_restarts,
    :max_seconds,
    :scope,
    :sync_interval,
    :sync_delay_on_topology_change,
    sync_interval_timer_ref: nil,
    children: %{},
    children_by_child_id: %{},
    restarts: []
  ]

  @doc """
  Returns a specification to start a dynamic supervisor under a supervisor.

  It accepts the same options as `start_link/1`.

  See `Supervisor` for more information about child specifications.
  """
  def child_spec(options) when is_list(options) do
    id =
      case Keyword.get(options, :name, SynSupervisor) do
        name when is_atom(name) -> name
        {:global, name} -> name
        {:via, _module, name} -> name
      end

    %{
      id: id,
      start: {SynSupervisor, :start_link, [options]},
      type: :supervisor
    }
  end

  @doc false
  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do
      @behaviour SynSupervisor
      unless Module.has_attribute?(__MODULE__, :doc) do
        @doc """
        Returns a specification to start this module under a supervisor.

        See `Supervisor`.
        """
      end

      def child_spec(arg) do
        default = %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [arg]},
          type: :supervisor
        }

        Supervisor.child_spec(default, unquote(Macro.escape(opts)))
      end

      defoverridable child_spec: 1
    end
  end

  @doc """
  Starts a supervisor with the given options.

  This function is typically not invoked directly, instead it is invoked
  when using a `SynSupervisor` as a child of another supervisor:

      children = [
        {SynSupervisor, name: MySupervisor}
      ]

  If the supervisor is successfully spawned, this function returns
  `{:ok, pid}`, where `pid` is the PID of the supervisor. If the supervisor
  is given a name and a process with the specified name already exists,
  the function returns `{:error, {:already_started, pid}}`, where `pid`
  is the PID of that process.

  Note that a supervisor started with this function is linked to the parent
  process and exits not only on crashes but also if the parent process exits
  with `:normal` reason.

  ## Options

    * `:scope` - scope (`:pg` scope) under which other SynSupervisor instances
      are grouped in the cluster. For different SynSupervisor clusters, use
      different scopes.

    * `:sync_interval` - interval in milliseconds, how often the supervisor
      should synchronize its local state with the cluster by processing.
      Optional, defaults to 5 minutes

    * `:sync_delay_on_topology_change` - interval in milliseconds, how often
      the supervisor should synchronize its local state with the cluster after a
      topology change is detected.
      Optional, defaults to 5 seconds

    * `:name` - registers the supervisor under the given name.
      The supported values are described under the "Name registration"
      section in the `GenServer` module docs.

    * `:strategy` - the restart strategy option. The only supported
      value is `:one_for_one` which means that no other child is
      terminated if a child process terminates. You can learn more
      about strategies in the `Supervisor` module docs.

    * `:max_restarts` - the maximum number of restarts allowed in
      a time frame. Defaults to `3`.

    * `:max_seconds` - the time frame in which `:max_restarts` applies.
      Defaults to `5`.

    * `:max_children` - the maximum amount of children to be running
      under this supervisor at the same time. When `:max_children` is
      exceeded, `start_child/2` returns `{:error, :max_children}`. Defaults
      to `:infinity`.

    * `:extra_arguments` - arguments that are prepended to the arguments
      specified in the child spec given to `start_child/2`. Defaults to
      an empty list.

  """
  @spec start_link([option | init_option]) :: Supervisor.on_start()
  def start_link(options) when is_list(options) do
    keys = [
      :extra_arguments,
      :max_children,
      :max_seconds,
      :max_restarts,
      :strategy,
      :scope,
      :sync_interval,
      :sync_delay_on_topology_change
    ]

    {sup_opts, start_opts} = Keyword.split(options, keys)
    start_link(Supervisor.Default, init(sup_opts), start_opts)
  end

  @doc """
  Starts a module-based supervisor process with the given `module` and `init_arg`.

  To start the supervisor, the `c:init/1` callback will be invoked in the given
  `module`, with `init_arg` as its argument. The `c:init/1` callback must return a
  supervisor specification which can be created with the help of the `init/1`
  function.

  If the `c:init/1` callback returns `:ignore`, this function returns
  `:ignore` as well and the supervisor terminates with reason `:normal`.
  If it fails or returns an incorrect value, this function returns
  `{:error, term}` where `term` is a term with information about the
  error, and the supervisor terminates with reason `term`.

  The `:name` option can also be given in order to register a supervisor
  name, the supported values are described in the "Name registration"
  section in the `GenServer` module docs.

  If the supervisor is successfully spawned, this function returns
  `{:ok, pid}`, where `pid` is the PID of the supervisor. If the supervisor
  is given a name and a process with the specified name already exists,
  the function returns `{:error, {:already_started, pid}}`, where `pid`
  is the PID of that process.

  Note that a supervisor started with this function is linked to the parent
  process and exits not only on crashes but also if the parent process exits
  with `:normal` reason.
  """
  @spec start_link(module, term, [option]) :: Supervisor.on_start()
  def start_link(module, init_arg, opts \\ []) do
    GenServer.start_link(__MODULE__, {module, init_arg, opts[:name]}, opts)
  end

  @doc """
  Dynamically adds a child specification to `supervisor` and starts that child.

  `child_spec` should be a valid child specification as detailed in the
  "Child specification" section of the documentation for `Supervisor`. The child
  process will be started as defined in the child specification. Note that while
  the `:id` field is still required in the spec, the value is ignored and
  therefore does not need to be unique.

  If the child process start function returns `{:ok, child}` or `{:ok, child,
  info}`, then child specification and PID are added to the supervisor and
  this function returns the same value.

  If the child process start function returns `:ignore`, then no child is added
  to the supervision tree and this function returns `:ignore` too.

  If the child process start function returns an error tuple or an erroneous
  value, or if it fails, the child specification is discarded and this function
  returns `{:error, error}` where `error` is the error or erroneous value
  returned from child process start function, or failure reason if it fails.

  If the supervisor already has N children in a way that N exceeds the amount
  of `:max_children` set on the supervisor initialization (see `init/1`), then
  this function returns `{:error, :max_children}`.
  """
  @spec start_child(
          Supervisor.supervisor(),
          Supervisor.child_spec()
          | {module, term}
          | module
          | (old_erlang_child_spec :: :supervisor.child_spec())
        ) ::
          on_start_child()
  def start_child(supervisor, {_, _, _, _, _, _} = child_spec) do
    validate_and_start_child(supervisor, child_spec)
  end

  def start_child(supervisor, child_spec) do
    validate_and_start_child(supervisor, Supervisor.child_spec(child_spec, []))
  end

  defp validate_and_start_child(supervisor, child_spec) do
    case validate_child(child_spec) do
      {:ok, child} ->
        call(supervisor, {:start_child, child})

      error ->
        {:error, error}
    end
  end

  defp validate_child(%{id: id, start: {mod, _, _} = start} = child) do
    restart = Map.get(child, :restart, :permanent)
    type = Map.get(child, :type, :worker)
    modules = Map.get(child, :modules, [mod])
    significant = Map.get(child, :significant, false)

    shutdown =
      case type do
        :worker -> Map.get(child, :shutdown, 5_000)
        :supervisor -> Map.get(child, :shutdown, :infinity)
      end

    validate_child(start, restart, shutdown, type, modules, significant, id)
  end

  defp validate_child({id, start, restart, shutdown, type, modules}) do
    validate_child(start, restart, shutdown, type, modules, false, id)
  end

  defp validate_child(other) do
    {:invalid_child_spec, other}
  end

  defp validate_child(start, restart, shutdown, type, modules, significant, id) do
    with :ok <- validate_start(start),
         :ok <- validate_restart(restart),
         :ok <- validate_shutdown(shutdown),
         :ok <- validate_type(type),
         :ok <- validate_modules(modules),
         :ok <- validate_significant(significant),
         :ok <- validate_id(id) do
      {:ok, {id, start, restart, shutdown, type, modules}}
    end
  end

  defp validate_start({m, f, args}) when is_atom(m) and is_atom(f) and is_list(args), do: :ok
  defp validate_start(mfa), do: {:invalid_mfa, mfa}

  defp validate_type(type) when type in [:supervisor, :worker], do: :ok
  defp validate_type(type), do: {:invalid_child_type, type}

  defp validate_restart(restart) when restart in [:permanent, :temporary, :transient], do: :ok
  defp validate_restart(restart), do: {:invalid_restart_type, restart}

  defp validate_shutdown(shutdown) when is_integer(shutdown) and shutdown >= 0, do: :ok
  defp validate_shutdown(shutdown) when shutdown in [:infinity, :brutal_kill], do: :ok
  defp validate_shutdown(shutdown), do: {:invalid_shutdown, shutdown}

  defp validate_significant(false), do: :ok
  defp validate_significant(significant), do: {:invalid_significant, significant}

  defp validate_modules(:dynamic), do: :ok

  defp validate_modules(mods) do
    if is_list(mods) and Enum.all?(mods, &is_atom/1) do
      :ok
    else
      {:invalid_modules, mods}
    end
  end

  defp validate_id(_id) do
    # just like in otp's supervisor, we just validate the id is there, not its
    # value
    :ok
  end

  @doc """
  Terminates the given child identified by `pid` or `child_id`.

  If successful, this function returns `:ok`. If there is no process with
  the given PID, this function returns `{:error, :not_found}`.
  """
  @spec terminate_child(Supervisor.supervisor(), pid | term) :: :ok | {:error, :not_found}
  def terminate_child(supervisor, pid) when is_pid(pid) do
    call(supervisor, {:terminate_child, pid})
  end

  def terminate_child(supervisor, child_id) do
    call(supervisor, {:terminate_child, child_id})
  end

  @doc """
  Returns a list with information about all children.

  Note that calling this function when supervising a large number
  of children under low memory conditions can cause an out of memory
  exception.

  This function returns a list of tuples containing:

    * `id` - the `:id` as defined in the child specification

    * `child` - the PID of the corresponding child process or the
      atom `:restarting` if the process is about to be restarted

    * `type` - `:worker` or `:supervisor` as defined in the child
      specification

    * `modules` - as defined in the child specification

  """
  @spec which_children(Supervisor.supervisor()) :: [
          # module() | :dynamic here because :supervisor.modules() is not exported
          {:undefined, pid | :restarting, :worker | :supervisor, [module()] | :dynamic}
        ]
  def which_children(supervisor) do
    call(supervisor, :which_children)
  end

  @spec which_children(Supervisor.supervisor(), :local | :global) :: [
          # module() | :dynamic here because :supervisor.modules() is not exported
          {:undefined, pid | :restarting, :worker | :supervisor, [module()] | :dynamic}
        ]
  def which_children(supervisor, :local) do
    call(supervisor, :which_children)
  end

  def which_children(supervisor, :global) do
    call(supervisor, {:which_children, :global})
  end

  @doc """
  Returns the pid of the child with the given `child_id`.

  If no matchin child is found, returns `nil`.
  """
  @spec whereis(supervisor :: Supervisor.supervisor(), child_id :: any()) :: nil | pid()
  def whereis(supervisor, child_id) do
    call(supervisor, {:whereis, child_id})
  end

  @doc """
  Returns a map containing count values for the supervisor.

  The map contains the following keys:

    * `:specs` - the number of children processes

    * `:active` - the count of all actively running child processes managed by
      this supervisor

    * `:supervisors` - the count of all supervisors whether or not the child
      process is still alive

    * `:workers` - the count of all workers, whether or not the child process
      is still alive

  """
  @spec count_children(Supervisor.supervisor()) :: %{
          specs: non_neg_integer,
          active: non_neg_integer,
          supervisors: non_neg_integer,
          workers: non_neg_integer
        }
  def count_children(supervisor) do
    call(supervisor, :count_children) |> :maps.from_list()
  end

  @spec count_children(Supervisor.supervisor(), :local | :global) :: %{
          specs: non_neg_integer,
          active: non_neg_integer,
          supervisors: non_neg_integer,
          workers: non_neg_integer
        }
  def count_children(supervisor, :local) do
    call(supervisor, :count_children) |> :maps.from_list()
  end

  def count_children(supervisor, :global) do
    call(supervisor, {:count_children, :global}) |> :maps.from_list()
  end

  @doc """
  Synchronously stops the given supervisor with the given `reason`.

  It returns `:ok` if the supervisor terminates with the given
  reason. If it terminates with another reason, the call exits.

  This function keeps OTP semantics regarding error reporting.
  If the reason is any other than `:normal`, `:shutdown` or
  `{:shutdown, _}`, an error report is logged.
  """
  @spec stop(Supervisor.supervisor(), reason :: term, timeout) :: :ok
  def stop(supervisor, reason \\ :normal, timeout \\ :infinity) do
    GenServer.stop(supervisor, reason, timeout)
  end

  @doc """
  Receives a set of `options` that initializes a dynamic supervisor.

  This is typically invoked at the end of the `c:init/1` callback of
  module-based supervisors. See the "Module-based supervisors" section
  in the module documentation for more information.

  It accepts the same `options` as `start_link/1` (except for `:name`)
  and it returns a tuple containing the supervisor options.

  ## Examples

      def init(_arg) do
        SynSupervisor.init(max_children: 1000)
      end

  """
  @spec init([init_option]) :: {:ok, sup_flags()}
  def init(options) when is_list(options) do
    strategy = Keyword.get(options, :strategy, :one_for_one)
    intensity = Keyword.get(options, :max_restarts, 3)
    period = Keyword.get(options, :max_seconds, 5)
    max_children = Keyword.get(options, :max_children, :infinity)
    extra_arguments = Keyword.get(options, :extra_arguments, [])
    scope = Keyword.get(options, :scope)

    sync_interval = Keyword.get(options, :sync_interval, @default_sync_interval)

    sync_delay_on_topology_change =
      Keyword.get(options, :sync_delay_on_topology_change, @sync_delay_on_topology_change)

    if scope == nil do
      raise ArgumentError, message: "Missing mandatory scope value"
    end

    flags = %{
      strategy: strategy,
      intensity: intensity,
      period: period,
      max_children: max_children,
      scope: scope,
      sync_interval: sync_interval,
      sync_delay_on_topology_change: sync_delay_on_topology_change,
      extra_arguments: extra_arguments
    }

    {:ok, flags}
  end

  ## Callbacks

  @impl true
  def init({mod, init_arg, name}) do
    Process.put(:"$initial_call", {:supervisor, mod, 1})
    Process.flag(:trap_exit, true)

    case mod.init(init_arg) do
      {:ok, flags} when is_map(flags) ->
        name =
          cond do
            is_nil(name) -> {self(), mod}
            is_atom(name) -> {:local, name}
            is_tuple(name) -> name
          end

        state = %SynSupervisor{mod: mod, args: init_arg, name: name}

        case init(state, flags) do
          {:ok, state} ->
            Distribution.start_and_join(state.scope)
            :net_kernel.monitor_nodes(true, node_type: :visible)
            {:ok, state}

          {:error, reason} ->
            {:stop, {:supervisor_data, reason}}
        end

      :ignore ->
        :ignore

      other ->
        {:stop, {:bad_return, {mod, :init, other}}}
    end
  end

  defp init(state, flags) do
    extra_arguments = Map.get(flags, :extra_arguments, [])
    max_children = Map.get(flags, :max_children, :infinity)
    max_restarts = Map.get(flags, :intensity, 1)
    max_seconds = Map.get(flags, :period, 5)
    strategy = Map.get(flags, :strategy, :one_for_one)
    auto_shutdown = Map.get(flags, :auto_shutdown, :never)
    scope = Map.get(flags, :scope)

    sync_interval = Map.get(flags, :sync_interval, @default_sync_interval)

    sync_delay_on_topology_change =
      Map.get(flags, :sync_delay_on_topology_change, @sync_delay_on_topology_change)

    with :ok <- validate_strategy(strategy),
         :ok <- validate_restarts(max_restarts),
         :ok <- validate_seconds(max_seconds),
         :ok <- validate_dynamic(max_children),
         :ok <- validate_extra_arguments(extra_arguments),
         :ok <- validate_auto_shutdown(auto_shutdown),
         :ok <- validate_scope(scope),
         :ok <- validate_sync_interval(sync_interval),
         :ok <- validate_sync_interval(sync_delay_on_topology_change) do
      # schedule the first sync interval to be short, since we want immediate
      # sync, then use the long period, since we have the nodeup/nodedown
      # events
      state = reschedule_sync_interval_timer(state, sync_delay_on_topology_change)

      {:ok,
       %{
         state
         | extra_arguments: extra_arguments,
           max_children: max_children,
           max_restarts: max_restarts,
           max_seconds: max_seconds,
           strategy: strategy,
           scope: scope,
           sync_interval: sync_interval,
           sync_delay_on_topology_change: sync_delay_on_topology_change
       }}
    end
  end

  defp validate_strategy(strategy) when strategy in [:one_for_one], do: :ok
  defp validate_strategy(strategy), do: {:error, {:invalid_strategy, strategy}}

  defp validate_restarts(restart) when is_integer(restart) and restart >= 0, do: :ok
  defp validate_restarts(restart), do: {:error, {:invalid_intensity, restart}}

  defp validate_seconds(seconds) when is_integer(seconds) and seconds > 0, do: :ok
  defp validate_seconds(seconds), do: {:error, {:invalid_period, seconds}}

  defp validate_dynamic(:infinity), do: :ok
  defp validate_dynamic(dynamic) when is_integer(dynamic) and dynamic >= 0, do: :ok
  defp validate_dynamic(dynamic), do: {:error, {:invalid_max_children, dynamic}}

  defp validate_extra_arguments(list) when is_list(list), do: :ok
  defp validate_extra_arguments(extra), do: {:error, {:invalid_extra_arguments, extra}}

  defp validate_auto_shutdown(auto_shutdown) when auto_shutdown in [:never], do: :ok

  defp validate_auto_shutdown(auto_shutdown),
    do: {:error, {:invalid_auto_shutdown, auto_shutdown}}

  defp validate_scope(scope) when is_atom(scope), do: :ok
  defp validate_scope(scope), do: {:error, {:invalid_scope, scope}}

  defp validate_sync_interval(interval) when is_integer(interval) and interval > 0, do: :ok
  defp validate_sync_interval(interval), do: {:error, {:invalid_sync_interval, interval}}

  @impl true
  def handle_call(:which_children, _from, state) do
    %{children: children} = state

    reply =
      for {pid, args} <- children do
        case args do
          {:restarting, {id, _, _, _, type, modules}} ->
            {id, :restarting, type, modules}

          {id, _, _, _, type, modules} ->
            {id, pid, type, modules}
        end
      end

    {:reply, reply, state}
  end

  def handle_call({:which_children, :global}, _from, state) do
    reply =
      state.scope
      |> Distribution.list_children()
      |> Enum.map(fn %Child{} = c ->
        {id, _, _, _, type, modules} = c.spec
        {id, c.pid, type, modules}
      end)

    {:reply, reply, state}
  end

  @impl true
  def handle_call({:whereis, child_id}, _from, state) do
    case Distribution.find_child(state.scope, child_id) do
      {:ok, %Child{pid: pid}} ->
        {:reply, pid, state}

      _ ->
        {:reply, nil, state}
    end
  end

  def handle_call(:count_children, _from, state) do
    %{children: children} = state
    specs = map_size(children)

    {active, workers, supervisors} =
      Enum.reduce(children, {0, 0, 0}, fn
        {_pid, {:restarting, {_, _, _, _, :worker, _}}}, {active, worker, supervisor} ->
          {active, worker + 1, supervisor}

        {_pid, {:restarting, {_, _, _, _, :supervisor, _}}}, {active, worker, supervisor} ->
          {active, worker, supervisor + 1}

        {_pid, {_, _, _, _, :worker, _}}, {active, worker, supervisor} ->
          {active + 1, worker + 1, supervisor}

        {_pid, {_, _, _, _, :supervisor, _}}, {active, worker, supervisor} ->
          {active + 1, worker, supervisor + 1}
      end)

    reply = [specs: specs, active: active, supervisors: supervisors, workers: workers]
    {:reply, reply, state}
  end

  def handle_call({:count_children, :global}, _from, state) do
    children = Distribution.list_children(state.scope)

    {active, workers, supervisors} =
      Enum.reduce(children, {0, 0, 0}, fn
        %Child{} = c, {active, worker, supervisor} ->
          case c.spec do
            {_, _, _, _, :worker, _} -> {active + 1, worker + 1, supervisor}
            {_, _, _, _, :supervisor, _} -> {active + 1, worker, supervisor + 1}
          end
      end)

    reply = [
      specs: Enum.count(children),
      active: active,
      supervisors: supervisors,
      workers: workers
    ]

    {:reply, reply, state}
  end

  def handle_call({:terminate_child, pid_or_child_id}, _from, %{children: _children} = state) do
    case Distribution.find_child(state.scope, pid_or_child_id) do
      {:error, :not_found} ->
        # try local children anyway
        terminate_local_children(pid_or_child_id, state)

      {:ok, %Child{node: node, supervisor_pid: supervisor} = c} ->
        if node == Node.self() do
          Distribution.untrack_spec(state.scope, c.spec)
          terminate_local_children(c.pid, state)
        else
          terminate_remote_children(node, supervisor, c.pid, state)
        end
    end
  end

  def handle_call({:start_child, child}, _from, state) do
    %{children: children, max_children: max_children} = state

    {child_id, _mfa, _restart, _shutdown, _type, _modules} = child

    case {map_size(children), Distribution.find_spec(state.scope, child_id)} do
      {n, {:error, :not_found}} when n < max_children -> handle_start_child(child, state)
      {_n, {:error, :not_found}} -> {:reply, {:error, :max_children}, state}
      {_n, _} -> {:reply, {:error, :already_present}, state}
    end
  end

  defp terminate_local_children(pid, %{children: children} = state) when is_pid(pid) do
    case children do
      %{^pid => info} ->
        :ok = terminate_children(%{pid => info}, state)
        {:reply, :ok, delete_child(pid, state)}

      %{} ->
        {:reply, {:error, :not_found}, state}
    end
  end

  defp terminate_local_children(child_id, %{children: children} = state) do
    children
    |> Enum.find(fn
      {_pid, {^child_id, _, _, _, _, _}} -> true
      _ -> false
    end)
    |> case do
      nil -> {:reply, {:error, :not_found}, state}
      {pid, _} -> terminate_local_children(pid, state)
    end
  end

  defp terminate_remote_children(node, sup, pid, state) do
    case :rpc.call(node, __MODULE__, :terminate_child, [sup, pid], 5_000) do
      {:badrpc, _reason} = err -> {:reply, err, state}
      res -> {:reply, res, state}
    end
  end

  defp handle_start_child({child_id, _mfa, _restart, _shutdown, _type, _modules} = child, state) do
    {assigned_node, assigned_supervisor} = Distribution.member_for_child(state.scope, child_id)

    case assigned_supervisor do
      nil ->
        {:reply, {:error, :remote_supervisor_not_found}, state}

      pid when pid == self() ->
        start_local_child(child, state)

      _ ->
        child
        |> start_remote_child({assigned_node, assigned_supervisor}, state)
        |> maybe_update_ring_and_retry(child, state)
    end
  end

  defp maybe_update_ring_and_retry({:reply, {:badrpc, :nodedown}, state}, child, _state) do
    Distribution.check_members(state.scope)
    handle_start_child(child, state)
  end

  defp maybe_update_ring_and_retry(res, _child, _state) do
    res
  end

  defp start_local_child(
         {id, {m, f, args} = mfa, restart, shutdown, type, modules} = child_spec,
         state
       ) do
    %{extra_arguments: extra} = state

    case reply = start_child(m, f, extra ++ args) do
      {:ok, pid, _} ->
        Distribution.child_join(state.scope, id, Node.self(), self(), pid, child_spec)
        {:reply, reply, save_child(pid, id, mfa, restart, shutdown, type, modules, state)}

      {:ok, pid} ->
        Distribution.child_join(state.scope, id, Node.self(), self(), pid, child_spec)
        {:reply, reply, save_child(pid, id, mfa, restart, shutdown, type, modules, state)}

      _ ->
        {:reply, reply, state}
    end
  end

  defp start_remote_child(child, {node, assigned_sup}, state) do
    {id, start, restart, shutdown, type, modules} = child

    child = %{
      id: id,
      start: start,
      restart: restart,
      shutdown: shutdown,
      type: type,
      modules: modules
    }

    case :rpc.call(node, __MODULE__, :start_child, [assigned_sup, child], 5_000) do
      {:badrpc, _reason} = err ->
        {:reply, err, state}

      res ->
        {:reply, res, state}
    end
  end

  defp start_child(m, f, a) do
    try do
      apply(m, f, a)
    catch
      kind, reason ->
        {:error, exit_reason(kind, reason, __STACKTRACE__)}
    else
      {:ok, pid, extra} when is_pid(pid) -> {:ok, pid, extra}
      {:ok, pid} when is_pid(pid) -> {:ok, pid}
      :ignore -> :ignore
      {:error, _} = error -> error
      other -> {:error, other}
    end
  end

  defp save_child(pid, id, mfa, restart, shutdown, type, modules, state) do
    mfa = mfa_for_restart(mfa, restart)
    child_spec = {id, mfa, restart, shutdown, type, modules}
    state = put_in(state.children_by_child_id[id], child_spec)
    put_in(state.children[pid], child_spec)
  end

  defp mfa_for_restart({m, f, _}, :temporary), do: {m, f, :undefined}
  defp mfa_for_restart(mfa, _), do: mfa

  defp exit_reason(:exit, reason, _), do: reason
  defp exit_reason(:error, reason, stack), do: {reason, stack}
  defp exit_reason(:throw, value, stack), do: {{:nocatch, value}, stack}

  @impl true
  def handle_cast(_msg, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info({:EXIT, pid, reason}, state) do
    case maybe_restart_child(pid, reason, state) do
      {:ok, state} -> {:noreply, state}
      {:shutdown, state} -> {:stop, :shutdown, state}
    end
  end

  def handle_info({:"$gen_restart", pid}, state) do
    %{children: children} = state

    case children do
      %{^pid => restarting_args} ->
        {:restarting, child} = restarting_args

        case restart_child(pid, child, state) do
          {:ok, state} -> {:noreply, state}
          {:shutdown, state} -> {:stop, :shutdown, state}
        end

      # We may hit clause if we send $gen_restart and then
      # someone calls terminate_child, removing the child.
      %{} ->
        {:noreply, state}
    end
  end

  def handle_info(:sync, state) do
    # track all specs: this does nothing if already tracking it, but we need to
    # do this for the existing specs when joining a cluster
    track_spec = fn child_spec, _ ->
      :ok = Distribution.track_spec(state.scope, child_spec, self())
    end

    Distribution.reduce_specs(state.scope, nil, track_spec)

    state = redistribute_processes(state)

    state = reschedule_sync_interval_timer(state)
    {:noreply, state}
  end

  def handle_info({:nodeup, _node, _node_type}, state) do
    state = reschedule_sync_interval_timer(state, state.sync_delay_on_topology_change)
    {:noreply, state}
  end

  def handle_info({:nodedown, _node, _node_type}, state) do
    state = reschedule_sync_interval_timer(state, state.sync_delay_on_topology_change)
    {:noreply, state}
  end

  def handle_info(msg, state) do
    :logger.error(
      %{
        label: {SynSupervisor, :unexpected_msg},
        report: %{
          msg: msg
        }
      },
      %{
        domain: [:otp, :elixir],
        error_logger: %{tag: :error_msg},
        report_cb: &__MODULE__.format_report/1
      }
    )

    {:noreply, state}
  end

  @impl true
  def code_change(_, %{mod: mod, args: init_arg} = state, _) do
    case mod.init(init_arg) do
      {:ok, flags} when is_map(flags) ->
        case init(state, flags) do
          {:ok, state} -> {:ok, state}
          {:error, reason} -> {:error, {:supervisor_data, reason}}
        end

      :ignore ->
        {:ok, state}

      error ->
        error
    end
  end

  @impl true
  def terminate(_, %{children: children} = state) do
    :ok = terminate_children(children, state)
  end

  defp terminate_children(children, state) do
    {pids, times, stacks} = monitor_children(children)
    size = map_size(pids)

    timers =
      Enum.reduce(times, %{}, fn {time, pids}, acc ->
        Map.put(acc, :erlang.start_timer(time, self(), :kill), pids)
      end)

    stacks = wait_children(pids, size, timers, stacks)

    for {pid, {child, reason}} <- stacks do
      report_error(:shutdown_error, reason, pid, child, state)
    end

    :ok
  end

  defp redistribute_processes(state) do
    maybe_redistribute = fn %Child{} = c, state ->
      {assigned_node, assigned_sup} = Distribution.member_for_child(state.scope, c.id)

      state = maybe_stop_child(c, assigned_node, assigned_sup, state)
      maybe_start_child(c, assigned_node, assigned_sup, state)
    end

    maybe_start_missing = fn child_spec, state ->
      {child_id, _, _, _, _, _} = child_spec
      {assigned_node, _assigned_sup} = Distribution.member_for_child(state.scope, child_id)

      if assigned_node == Node.self() and not child_running?(state, child_id) do
        {_, _, state} = start_local_child(child_spec, state)
        state
      else
        state
      end
    end

    Distribution.check_members(state.scope)
    state = Distribution.reduce_child(state.scope, state, maybe_redistribute)
    Distribution.reduce_specs(state.scope, state, maybe_start_missing)
  end

  defp child_running?(%{children_by_child_id: children_by_child_id}, child_id) do
    not is_nil(Map.get(children_by_child_id, child_id))
  end

  defp maybe_stop_child(%Child{} = c, assigned_node, _assigned_sup, state) do
    if assigned_node != c.node and c.node == Node.self() do
      {_, _, state} = terminate_local_children(c.pid, state)
      state
    else
      state
    end
  end

  defp maybe_start_child(%Child{} = c, assigned_node, _assigned_sup, state) do
    if assigned_node == Node.self() and c.node != Node.self() do
      {_, _, state} = start_local_child(c.spec, state)
      state
    else
      state
    end
  end

  defp monitor_children(children) do
    Enum.reduce(children, {%{}, %{}, %{}}, fn
      {_, {:restarting, _}}, acc ->
        acc

      {pid, {_, _, restart, _, _, _} = child}, {pids, times, stacks} ->
        case monitor_child(pid) do
          :ok ->
            times = exit_child(pid, child, times)
            {Map.put(pids, pid, child), times, stacks}

          {:error, :normal} when restart != :permanent ->
            {pids, times, stacks}

          {:error, reason} ->
            {pids, times, Map.put(stacks, pid, {child, reason})}
        end
    end)
  end

  defp monitor_child(pid) do
    ref = Process.monitor(pid)
    Process.unlink(pid)

    receive do
      {:EXIT, ^pid, reason} ->
        receive do
          {:DOWN, ^ref, :process, ^pid, _} -> {:error, reason}
        end
    after
      0 -> :ok
    end
  end

  defp exit_child(pid, {_, _, _, shutdown, _, _}, times) do
    case shutdown do
      :brutal_kill ->
        Process.exit(pid, :kill)
        times

      :infinity ->
        Process.exit(pid, :shutdown)
        times

      time ->
        Process.exit(pid, :shutdown)
        Map.update(times, time, [pid], &[pid | &1])
    end
  end

  defp wait_children(_pids, 0, timers, stacks) do
    for {timer, _} <- timers do
      _ = :erlang.cancel_timer(timer)

      receive do
        {:timeout, ^timer, :kill} -> :ok
      after
        0 -> :ok
      end
    end

    stacks
  end

  defp wait_children(pids, size, timers, stacks) do
    receive do
      {:DOWN, _ref, :process, pid, reason} ->
        case pids do
          %{^pid => child} ->
            stacks = wait_child(pid, child, reason, stacks)
            wait_children(pids, size - 1, timers, stacks)

          %{} ->
            wait_children(pids, size, timers, stacks)
        end

      {:timeout, timer, :kill} ->
        for pid <- Map.fetch!(timers, timer), do: Process.exit(pid, :kill)
        wait_children(pids, size, Map.delete(timers, timer), stacks)
    end
  end

  defp wait_child(pid, {_, _, _, :brutal_kill, _, _} = child, reason, stacks) do
    case reason do
      :killed -> stacks
      _ -> Map.put(stacks, pid, {child, reason})
    end
  end

  defp wait_child(pid, {_, _, restart, _, _, _} = child, reason, stacks) do
    case reason do
      {:shutdown, _} -> stacks
      :shutdown -> stacks
      :normal when restart != :permanent -> stacks
      reason -> Map.put(stacks, pid, {child, reason})
    end
  end

  defp maybe_restart_child(pid, reason, %{children: children} = state) do
    case children do
      %{^pid => {_, _, restart, _, _, _} = child} ->
        maybe_restart_child(restart, reason, pid, child, state)

      %{} ->
        {:ok, state}
    end
  end

  defp maybe_restart_child(:permanent, reason, pid, child, state) do
    report_error(:child_terminated, reason, pid, child, state)
    restart_child(pid, child, state)
  end

  defp maybe_restart_child(_, :normal, pid, child, state) do
    Distribution.untrack_spec(state.scope, child)
    {:ok, delete_child(pid, state)}
  end

  defp maybe_restart_child(_, :shutdown, pid, child, state) do
    Distribution.untrack_spec(state.scope, child)
    {:ok, delete_child(pid, state)}
  end

  defp maybe_restart_child(_, {:shutdown, _}, pid, child, state) do
    Distribution.untrack_spec(state.scope, child)
    {:ok, delete_child(pid, state)}
  end

  defp maybe_restart_child(:transient, reason, pid, child, state) do
    report_error(:child_terminated, reason, pid, child, state)
    restart_child(pid, child, state)
  end

  defp maybe_restart_child(:temporary, reason, pid, child, state) do
    report_error(:child_terminated, reason, pid, child, state)
    Distribution.untrack_spec(state.scope, child)
    {:ok, delete_child(pid, state)}
  end

  defp delete_child(
         pid,
         %{children: children, children_by_child_id: children_by_child_id} = state
       ) do
    children_by_child_id =
      case Map.get(children, pid) do
        {child_id, _, _, _, _, _} ->
          Map.delete(children_by_child_id, child_id)

        _ ->
          children_by_child_id
      end

    %{state | children: Map.delete(children, pid), children_by_child_id: children_by_child_id}
  end

  defp restart_child(pid, child, state) do
    case add_restart(state) do
      {:ok, %{strategy: strategy} = state} ->
        case restart_child(strategy, pid, child, state) do
          {:ok, state} ->
            {:ok, state}

          {:try_again, state} ->
            send(self(), {:"$gen_restart", pid})
            {:ok, state}
        end

      {:shutdown, state} ->
        report_error(:shutdown, :reached_max_restart_intensity, pid, child, state)
        Distribution.untrack_spec(state.scope, child)
        {:shutdown, delete_child(pid, state)}
    end
  end

  defp add_restart(state) do
    %{max_seconds: max_seconds, max_restarts: max_restarts, restarts: restarts} = state

    now = :erlang.monotonic_time(1)
    restarts = add_restart([now | restarts], now, max_seconds)
    state = %{state | restarts: restarts}

    if length(restarts) <= max_restarts do
      {:ok, state}
    else
      {:shutdown, state}
    end
  end

  defp add_restart(restarts, now, period) do
    for then <- restarts, now <= then + period, do: then
  end

  defp restart_child(:one_for_one, current_pid, child, state) do
    {id, {m, f, args} = mfa, restart, shutdown, type, modules} = child
    %{extra_arguments: extra} = state

    case start_child(m, f, extra ++ args) do
      {:ok, pid, _} ->
        Distribution.child_join(state.scope, id, Node.self(), self(), pid, child)
        state = delete_child(current_pid, state)
        {:ok, save_child(pid, id, mfa, restart, shutdown, type, modules, state)}

      {:ok, pid} ->
        Distribution.child_join(state.scope, id, Node.self(), self(), pid, child)
        state = delete_child(current_pid, state)
        {:ok, save_child(pid, id, mfa, restart, shutdown, type, modules, state)}

      :ignore ->
        Distribution.untrack_spec(state.scope, child)
        {:ok, delete_child(current_pid, state)}

      {:error, reason} ->
        report_error(:start_error, reason, {:restarting, current_pid}, child, state)
        state = put_in(state.children[current_pid], {:restarting, child})
        {:try_again, state}
    end
  end

  defp report_error(error, reason, pid, child, %{name: name, extra_arguments: extra}) do
    :logger.error(
      %{
        label: {:supervisor, error},
        report: [
          {:supervisor, name},
          {:errorContext, error},
          {:reason, reason},
          {:offender, extract_child(pid, child, extra)}
        ]
      },
      %{
        domain: [:otp, :sasl],
        report_cb: &:logger.format_otp_report/1,
        logger_formatter: %{title: "SUPERVISOR REPORT"},
        error_logger: %{tag: :error_report, type: :supervisor_report}
      }
    )
  end

  defp extract_child(pid, {id, {m, f, args}, restart, shutdown, type, _modules}, extra) do
    [
      pid: pid,
      id: id,
      mfargs: {m, f, extra ++ args},
      restart_type: restart,
      shutdown: shutdown,
      child_type: type
    ]
  end

  defp reschedule_sync_interval_timer(state, interval \\ nil) do
    interval = interval || state.sync_interval

    case state.sync_interval_timer_ref do
      ref when is_reference(ref) ->
        Process.cancel_timer(ref)

      _ ->
        false
    end

    timer_ref = Process.send_after(self(), :sync, interval)
    %SynSupervisor{state | sync_interval_timer_ref: timer_ref}
  end

  @impl true
  def format_status(:terminate, [_pdict, state]) do
    state
  end

  def format_status(_, [_pdict, %{mod: mod} = state]) do
    [data: [{~c"State", state}], supervisor: [{~c"Callback", mod}]]
  end

  ## Helpers

  @compile {:inline, call: 2}

  defp call(supervisor, req) do
    GenServer.call(supervisor, req, :infinity)
  end

  @doc false
  def format_report(%{
        label: {__MODULE__, :unexpected_msg},
        report: %{msg: msg}
      }) do
    {~c"SynSupervisor received unexpected message: ~p~n", [msg]}
  end
end
