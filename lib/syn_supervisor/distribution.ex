defmodule SynSupervisor.Distribution do
  @moduledoc """
  Module to store and retrieve SynSupervisor's distribution information
  """

  alias SynSupervisor.Distribution.Child

  @type scope_t() :: atom()

  @type group_t() :: any()

  @type child_mapper_t :: (Child.t() -> any())
  @type nodes_t :: MapSet.t(Node.t())

  @type status_t :: %{
          ready?: boolean(),
          expected_remote_nodes: nodes_t(),
          spec_scope_nodes: nodes_t(),
          child_scope_nodes: nodes_t(),
          node_scope_nodes: nodes_t()
        }

  @spec start_and_join(scope_t()) :: :ok
  def start_and_join(scope) do
    # define and start 3 scopes:
    # - one to store the nodes/supervisors which have joined
    # - one to store the child specifications
    # - one to store the children that have been started
    # three different scopes are used to have faster lookups (to avoid calling
    # :syn.group_names(scope) |> Enum.filter(filter_fun) to only get for
    # example the child specs)
    node_scope = node_scope(scope)
    spec_scope = spec_scope(scope)
    child_scope = child_scope(scope)
    start([node_scope, spec_scope, child_scope])

    :syn.join(node_scope, Node.self(), self())
  end

  @spec ready?(scope_t(), list(Node.t())) :: boolean()
  def ready?(scope, expected_remote_nodes \\ Node.list()) do
    status(scope, expected_remote_nodes).ready?
  end

  @spec status(scope_t(), list(Node.t())) :: status_t()
  def status(scope, expected_remote_nodes \\ Node.list()) do
    expected_remote_nodes = MapSet.new(expected_remote_nodes)

    node_scope_nodes = pg_subcluster_nodes(node_scope(scope))
    spec_scope_nodes = pg_subcluster_nodes(spec_scope(scope))
    child_scope_nodes = pg_subcluster_nodes(child_scope(scope))

    ready? =
      Enum.all?(
        [node_scope_nodes, spec_scope_nodes, child_scope_nodes],
        &MapSet.equal?(&1, expected_remote_nodes)
      )

    %{
      ready?: ready?,
      expected_remote_nodes: expected_remote_nodes,
      node_scope_nodes: node_scope_nodes,
      spec_scope_nodes: spec_scope_nodes,
      child_scope_nodes: child_scope_nodes
    }
  end

  @spec child_join(scope_t(), Child.id_t(), Node.t(), pid(), pid(), Child.spec_t()) ::
          :ok | {:error, term()}
  def child_join(scope, id, node, supervisor, child_pid, child_spec) do
    child = %Child{
      id: id,
      node: node,
      pid: child_pid,
      supervisor_pid: supervisor
    }

    case :syn.join(child_scope(scope), id, child_pid, child) do
      :ok ->
        track_spec(scope, child_spec)
        :ok

      err ->
        err
    end
  end

  @spec find_child(scope_t(), Child.id_t() | pid()) :: {:ok, Child.t()} | {:error, :not_found}
  def find_child(scope, pid) when is_pid(pid) do
    scope
    |> child_scope()
    |> :syn.group_names()
    |> Enum.find_value({:error, :not_found}, fn id ->
      case :syn.member(child_scope(scope), id, pid) do
        {^pid, %Child{} = child} -> {:ok, %{child | pid: pid}}
        _ -> false
      end
    end)
  end

  def find_child(scope, id) do
    case :syn.members(child_scope(scope), id) do
      [{pid, %Child{} = child}] ->
        {:ok, %{child | pid: pid}}

      [{pid, %Child{} = child} | _rest] ->
        {:ok, %{child | pid: pid}}

      [] ->
        {:error, :not_found}
    end
  end

  @spec spec_for_child(scope_t(), Child.t()) :: {:ok, Child.spec_t()} | {:error, :not_found}
  def spec_for_child(scope, %Child{} = c) do
    find_spec(scope, c.id)
  end

  @spec list_children(scope_t()) :: list(Child.t())
  def list_children(scope) do
    scope
    |> child_scope()
    |> :syn.group_names()
    |> Enum.flat_map(fn id ->
      :syn.members(child_scope(scope), id)
    end)
    |> Enum.map(fn {pid, %Child{} = child} ->
      %{child | pid: pid}
    end)
  end

  @spec find_spec(scope_t(), Child.id_t()) :: {:ok, Child.spec_t()} | {:error, :not_found}
  def find_spec(scope, child_id) do
    case :syn.members(spec_scope(scope), child_id) do
      [{_supervisor_pid, child_spec} | _] ->
        {:ok, child_spec}

      [] ->
        {:error, :not_found}
    end
  end

  @spec reduce_child(scope_t(), acc, (Child.t(), acc -> acc)) :: acc when acc: any()
  def reduce_child(scope, acc, fun) do
    scope
    |> child_scope()
    |> :syn.group_names()
    |> Enum.reduce(acc, fn id, acc ->
      :syn.members(child_scope(scope), id)
      |> Enum.reduce(acc, fn {pid, %Child{} = child}, acc ->
        fun.(%{child | pid: pid}, acc)
      end)
    end)
  end

  @spec reduce_specs(scope_t(), acc, (Child.spec_t(), acc -> acc)) :: acc when acc: any()
  def reduce_specs(scope, acc, fun) do
    scope
    |> spec_scope()
    |> :syn.group_names()
    |> Enum.reduce(acc, fn child_id, acc ->
      case :syn.members(spec_scope(scope), child_id) do
        [{_supervisor_pid, child_spec} | _] ->
          fun.(child_spec, acc)

        [] ->
          acc
      end
    end)
  end

  @spec node_for_child(scope_t(), Child.spec_t()) :: Node.t()
  def node_for_child(scope, child_spec) do
    scope
    |> create_ring()
    |> HashRing.Managed.key_to_node(child_spec)
  end

  @spec member_for_node(scope_t(), Node.t()) :: nil | pid()
  def member_for_node(scope, node) do
    case :syn.members(node_scope(scope), node) do
      [{member, _meta} | _] -> member
      _ -> nil
    end
  end

  @spec member_for_child(scope_t(), Child.spec_t()) :: {Node.t(), nil | pid()}
  def member_for_child(scope, child_spec) do
    node = node_for_child(scope, child_spec)
    {node, member_for_node(scope, node)}
  end

  @spec track_spec(scope_t(), Child.spec_t(), pid()) :: :ok | {:error, term()}
  def track_spec(scope, {child_id, _, _, _, _, _} = child_spec, supervisor_pid) do
    :syn.join(spec_scope(scope), child_id, supervisor_pid, child_spec)
  end

  @spec track_spec(scope_t(), Child.spec_t()) :: list(:ok | {:error, term()})
  def track_spec(scope, {child_id, _, _, _, _, _} = child_spec) do
    scope
    |> supervisors()
    |> Enum.map(&:syn.join(spec_scope(scope), child_id, &1, child_spec))
  end

  @spec untrack_spec(scope_t(), Child.spec_t() | Child.id_t()) :: list(:ok | {:error, term()})
  def untrack_spec(scope, {child_id, _, _, _, _, _}) do
    untrack_spec(scope, child_id)
  end

  def untrack_spec(scope, child_id) do
    scope
    |> supervisors()
    |> Enum.map(&:syn.leave(spec_scope(scope), child_id, &1))
  end

  @spec check_members(scope_t()) :: :ok
  def check_members(scope) do
    create_ring(scope, check_members: true)
    :ok
  end

  @spec create_ring(scope_t(), list({:check_members, boolean()})) :: scope_t()
  defp create_ring(scope, opts \\ []) do
    maybe_create_hash_ring(scope)

    nodes = get_nodes(scope)

    if Keyword.get(opts, :check_members, false) do
      current_nodes = MapSet.new(nodes)

      scope
      |> HashRing.Managed.nodes()
      |> MapSet.new()
      |> MapSet.difference(current_nodes)
      |> Enum.each(&HashRing.Managed.remove_node(scope, &1))
    end

    # build a consistent hash ring of existing nodes to distribute
    # child processes among them
    HashRing.Managed.add_nodes(scope, nodes)
    scope
  end

  @spec maybe_create_hash_ring(scope_t()) :: pid()
  defp maybe_create_hash_ring(scope) do
    case HashRing.Managed.new(scope) do
      {:ok, pid} -> pid
      {:error, {:already_started, pid}} -> pid
    end
  end

  @spec supervisors(scope_t()) :: list(pid())
  defp supervisors(scope) do
    scope
    |> get_nodes()
    |> Enum.flat_map(&:syn.members(node_scope(scope), &1))
    |> Enum.map(fn {pid, _meta} -> pid end)
    |> Enum.uniq()
  end

  @spec node_scope(scope_t()) :: scope_t()
  defp node_scope(scope), do: :"#{scope}-node"

  @spec spec_scope(scope_t()) :: scope_t()
  defp spec_scope(scope), do: :"#{scope}-spec"

  @spec child_scope(scope_t()) :: scope_t()
  defp child_scope(scope), do: :"#{scope}-child"

  @spec start(list(scope_t())) :: :ok
  defp start(scopes) do
    :syn.add_node_to_scopes(scopes)
  end

  @spec get_nodes(scope_t()) :: list(atom())
  defp get_nodes(scope) do
    scope
    |> node_scope()
    |> :syn.group_names()
  end

  @spec pg_subcluster_nodes(scope_t()) :: nodes_t()
  defp pg_subcluster_nodes(scope) do
    :pg
    |> :syn.subcluster_nodes(scope)
    |> MapSet.new()
  end
end
