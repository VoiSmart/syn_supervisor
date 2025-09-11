defmodule SynSupervisor.Distribution do
  @moduledoc """
  Module to store and retrieve SynSupervisor's distribution information
  """

  alias SynSupervisor.Distribution.Child

  @type scope_t() :: atom()

  @type group_t() :: any()

  @type child_mapper_t :: (Child.t() -> any())

  @spec start_and_join(scope_t()) :: :ok
  def start_and_join(scope) do
    # define and start 3 scopes:
    # - one to store the nodes/supervisors which have joined
    # - one to store the child specifications
    # - one to store the childrend that have been started
    # three different scopes are used to have faster lookups (to avoid calling
    # :syn.group_names(scope) |> Enum.filter(filter_fun) to only get for
    # example the child specs)
    node_scope = node_scope(scope)
    spec_scope = spec_scope(scope)
    child_scope = child_scope(scope)
    start([node_scope, spec_scope, child_scope])

    :syn.join(node_scope, Node.self(), self())
  end

  @spec child_join(scope_t(), Child.id_t(), Node.t(), pid(), pid(), Child.spec_t()) ::
          :ok | {:error, term()}
  def child_join(scope, id, node, supervisor, child_pid, child_spec) do
    child = %Child{
      id: id,
      node: node,
      pid: child_pid,
      spec: child_spec,
      supervisor_pid: supervisor
    }

    case :syn.join(child_scope(scope), child, child_pid) do
      :ok ->
        track_spec(scope, child_spec)
        :ok

      err ->
        err
    end
  end

  @spec find_child(scope_t(), Child.id_t() | pid()) :: {:ok, Child.t()} | {:error, :not_found}
  def find_child(scope, pid) when is_pid(pid) do
    find_child_with_fn(
      scope,
      fn
        %Child{pid: ^pid} = c ->
          {:ok, c}

        _ ->
          false
      end
    )
  end

  def find_child(scope, id) do
    find_child_with_fn(
      scope,
      fn
        %Child{id: ^id} = c ->
          {:ok, c}

        _ ->
          false
      end
    )
  end

  @spec list_children(scope_t()) :: list(Child.t())
  def list_children(scope) do
    get_children(scope)
  end

  @spec find_spec(scope_t(), Child.id_t()) :: {:ok, Child.spec_t()} | {:error, :not_found}
  def find_spec(scope, child_id) do
    scope
    |> get_specs()
    |> Enum.find_value(
      {:error, :not_found},
      fn
        {^child_id, _, _, _, _, _} = s ->
          {:ok, s}

        _ ->
          false
      end
    )
  end

  @spec map_children(scope_t(), (Child.t() -> arg)) :: list(arg) when arg: any
  def map_children(scope, child_mapper_fun) do
    scope
    |> get_children()
    |> Enum.map(fn child -> child_mapper_fun.(child) end)
  end

  @spec each_child(scope_t(), (Child.t() -> any())) :: :ok
  def each_child(scope, fun) do
    scope
    |> get_children()
    |> Enum.each(fn child -> fun.(child) end)
  end

  @spec reduce_child(scope_t(), acc, (Child.t(), acc -> acc)) :: acc when acc: any()
  def reduce_child(scope, acc, fun) do
    scope
    |> get_children()
    |> Enum.reduce(acc, fn child, acc -> fun.(child, acc) end)
  end

  @spec reduce_specs(scope_t(), acc, (Child.spec_t(), acc -> acc)) :: acc when acc: any()
  def reduce_specs(scope, acc, fun) do
    scope
    |> get_specs()
    |> Enum.reduce(acc, fn spec, acc -> fun.(spec, acc) end)
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
  def track_spec(scope, child_spec, supervisor_pid) do
    :syn.join(spec_scope(scope), child_spec, supervisor_pid)
  end

  @spec track_spec(scope_t(), Child.spec_t()) :: list(:ok | {:error, term()})
  def track_spec(scope, child_spec) do
    scope
    |> supervisors()
    |> then(&multi_join(spec_scope(scope), child_spec, &1))
  end

  @spec untrack_spec(scope_t(), Child.spec_t()) :: list(:ok | {:error, term()})
  def untrack_spec(scope, child_spec) do
    scope
    |> supervisors()
    |> then(&multi_leave(spec_scope(scope), child_spec, &1))
  end

  defp multi_join(scope, group, pids) do
    Enum.map(pids, &:syn.join(scope, group, &1))
  end

  defp multi_leave(scope, group, pids) do
    Enum.map(pids, &:syn.leave(scope, group, &1))
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

  @spec find_child_with_fn(scope_t(), (Child.t() -> boolean())) ::
          {:ok, Child.t()} | {:error, :not_found}
  defp find_child_with_fn(scope, fun) do
    scope
    |> get_children()
    |> Enum.find_value(
      {:error, :not_found},
      fn c ->
        if fun.(c) do
          {:ok, c}
        else
          false
        end
      end
    )
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

  @spec get_children(scope_t()) :: list(Child.t())
  defp get_children(scope) do
    scope
    |> child_scope()
    |> :syn.group_names()
  end

  @spec get_nodes(scope_t()) :: list(atom())
  defp get_nodes(scope) do
    scope
    |> node_scope()
    |> :syn.group_names()
  end

  @spec get_specs(scope_t()) :: list(Child.spec_t())
  defp get_specs(scope) do
    scope
    |> spec_scope()
    |> :syn.group_names()
  end
end
