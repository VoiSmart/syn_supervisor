defmodule SynSupervisor.ClusteredTest do
  @moduledoc false
  use ExUnit.Case, async: false

  import AssertAsync

  @supervisor TestApp.DistributedSupervisor

  alias SynSupervisor.Distribution
  alias SynSupervisor.Test.Support.Worker

  setup do
    on_exit(fn ->
      # TODO investigate a better way to remove LocalCluster flakiness
      :timer.sleep(1000)
    end)

    :ok
  end

  test "starts child on a single node in the cluster" do
    [node1, node2] = start_nodes(:test_app, "foo", 2)

    child_spec = Worker.child_spec(:init_args)

    # start_child(node1, child_spec)
    {:ok, pid} = start_child(node2, child_spec)

    assert_async do
      assert %{
               ^node1 => [{_, ^pid, :worker, [Worker]}],
               ^node2 => []
             } = local_children([node1, node2])
    end
  end

  test "starts children on a different nodes in the cluster" do
    [node1, node2] = start_nodes(:test_app, "foo", 2)

    child_spec_1 = Worker.child_spec(:init_args_a)
    child_spec_2 = Worker.child_spec(:init_args_b)

    {:ok, pid1} = start_child(node1, child_spec_1)
    {:ok, pid2} = start_child(node2, child_spec_2)

    assert_async do
      assert %{
               ^node1 => [{{Worker, :init_args_b}, ^pid2, :worker, [Worker]}],
               ^node2 => [{{Worker, :init_args_a}, ^pid1, :worker, [Worker]}]
             } = local_children([node1, node2])
    end
  end

  test "distributes children between nodes when topology changes" do
    [node1, node2] = start_nodes(:test_app, "foo", 2)

    child_spec_1 = Worker.child_spec(:a)
    child_spec_2 = Worker.child_spec(:c)
    child_spec_3 = Worker.child_spec(:e)

    {:ok, pid1} = start_child(node1, child_spec_1)
    {:ok, pid2} = start_child(node1, child_spec_2)
    {:ok, pid3} = start_child(node1, child_spec_3)

    assert_async do
      assert %{
               ^node1 => [
                 {{Worker, :a}, ^pid1, :worker, [Worker]},
                 {{Worker, :e}, ^pid3, :worker, [Worker]}
               ],
               ^node2 => [
                 {{Worker, :c}, ^pid2, :worker, [Worker]}
               ]
             } = local_children([node1, node2])
    end

    # now start another node and a process should migrate to it
    [node3] = start_nodes(:test_app, "bar", 1)

    assert_async do
      assert %{
               ^node1 => [{{Worker, :e}, _, :worker, [Worker]}],
               ^node2 => [{{Worker, :c}, _, :worker, [Worker]}],
               ^node3 => [{{Worker, :a}, _, :worker, [Worker]}]
             } = local_children([node1, node2, node3])
    end
  end

  test "restart process on another node when the node it was scheduled on goes down" do
    [node1, node2, node3] = start_nodes(:test_app, "foo", 3)

    child_spec_1 = Worker.child_spec(:a)
    child_spec_2 = Worker.child_spec(:e)
    child_spec_3 = Worker.child_spec(:c)

    {:ok, _pid1} = start_child(node1, child_spec_1)
    {:ok, _pid2} = start_child(node1, child_spec_2)
    {:ok, _pid3} = start_child(node1, child_spec_3)

    assert_async do
      assert %{
               ^node1 => [{{Worker, :e}, _, :worker, [Worker]}],
               ^node2 => [{{Worker, :c}, _, :worker, [Worker]}],
               ^node3 => [{{Worker, :a}, _, :worker, [Worker]}]
             } = local_children([node1, node2, node3])
    end

    stop_nodes([node3])

    assert_async do
      assert %{
               ^node1 => [
                 {{Worker, :a}, _, :worker, [Worker]},
                 {{Worker, :e}, _, :worker, [Worker]}
               ],
               ^node2 => [
                 {{Worker, :c}, _, :worker, [Worker]}
               ]
             } = local_children([node1, node2])
    end
  end

  describe "Distribution.status/2" do
    test "tracks node, spec, and child scopes independently" do
      [node1, node2] = start_nodes(:test_app, "status", 2)

      scope = :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower) |> String.to_atom()

      node_scope = :"#{scope}-node"
      spec_scope = :"#{scope}-spec"
      child_scope = :"#{scope}-child"

      assert :ok =
               :rpc.call(node1, :syn, :add_node_to_scopes, [[node_scope, spec_scope, child_scope]])

      assert :ok = :rpc.call(node2, :syn, :add_node_to_scopes, [[node_scope]])

      expected_remote_nodes = MapSet.new([node2])
      expected_node_scope_nodes = MapSet.new([node2])
      expected_no_nodes = MapSet.new()

      assert_async do
        assert %{
                 ready?: false,
                 expected_remote_nodes: ^expected_remote_nodes,
                 node_scope_nodes: ^expected_node_scope_nodes,
                 spec_scope_nodes: ^expected_no_nodes,
                 child_scope_nodes: ^expected_no_nodes
               } = :rpc.call(node1, Distribution, :status, [scope, [node2]])
      end
    end
  end

  describe "which_children/1" do
    test "returns children running on the local node" do
      [node1, node2] = start_nodes(:test_app, "foo", 2)

      child_spec_1 = Worker.child_spec(:init_args_a)
      child_spec_2 = Worker.child_spec(:init_args_b)

      {:ok, _pid1} = start_child(node1, child_spec_1)
      {:ok, _pid2} = start_child(node2, child_spec_2)

      assert_async do
        assert [{{Worker, :init_args_b}, _, :worker, [Worker]}] =
                 :rpc.call(node1, SynSupervisor, :which_children, [@supervisor])
      end

      assert_async do
        assert [{{Worker, :init_args_a}, _, :worker, [Worker]}] =
                 :rpc.call(node2, SynSupervisor, :which_children, [@supervisor])
      end
    end
  end

  describe "it is not possible to start children with the same id" do
    test "on the same node" do
      [node1, _node2] = start_nodes(:test_app, "foo", 2)
      child_spec_1 = Worker.child_spec(:init_args_a)

      {:ok, _pid1} = start_child(node1, child_spec_1)
      assert {:error, :already_present} = start_child(node1, child_spec_1)
    end

    test "on a different node" do
      [node1, node2] = start_nodes(:test_app, "foo", 2)
      child_spec_1 = Worker.child_spec(:init_args_a)

      {:ok, _pid1} = start_child(node1, child_spec_1)
      assert {:error, :already_present} = start_child(node2, child_spec_1)
    end
  end

  describe "which_children/2" do
    test "returns children running on the node when called with :local" do
      [node1, node2] = start_nodes(:test_app, "foo", 2)

      child_spec_1 = Worker.child_spec(:init_args_a)
      child_spec_2 = Worker.child_spec(:init_args_b)

      {:ok, _pid1} = start_child(node1, child_spec_1)
      {:ok, _pid2} = start_child(node2, child_spec_2)

      assert_async do
        assert [{{Worker, :init_args_b}, _, :worker, [Worker]}] =
                 :rpc.call(node1, SynSupervisor, :which_children, [@supervisor, :local])
      end

      assert_async do
        assert [{{Worker, :init_args_a}, _, :worker, [Worker]}] =
                 :rpc.call(node2, SynSupervisor, :which_children, [@supervisor, :local])
      end
    end

    test "returns all children running on the node when called with :global" do
      [node1, node2] = start_nodes(:test_app, "foo", 2)

      child_spec_1 = Worker.child_spec(:init_args_a)
      child_spec_2 = Worker.child_spec(:init_args_b)

      {:ok, pid1} = start_child(node1, child_spec_1)
      {:ok, pid2} = start_child(node2, child_spec_2)

      assert_async do
        assert [
                 {{Worker, :init_args_a}, wpid1, :worker, [Worker]},
                 {{Worker, :init_args_b}, wpid2, :worker, [Worker]}
               ] = :rpc.call(node1, SynSupervisor, :which_children, [@supervisor, :global])

        assert pid1 in [wpid1, wpid2]
        assert pid2 in [wpid1, wpid2]
      end

      assert_async do
        assert [
                 {{Worker, :init_args_a}, wpid1, :worker, [Worker]},
                 {{Worker, :init_args_b}, wpid2, :worker, [Worker]}
               ] = :rpc.call(node2, SynSupervisor, :which_children, [@supervisor, :global])

        assert pid1 in [wpid1, wpid2]
        assert pid2 in [wpid1, wpid2]
      end
    end
  end

  describe "count_children/1" do
    test "counts children running on the local node" do
      [node1, node2] = start_nodes(:test_app, "foo", 2)

      child_spec_1 = Worker.child_spec(:init_args_a)
      child_spec_2 = Worker.child_spec(:init_args_b)

      {:ok, _pid1} = start_child(node1, child_spec_1)
      {:ok, _pid2} = start_child(node2, child_spec_2)

      assert_async do
        assert %{active: 1, specs: 1, supervisors: 0, workers: 1} =
                 :rpc.call(node1, SynSupervisor, :count_children, [@supervisor])
      end

      assert_async do
        assert %{active: 1, specs: 1, supervisors: 0, workers: 1} =
                 :rpc.call(node2, SynSupervisor, :count_children, [@supervisor])
      end
    end
  end

  describe "count_children/2" do
    test "counts children running on the local node when called with :local" do
      [node1, node2] = start_nodes(:test_app, "foo", 2)

      child_spec_1 = Worker.child_spec(:init_args_a)
      child_spec_2 = Worker.child_spec(:init_args_b)

      {:ok, _pid1} = start_child(node1, child_spec_1)
      {:ok, _pid2} = start_child(node2, child_spec_2)

      assert_async do
        assert %{active: 1, specs: 1, supervisors: 0, workers: 1} =
                 :rpc.call(node1, SynSupervisor, :count_children, [@supervisor, :local])
      end

      assert_async do
        assert %{active: 1, specs: 1, supervisors: 0, workers: 1} =
                 :rpc.call(node2, SynSupervisor, :count_children, [@supervisor, :local])
      end
    end

    test "counts children running on all nodes when called with :global" do
      [node1, node2] = start_nodes(:test_app, "foo", 2)

      child_spec_1 = Worker.child_spec(:init_args_1)
      child_spec_2 = Worker.child_spec(:init_args_2)

      {:ok, _pid1} = start_child(node1, child_spec_1)
      {:ok, _pid2} = start_child(node2, child_spec_2)

      assert_async do
        assert %{active: 2, specs: 2, supervisors: 0, workers: 2} =
                 :rpc.call(node1, SynSupervisor, :count_children, [@supervisor, :global])
      end

      assert_async do
        assert %{active: 2, specs: 2, supervisors: 0, workers: 2} =
                 :rpc.call(node2, SynSupervisor, :count_children, [@supervisor, :global])
      end
    end
  end

  describe "whereis/2" do
    test "will return nil when a child_id is not found" do
      [node1, _node2] = start_nodes(:test_app, "foo", 2)

      child_spec_1 = Worker.child_spec(:init_args_a)

      {:ok, _pid1} = start_child(node1, child_spec_1)

      assert_async do
        assert is_nil(:rpc.call(node1, SynSupervisor, :whereis, [@supervisor, :no_such_id]))
      end
    end

    test "will return nil when the child is not started, its pid when it is" do
      [node1, _node2] = start_nodes(:test_app, "foo", 2)

      child_spec_1 = Worker.child_spec(:init_args_a)

      assert_async do
        assert is_nil(:rpc.call(node1, SynSupervisor, :whereis, [@supervisor, :init_args_a]))
      end

      {:ok, pid1} = start_child(node1, child_spec_1)

      assert_async do
        assert :rpc.call(node1, SynSupervisor, :whereis, [@supervisor, child_spec_1.id]) == pid1
      end
    end

    test "will return a child pid with both local and non local children" do
      [node1, node2] = start_nodes(:test_app, "foo", 2)

      child_spec_1 = Worker.child_spec(:init_args_a)
      child_spec_2 = Worker.child_spec(:init_args_b)

      assert_async do
        assert is_nil(:rpc.call(node1, SynSupervisor, :whereis, [@supervisor, child_spec_1.id]))
      end

      {:ok, pid1} = start_child(node1, child_spec_1)
      {:ok, _pid2} = start_child(node2, child_spec_2)

      assert_async do
        assert :rpc.call(node1, SynSupervisor, :whereis, [@supervisor, child_spec_1.id]) == pid1
      end

      assert_async do
        assert :rpc.call(node2, SynSupervisor, :whereis, [@supervisor, child_spec_1.id]) == pid1
      end
    end
  end

  defp start_child(node, child_spec) do
    :rpc.call(node, SynSupervisor, :start_child, [@supervisor, child_spec])
  end

  defp local_children(nodes) do
    for node <- nodes, into: %{} do
      local_children = :rpc.call(node, SynSupervisor, :which_children, [@supervisor])

      {node, Enum.sort(local_children)}
    end
  end

  defp start_nodes(app, prefix, n) do
    LocalCluster.start_nodes(prefix, n,
      applications: [:syn, :libring, app],
      files: ["test/support/syn_supervisor/worker.ex"]
    )
  end

  defp stop_nodes(nodes) do
    LocalCluster.stop_nodes(nodes)
  end
end
