if Code.ensure_loaded?(Temporalex.Backend.Grpc) do
  defmodule Temporalex.GrpcBackendIntegrationTest do
    @moduledoc """
    What only the gRPC client backend does, against a live dev server: one-page
    and long-polled history, visibility listing, Versioning Override, reset,
    Worker Deployment routing, request-id idempotency, memo on start, and the
    `payload_codec: :json` refusal. Plus parity checks that read the same
    workflow through both backends and compare, and the NIF backend's
    `:unsupported` answer for the operations it lacks.

    The per-method parity suites (client_api, client semantics, fetch_history,
    signal_with_start, structured_errors, json_codec, client_tls) run on both
    backends; see `Temporalex.TestSupport.Backends`.

    Skipped by default; run with `mix test --include external`.
    """

    use ExUnit.Case, async: false

    @moduletag :external

    alias Temporalex.Backend.TemporalCore.Codec
    alias Temporalex.Client
    alias Temporalex.TestSupport.Backends
    alias Temporalex.TestSupport.Server
    alias Temporalex.Workflow.API

    defmodule Counter do
      use Temporalex.Workflow

      def handle_query("count", _args, state), do: {:reply, state}

      def run(initial) do
        API.publish_state(initial)

        result =
          API.phase(initial,
            signal: %{
              "tick" => fn _args, count ->
                API.publish_state(count + 1)
                {:noreply, count + 1}
              end,
              "stop" => fn _args, count -> {:stop, count} end
            }
          )

        {:ok, result}
      end
    end

    defmodule Echo do
      use Temporalex.Workflow

      def run(input), do: {:ok, input}
    end

    setup_all do
      unless Server.reachable?(),
        do: raise("Temporal dev server not reachable at #{Server.address()}")

      task_queue = "grpc-backend-#{System.unique_integer([:positive])}"
      worker = Module.concat(__MODULE__, :"Worker#{System.unique_integer([:positive])}")

      clients =
        Backends.start_clients(__MODULE__,
          target: Server.target(),
          namespace: Temporalex.TestSupport.Namespace.name(),
          task_queue: task_queue
        )

      json_client = Module.concat(__MODULE__, :"JsonClient#{System.unique_integer([:positive])}")

      {:ok, _} =
        Client.start_link(
          name: json_client,
          backend: Temporalex.Backend.Grpc,
          target: Server.target(),
          namespace: Temporalex.TestSupport.Namespace.name(),
          task_queue: task_queue,
          payload_codec: :json
        )

      {:ok, worker_pid} =
        Temporalex.Worker.start_link(
          name: worker,
          client: clients.nif,
          task_queue: task_queue,
          workflows: [Counter, Echo],
          activities: []
        )

      on_exit(fn ->
        try do
          if Process.alive?(worker_pid), do: Supervisor.stop(worker_pid, :normal, 5_000)
        catch
          :exit, _ -> :ok
        end
      end)

      {:ok, nif: clients.nif, grpc: clients.grpc, json: json_client, task_queue: task_queue}
    end

    defp wid(prefix), do: "grpc-#{prefix}-#{System.unique_integer([:positive])}"

    defp start_counter(client, prefix, opts \\ []) do
      {:ok, handle} =
        Client.start_workflow(
          client,
          Counter,
          0,
          [workflow_id: wid(prefix), timeout: 10_000] ++ opts
        )

      assert eventually(fn ->
               Client.query_workflow(handle, "count", [], timeout: 5_000) == {:ok, 0}
             end)

      handle
    end

    defp stop_counter(handle) do
      :ok = Client.signal_workflow(handle, "stop", [], timeout: 5_000)
      Client.get_result(handle, timeout: 15_000)
    end

    defp decode_history(bytes) do
      {:ok, events} = Codec.history_from_bytes(bytes)
      events
    end

    describe "fetch_history_page" do
      test "pages through history with a page size and token", %{grpc: grpc} do
        handle = start_counter(grpc, "page")
        :ok = Client.signal_workflow(handle, "tick", [], timeout: 5_000)
        assert {:ok, 1} = stop_counter(handle)

        {:ok, full} = Client.fetch_workflow_history(handle, timeout: 10_000)
        pages = pages(handle, nil, [])

        assert length(pages) > 1
        assert Enum.flat_map(pages, & &1) |> Enum.map(& &1.id) == Enum.map(full.events, & &1.id)
      end

      test "a follower's token returns new events when they land", %{grpc: grpc} do
        handle = start_counter(grpc, "follow")

        # For an open run the server hands back a token at the end of history
        # only when wait_new_event is set.
        assert {:ok, %{history: bytes, next_page_token: token}} =
                 Client.fetch_history_page(handle, wait_new_event: true, timeout: 30_000)

        assert is_binary(token)
        last_id = bytes |> decode_history() |> List.last() |> Map.fetch!(:id)

        parent = self()

        spawn(fn ->
          Process.sleep(500)
          send(parent, {:signalled, Client.signal_workflow(handle, "tick", [], timeout: 5_000)})
        end)

        assert {:ok, %{history: new_bytes}} =
                 Client.fetch_history_page(handle,
                   page_token: token,
                   wait_new_event: true,
                   timeout: 30_000
                 )

        assert_receive {:signalled, :ok}, 5_000
        new_events = decode_history(new_bytes)
        assert Enum.all?(new_events, &(&1.id > last_id))
        assert :workflow_execution_signaled in Enum.map(new_events, & &1.type)

        stop_counter(handle)
      end

      test "event_filter: :close returns only the close event", %{grpc: grpc} do
        {:ok, handle} =
          Client.start_workflow(grpc, Echo, "x", workflow_id: wid("close"), timeout: 10_000)

        assert {:ok, "x"} = Client.get_result(handle, timeout: 15_000)

        assert {:ok, %{history: bytes}} = Client.fetch_history_page(handle, event_filter: :close)
        assert [%{type: :workflow_execution_completed}] = decode_history(bytes)
      end
    end

    defp pages(handle, token, acc) do
      {:ok, %{history: bytes, next_page_token: next}} =
        Client.fetch_history_page(handle,
          page_token: token,
          maximum_page_size: 3,
          timeout: 10_000
        )

      acc = [decode_history(bytes) | acc]
      if next, do: pages(handle, next, acc), else: Enum.reverse(acc)
    end

    test "list_workflows finds executions by visibility query, in the describe shape", %{
      grpc: grpc
    } do
      {:ok, handle} =
        Client.start_workflow(grpc, Echo, 1, workflow_id: wid("list"), timeout: 10_000)

      assert {:ok, 1} = Client.get_result(handle, timeout: 15_000)

      query = "WorkflowId = '#{handle.workflow_id}'"

      assert eventually(
               fn ->
                 match?(
                   {:ok, %{executions: [%{status: :completed}]}},
                   Client.list_workflows(grpc, query, page_size: 10)
                 )
               end,
               15_000
             )

      {:ok, %{executions: [execution]}} = Client.list_workflows(grpc, query)
      {:ok, described} = Client.describe_workflow(handle)

      assert Map.keys(execution) |> Enum.sort() == Map.keys(described) |> Enum.sort()
      assert execution.workflow_id == handle.workflow_id
      assert execution.run_id == handle.run_id
      assert execution.workflow_type == Echo.__workflow_type__()
    end

    describe "reset_workflow" do
      # The request id is passed through; the server decides what a resend
      # means. Measured on server 1.32: a second reset of the same base run
      # with the same request id starts another run, so no dedup is asserted.
      test "starts a new run from a workflow-task-completed event", %{grpc: grpc} do
        {:ok, handle} =
          Client.start_workflow(grpc, Echo, "r", workflow_id: wid("reset"), timeout: 10_000)

        assert {:ok, "r"} = Client.get_result(handle, timeout: 15_000)

        {:ok, history} = Client.fetch_workflow_history(handle)
        %{id: event_id} = Temporalex.History.last(history, :workflow_task_completed)

        assert {:ok, %{run_id: new_run}} =
                 Client.reset_workflow(handle,
                   event_id: event_id,
                   reason: "grpc backend test",
                   request_id: "reset-#{System.unique_integer([:positive])}"
                 )

        assert is_binary(new_run) and new_run != handle.run_id

        assert {:ok, "r"} =
                 Client.get_result(%{handle | run_id: new_run}, timeout: 15_000)
      end

      test "without :event_id is an invalid option", %{grpc: grpc} do
        assert {:error, %Temporalex.TransportError{category: :invalid_options}} =
                 Client.reset_workflow(grpc, "anything", [])
      end
    end

    describe "request ids" do
      test "a resent start with the same request id is the same start", %{grpc: grpc} do
        workflow_id = wid("start-rid")

        opts = [
          workflow_id: workflow_id,
          request_id: "rid-#{workflow_id}",
          id_conflict_policy: :fail
        ]

        assert {:ok, first} = Client.start_workflow(grpc, Counter, 0, opts)
        assert {:ok, again} = Client.start_workflow(grpc, Counter, 0, opts)
        assert again.run_id == first.run_id

        # A different request id is a different start, refused by the policy.
        assert {:error, %Temporalex.WorkflowAlreadyStartedError{}} =
                 Client.start_workflow(grpc, Counter, 0, Keyword.put(opts, :request_id, "other"))

        stop_counter(first)
      end

      test "a resent signal with the same request id is delivered once", %{grpc: grpc} do
        handle = start_counter(grpc, "signal-rid")
        request_id = "sig-#{handle.workflow_id}"

        assert :ok = Client.signal_workflow(handle, "tick", [], request_id: request_id)
        assert :ok = Client.signal_workflow(handle, "tick", [], request_id: request_id)
        assert :ok = Client.signal_workflow(handle, "tick", [], request_id: request_id <> "-2")

        assert {:ok, 2} = stop_counter(handle)
      end

      test "a resent cancel with the same request id succeeds", %{grpc: grpc} do
        handle = start_counter(grpc, "cancel-rid")
        request_id = "cancel-#{handle.workflow_id}"

        assert :ok = Client.cancel_workflow(handle, request_id: request_id)
        assert :ok = Client.cancel_workflow(handle, request_id: request_id)
        Client.terminate_workflow(handle, reason: "cleanup")
      end

      test "a resent terminate with the same request id succeeds; another reports not found",
           %{grpc: grpc} do
        handle = start_counter(grpc, "terminate-rid")
        request_id = "term-#{handle.workflow_id}"

        assert :ok = Client.terminate_workflow(handle, reason: "rid", request_id: request_id)
        assert :ok = Client.terminate_workflow(handle, reason: "rid", request_id: request_id)

        assert {:error, %Temporalex.WorkflowNotFoundError{}} =
                 Client.terminate_workflow(handle, reason: "rid", request_id: "someone-else")

        assert {:error, %Temporalex.WorkflowNotFoundError{}} = Client.terminate_workflow(handle)
        assert {:error, %Temporalex.WorkflowTerminatedError{}} = Client.get_result(handle)
      end
    end

    describe "payload_codec: :json" do
      test "sends json/plain that the worker and the NIF client both decode", %{
        json: json,
        nif: nif
      } do
        {:ok, handle} =
          Client.start_workflow(json, Echo, %{"a" => [1, 2.5, "three", nil, true]},
            workflow_id: wid("json"),
            timeout: 10_000
          )

        assert {:ok, %{"a" => [1, 2.5, "three", nil, true]}} = Client.get_result(handle)
        assert {:ok, history} = Client.fetch_workflow_history(nif, handle.workflow_id, [])
        started = Temporalex.History.last(history, :workflow_execution_started)
        [payload] = started.attributes.input.payloads
        assert payload.metadata["encoding"] == "json/plain"
      end

      test "refuses a value JSON cannot represent instead of falling back to ETF", %{json: json} do
        for value <- [{:tuple, 1}, [key: "keyword list"], %{"pid" => self()}, <<0xFF>>] do
          assert {:error,
                  %Temporalex.TransportError{category: :payload_conversion, message: message}} =
                   Client.start_workflow(json, Echo, value, workflow_id: wid("json-refused"))

          assert message =~ "payload_codec: :json cannot represent"
        end

        handle = start_counter(json, "json-signal-refused")

        assert {:error, %Temporalex.TransportError{category: :payload_conversion}} =
                 Client.signal_workflow(handle, "tick", [{:not, :json}])

        assert {:error, %Temporalex.TransportError{category: :payload_conversion}} =
                 Client.terminate_workflow(handle, details: {:not, :json})

        stop_counter(handle)
      end
    end

    describe "parity" do
      test "describe and history read the same through both backends", %{nif: nif, grpc: grpc} do
        {:ok, handle} =
          Client.start_workflow(grpc, Echo, {:etf, :term}, workflow_id: wid("parity"))

        assert {:ok, {:etf, :term}} = Client.get_result(handle)

        assert {:ok, described} = Client.describe_workflow(grpc, handle.workflow_id, [])
        assert {:ok, ^described} = Client.describe_workflow(nif, handle.workflow_id, [])
        assert described.status == :completed

        assert {:ok, grpc_history} = Client.fetch_workflow_history(grpc, handle.workflow_id, [])
        assert {:ok, nif_history} = Client.fetch_workflow_history(nif, handle.workflow_id, [])
        assert grpc_history == nif_history

        assert {:ok, grpc_raw} =
                 Client.fetch_workflow_history(grpc, handle.workflow_id, raw: true)

        assert {:ok, nif_raw} = Client.fetch_workflow_history(nif, handle.workflow_id, raw: true)
        assert decode_history(grpc_raw) == decode_history(nif_raw)
      end

      test "memo on start is carried (the NIF client has no memo option); both read it back",
           %{nif: nif, grpc: grpc} do
        {:ok, handle} =
          Client.start_workflow(grpc, Echo, 1,
            workflow_id: wid("memo"),
            memo: %{"origin" => "grpc", "n" => 7}
          )

        assert {:ok, %{memo: %{"origin" => "grpc", "n" => 7}}} = Client.describe_workflow(handle)

        assert {:ok, %{memo: %{"origin" => "grpc", "n" => 7}}} =
                 Client.describe_workflow(nif, handle.workflow_id, [])
      end

      test "a result wait that runs out reports the same timeout error", %{nif: nif, grpc: grpc} do
        handle = start_counter(grpc, "timeout")

        for client <- [nif, grpc] do
          assert {:error,
                  %Temporalex.TransportError{category: :timeout, operation: :get_result} = error} =
                   Client.get_result(%{handle | client: client}, timeout: 1_000)

          assert error.message =~ "1000ms"
        end

        stop_counter(handle)
      end
    end

    describe "worker deployments and versioning override" do
      @describetag timeout: 120_000

      test "routing, describe and pinned / auto-upgrade overrides", ctx do
        deployment = "grpc-deploy-#{System.unique_integer([:positive])}"
        queue = "#{ctx.task_queue}-versioned"
        worker = Module.concat(__MODULE__, :"VWorker#{System.unique_integer([:positive])}")

        {:ok, worker_pid} =
          Temporalex.Worker.start_link(
            name: worker,
            client: ctx.nif,
            task_queue: queue,
            workflows: [Counter],
            activities: [],
            versioning: [
              deployment_name: deployment,
              build_id: "b1",
              use_versioning: true,
              default_behavior: :auto_upgrade
            ]
          )

        on_exit(fn ->
          try do
            if Process.alive?(worker_pid), do: Supervisor.stop(worker_pid, :normal, 5_000)
          catch
            :exit, _ -> :ok
          end
        end)

        assert eventually(
                 fn ->
                   match?(
                     {:ok, %{versions: [_ | _]}},
                     Client.describe_worker_deployment(ctx.grpc, deployment)
                   )
                 end,
                 30_000
               ),
               "the versioned worker never registered its deployment version"

        {:ok, before} = Client.describe_worker_deployment(ctx.grpc, deployment)
        assert before.name == deployment
        assert %{deployment_name: ^deployment, build_id: "b1"} = hd(before.versions).version

        assert {:ok, %{conflict_token: token}} =
                 Client.set_worker_deployment_current_version(ctx.grpc, deployment, "b1",
                   conflict_token: before.conflict_token
                 )

        assert is_binary(token)

        assert {:ok, %{current_version: %{deployment_name: ^deployment, build_id: "b1"}}} =
                 Client.describe_worker_deployment(ctx.grpc, deployment)

        {:ok, handle} =
          Client.start_workflow(ctx.grpc, Counter, 0,
            workflow_id: wid("versioned"),
            task_queue: queue
          )

        assert {:ok, %{versioning_override: {:pinned, ^deployment, "b1"}}} =
                 Client.update_workflow_options(handle,
                   versioning_override: {:pinned, deployment, "b1"}
                 )

        assert {:ok, %{versioning_override: :auto_upgrade}} =
                 Client.update_workflow_options(handle, versioning_override: :auto_upgrade)

        assert {:ok, %{versioning_override: :unset}} =
                 Client.update_workflow_options(handle, versioning_override: :unset)

        assert {:error, %Temporalex.TransportError{category: :invalid_options}} =
                 Client.update_workflow_options(handle, versioning_override: :sideways)

        Client.terminate_workflow(handle, reason: "cleanup")
      end
    end

    describe "NIF backend" do
      test "answers :unsupported for the operations its client lacks", %{nif: nif} do
        calls = [
          fn -> Client.fetch_history_page(nif, "w", []) end,
          fn -> Client.list_workflows(nif, "") end,
          fn -> Client.update_workflow_options(nif, "w", versioning_override: :unset) end,
          fn -> Client.reset_workflow(nif, "w", event_id: 3) end,
          fn -> Client.set_worker_deployment_current_version(nif, "d", "b") end,
          fn -> Client.describe_worker_deployment(nif, "d") end
        ]

        for call <- calls do
          assert {:error, %Temporalex.TransportError{category: :unsupported, message: message}} =
                   call.()

          assert message =~ "Temporalex.Backend.Grpc"
        end
      end
    end

    test "a worker cannot run on the gRPC backend, and says why", %{grpc: grpc, task_queue: tq} do
      Process.flag(:trap_exit, true)

      result =
        Temporalex.Worker.start_link(
          name: Module.concat(__MODULE__, :"BadWorker#{System.unique_integer([:positive])}"),
          client: grpc,
          task_queue: tq,
          workflows: [Echo],
          activities: []
        )

      assert {:error, reason} = result
      assert inspect(reason) =~ "client-only"
    end

    defp eventually(fun, timeout \\ 10_000) do
      deadline = System.monotonic_time(:millisecond) + timeout
      do_eventually(fun, deadline)
    end

    defp do_eventually(fun, deadline) do
      cond do
        fun.() -> true
        System.monotonic_time(:millisecond) >= deadline -> false
        true -> Process.sleep(200) && do_eventually(fun, deadline)
      end
    end
  end
end
