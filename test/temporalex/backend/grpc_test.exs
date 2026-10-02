defmodule Temporalex.Backend.OptionalOperationsTest do
  @moduledoc """
  The optional client operations on backends that lack them: the NIF backend
  answers :unsupported itself, and a backend that does not define them gets
  the same answer from Temporalex.Client. No server needed.
  """

  use ExUnit.Case, async: true

  alias Temporalex.Backend.TemporalCore
  alias Temporalex.Client

  test "TemporalCore answers {:unsupported, message} naming the gRPC backend" do
    state = %TemporalCore.ClientState{}

    for result <- [
          TemporalCore.fetch_history_page(state, "w", nil, []),
          TemporalCore.list_workflows(state, "", []),
          TemporalCore.update_workflow_options(state, "w", nil, []),
          TemporalCore.reset_workflow(state, "w", nil, []),
          TemporalCore.set_worker_deployment_current_version(state, "d", "b", []),
          TemporalCore.describe_worker_deployment(state, "d", [])
        ] do
      assert {:error, {:unsupported, message}} = result
      assert message =~ "Temporalex.Backend.Grpc"
    end
  end

  test "Client answers :unsupported for a backend without the callback" do
    {:ok, client} = Client.start_link(name: nil, backend: Temporalex.Backend.Test)

    assert {:error,
            %Temporalex.TransportError{
              category: :unsupported,
              operation: :list_workflows,
              message: message
            }} = Client.list_workflows(client, "")

    assert message =~ "Temporalex.Backend.Test"

    assert {:error,
            %Temporalex.TransportError{category: :unsupported, operation: :reset_workflow}} =
             Client.reset_workflow(client, "w", event_id: 3)
  end

  test "Error normalizes {:unsupported, message} to a TransportError" do
    assert %Temporalex.TransportError{category: :unsupported, message: "nope", operation: :op} =
             Temporalex.Error.normalize_client_reason({:unsupported, "nope"}, operation: :op)
  end
end

if Code.ensure_loaded?(Temporalex.Backend.Grpc) do
  defmodule Temporalex.Backend.GrpcTest do
    @moduledoc """
    Unit coverage for Temporalex.Backend.Grpc's conversions — payloads,
    failures, start options, targets, error mapping — with no server.
    """

    use ExUnit.Case, async: true

    alias Temporal.Api.Common.V1.Payload
    alias Temporal.Api.Common.V1.Payloads, as: PayloadList
    alias Temporal.Api.Errordetails.V1.WorkflowExecutionAlreadyStartedFailure
    alias Temporal.Api.Failure.V1, as: F
    alias Temporal.Api.Workflowservice.V1, as: WS
    alias Temporalex.Backend.Grpc
    alias Temporalex.Backend.Grpc.Connection
    alias Temporalex.Backend.Grpc.Payloads
    alias Temporalex.Backend.Grpc.Results
    alias Temporalex.Backend.Grpc.Rpc
    alias Temporalex.Backend.Grpc.StartOptions
    alias Temporalex.TestSupport.TemporalDevServer

    describe "payload encoding" do
      test ":etf encodes any term as binary/erlang-eterm" do
        assert {:ok, %Payload{metadata: %{"encoding" => "binary/erlang-eterm"}, data: data}} =
                 Payloads.encode({:a, self()}, :etf)

        assert :erlang.binary_to_term(data) == {:a, self()}
      end

      test ":json encodes JSON-compatible terms as json/plain" do
        assert {:ok, %Payload{metadata: %{"encoding" => "json/plain"}, data: ~s({"a":[1,null]})}} =
                 Payloads.encode(%{"a" => [1, nil]}, :json)
      end

      test ":json refuses what JSON cannot represent, without inspecting the value" do
        for {value, kind} <- [
              {{:secret, "s3cr3t"}, "a tuple"},
              {[api_key: "s3cr3t"], "keyword list"},
              {self(), "a pid"},
              {make_ref(), "a reference"},
              {<<0xFF, 0xFE>>, "not valid UTF-8"},
              {%{"k" => {:nested}}, "map"}
            ] do
          assert {:error, {:payload_conversion, message}} = Payloads.encode(value, :json)
          assert message =~ "payload_codec: :json cannot represent"
          assert message =~ kind
          refute message =~ "s3cr3t"
        end
      end

      test "lists and headers stop at the first refused value" do
        assert {:error, {:payload_conversion, _}} = Payloads.encode_list([1, {:x}], :json)
        assert {:error, {:payload_conversion, _}} = Payloads.header(%{"h" => {:x}}, :json)
        assert {:ok, nil} = Payloads.header(nil, :json)
        assert {:ok, %{fields: %{"k" => _}}} = Payloads.header(%{k: 1}, :etf)
      end
    end

    describe "payload decoding" do
      test "accepts json/plain and binary/erlang-eterm whatever the codec" do
        assert {:ok, %{"a" => 1}} =
                 Payloads.decode(%Payload{
                   metadata: %{"encoding" => "json/plain"},
                   data: ~s({"a":1})
                 })

        assert {:ok, {:ok, [1]}} =
                 Payloads.decode(%Payload{
                   metadata: %{"encoding" => "binary/erlang-eterm"},
                   data: :erlang.term_to_binary({:ok, [1]})
                 })
      end

      test "empty data and binary/null are nil; a missing encoding is ETF, as in the NIF" do
        assert {:ok, nil} = Payloads.decode(%Payload{metadata: %{}, data: ""})
        assert {:ok, nil} = Payloads.decode(%Payload{metadata: %{"encoding" => "binary/null"}})

        assert {:ok, :x} =
                 Payloads.decode(%Payload{metadata: %{}, data: :erlang.term_to_binary(:x)})
      end

      test "ETF is decoded with :safe — a payload cannot mint atoms" do
        # An atom this node has never seen, built by hand: ETF SMALL_ATOM_UTF8_EXT.
        name = "temporalex_never_seen_#{System.unique_integer([:positive])}"
        data = <<131, 119, byte_size(name)>> <> name

        assert {:error, {:payload_conversion, message}} =
                 Payloads.decode(%Payload{
                   metadata: %{"encoding" => "binary/erlang-eterm"},
                   data: data
                 })

        assert message =~ ":safe"
      end

      test "bad JSON is a payload conversion error" do
        assert {:error, {:payload_conversion, "json/plain decode: " <> _}} =
                 Payloads.decode(%Payload{metadata: %{"encoding" => "json/plain"}, data: "{"})
      end

      test "a result is the first payload, or nil" do
        assert {:ok, nil} = Payloads.decode_first(nil)
        assert {:ok, nil} = Payloads.decode_first(%PayloadList{payloads: []})

        {:ok, list} = Payloads.encode_list([1, 2], :etf)
        assert {:ok, 1} = Payloads.decode_first(list)
      end
    end

    describe "failures" do
      test "become the Temporalex.Failure structs the NIF builds, causes included" do
        {:ok, details} = Payloads.encode_list([:why], :etf)

        failure = %F.Failure{
          message: "activity failed",
          source: "s",
          stack_trace: "st",
          failure_info:
            {:activity_failure_info,
             %F.ActivityFailureInfo{
               activity_id: "1",
               activity_type: %Temporal.Api.Common.V1.ActivityType{name: "charge"},
               identity: "w",
               retry_state: :RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED
             }},
          cause: %F.Failure{
            message: "boom",
            failure_info:
              {:application_failure_info,
               %F.ApplicationFailureInfo{type: "Boom", non_retryable: true, details: details}},
            cause: %F.Failure{
              message: "late",
              failure_info:
                {:timeout_failure_info,
                 %F.TimeoutFailureInfo{timeout_type: :TIMEOUT_TYPE_HEARTBEAT}}
            }
          }
        }

        assert {:ok,
                %Temporalex.Failure.ActivityError{
                  message: "activity failed",
                  source: "s",
                  stack_trace: "st",
                  activity_id: "1",
                  activity_type: "charge",
                  identity: "w",
                  retry_state: :maximum_attempts_reached,
                  cause: %Temporalex.Failure.ApplicationError{
                    message: "boom",
                    type: "Boom",
                    retryable?: false,
                    details: [:why],
                    cause: %Temporalex.Failure.TimeoutError{timeout_type: :heartbeat, cause: nil}
                  }
                }} = Payloads.failure(failure)
      end

      test "unmodelled kinds are UnknownError with the NIF's failure_type atoms" do
        assert {:ok, %Temporalex.Failure.UnknownError{failure_type: :terminated_failure}} =
                 Payloads.failure(%F.Failure{
                   failure_info: {:terminated_failure_info, %F.TerminatedFailureInfo{}}
                 })

        assert {:ok, %Temporalex.Failure.UnknownError{failure_type: :unknown_failure}} =
                 Payloads.failure(%F.Failure{})
      end
    end

    describe "start options" do
      defp build(opts, codec \\ :etf) do
        StartOptions.build("ns", "wid", "Type", "tq", :input, opts, %{
          payload_codec: codec,
          identity: "me"
        })
      end

      test "a plain start" do
        assert {:ok, {:start, %WS.StartWorkflowExecutionRequest{} = request}} =
                 build(
                   execution_timeout: 1_500,
                   id_conflict_policy: :use_existing,
                   retry_policy: [maximum_attempts: 3, initial_interval: 250],
                   priority: [priority_key: 2, fairness_key: "tenant"],
                   memo: %{"m" => 1},
                   static_summary: "sum",
                   request_id: "rid-1"
                 )

        assert request.workflow_id == "wid"
        assert request.task_queue.name == "tq"
        assert request.request_id == "rid-1"
        assert request.identity == "me"

        assert request.workflow_execution_timeout == %Google.Protobuf.Duration{
                 seconds: 1,
                 nanos: 500_000_000
               }

        assert request.workflow_id_conflict_policy == :WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
        assert request.retry_policy.maximum_attempts == 3
        assert request.priority.priority_key == 2
        assert Map.keys(request.memo.fields) == ["m"]
        assert request.user_metadata.summary.data == ~s("sum")
      end

      test "a generated request id is a v4 uuid" do
        {:ok, {:start, request}} = build([])

        assert request.request_id =~
                 ~r/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/
      end

      test ":start_signal makes it signal-with-start, and needs a name" do
        assert {:ok,
                {:signal_with_start, %WS.SignalWithStartWorkflowExecutionRequest{} = request}} =
                 build(start_signal: [name: "go", args: [1, 2]])

        assert request.signal_name == "go"
        assert length(request.signal_input.payloads) == 2

        assert {:error, {:invalid_options, "start_signal requires a name"}} =
                 build(start_signal: [args: [1]])
      end

      test ":terminate_if_running is sent as its server-side equivalent, and contradictions refused" do
        assert {:ok, {:start, request}} = build(id_reuse_policy: :terminate_if_running)
        assert request.workflow_id_reuse_policy == :WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE

        assert request.workflow_id_conflict_policy ==
                 :WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING

        assert {:error, {:invalid_options, message}} =
                 build(id_reuse_policy: :terminate_if_running, id_conflict_policy: :use_existing)

        assert message =~ "contradicts"
      end

      test "refuses what the NIF refuses" do
        for opts <- [
              [run_timeout: -1],
              [retry_policy: [backoff_coefficient: 0.5]],
              [retry_policy: [maximum_attempts: -1]],
              [priority: [priority_key: 0]],
              [priority: [fairness_key: String.duplicate("x", 65)]],
              [priority: [fairness_weight: 0]],
              [id_reuse_policy: :sometimes],
              [search_attributes: %{"Bad" => {:tuple}}]
            ] do
          assert {:error, {:invalid_options, _}} = build(opts), inspect(opts)
        end
      end

      test "payload_codec: :json refuses a non-JSON input" do
        assert {:error, {:payload_conversion, _}} =
                 StartOptions.build("ns", "w", "T", "tq", {:no}, [], %{
                   payload_codec: :json,
                   identity: "i"
                 })
      end
    end

    describe "targets" do
      test "follow the NIF's rules" do
        assert {:ok, "127.0.0.1:7233", :http} = Connection.endpoint("127.0.0.1:7233", nil)
        assert {:ok, "127.0.0.1:7233", :https} = Connection.endpoint("127.0.0.1:7233", [])

        assert {:ok, "temporal.example:443", :https} =
                 Connection.endpoint("https://temporal.example:443", nil)

        assert {:ok, "localhost:7233", :http} = Connection.endpoint("http://localhost", nil)

        assert {:error, {:connect_error, message}} =
                 Connection.endpoint("http://127.0.0.1:7233", domain: "localhost")

        assert message =~ "https"
      end
    end

    describe "rpc failures" do
      test "map onto the NIF's reasons" do
        assert :not_found = Rpc.interaction_reason(%GRPC.RPCError{status: 5, message: "gone"})

        assert {:rpc, "PermissionDenied: no"} =
                 Rpc.interaction_reason(%GRPC.RPCError{status: 7, message: "no"})

        detail = %Google.Protobuf.Any{
          type_url:
            "type.googleapis.com/temporal.api.errordetails.v1.WorkflowExecutionAlreadyStartedFailure",
          value:
            WorkflowExecutionAlreadyStartedFailure.encode(%WorkflowExecutionAlreadyStartedFailure{
              run_id: "r1"
            })
        }

        assert {:already_started, "r1"} =
                 Rpc.start_reason(%GRPC.RPCError{status: 6, message: "dup", details: [detail]})

        assert {:already_started, nil} =
                 Rpc.start_reason(%GRPC.RPCError{status: 6, message: "dup"})
      end
    end

    test "statuses and timestamps convert as in the NIF" do
      assert Results.status(:WORKFLOW_EXECUTION_STATUS_CANCELED) == :cancelled
      assert Results.status(:WORKFLOW_EXECUTION_STATUS_PAUSED) == :paused
      assert Results.status(99) == :unspecified
      assert Results.millis(%Google.Protobuf.Timestamp{seconds: 2, nanos: 345_678_901}) == 2345
      assert Results.millis(nil) == nil
    end

    describe "client lifecycle" do
      test "an unreachable server fails the client's start, as with the NIF" do
        port = TemporalDevServer.free_port()

        assert {:error, %Temporalex.TransportError{category: category}} =
                 Grpc.start_client([target: "127.0.0.1:#{port}", connect_timeout: 2_000], self())

        assert category in [:connect, :connect_timeout]
      end

      test "an invalid payload codec raises, as with the NIF" do
        assert_raise ArgumentError, ~r/payload_codec/, fn ->
          Grpc.start_client([payload_codec: :xml], self())
        end
      end

      test "worker callbacks say the backend is client-only" do
        for result <- [
              Grpc.start_worker(nil, [], self()),
              Grpc.complete_workflow_activation(nil, nil),
              Grpc.complete_activity_task(nil, nil),
              Grpc.record_activity_heartbeat(nil, "t", nil),
              Grpc.shutdown_worker(nil)
            ] do
          assert {:error, {:unsupported, message}} = result
          assert message =~ "client-only"
          assert message =~ "Temporalex.Backend.TemporalCore"
        end
      end
    end
  end
end
