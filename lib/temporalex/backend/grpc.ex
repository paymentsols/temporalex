if Code.ensure_loaded?(GRPC.Stub) and
     Code.ensure_loaded?(Temporal.Api.Workflowservice.V1.WorkflowService.Stub) do
  defmodule Temporalex.Backend.Grpc do
    @moduledoc """
    Pure-Elixir, client-only backend: Temporal's WorkflowService over gRPC.

    It replaces the client half of `Temporalex.Backend.TemporalCore` — start,
    result, signal, query, update, cancel, terminate, describe and history —
    with the same results and errors, and adds operations the NIF's client does
    not have: `fetch_history_page/4` (one page, optionally long-polled),
    `list_workflows/3`, `update_workflow_options/4` (Versioning Override),
    `reset_workflow/4`, and the Worker Deployment calls. Workers are not its
    business: a worker still runs on Temporal's Rust core through
    `Temporalex.Backend.TemporalCore`, so `start_worker/3` here returns an
    error saying so.

        {:ok, _} =
          Temporalex.Client.start_link(
            name: MyApp.Temporal,
            backend: Temporalex.Backend.Grpc,
            target: "https://temporal.internal:7233",
            namespace: "default",
            api_key: token,
            tls: [server_root_ca_cert_file: "ca.crt", domain: "temporal.internal"],
            payload_codec: :json
          )

    It takes the TemporalCore client options (`:target`/`:url`/`:address`,
    `:namespace`, `:task_queue`, `:api_key`, `:headers`, `:tls`, the four
    timeouts, `:payload_codec`) plus `:identity` and `:reconnect_attempts`.

    Its optional dependencies (`:temporalio`, `:grpc`, `:protobuf`, `:mint`)
    must be in the application's deps; without them this module is not
    compiled. See `docs/backends.md` for the comparison with the NIF backend,
    including the deliberate differences: with `payload_codec: :json` a value
    JSON cannot represent is refused rather than sent as ETF, and results are
    decoded with `binary_to_term(data, [:safe])`.
    """

    @behaviour Temporalex.Backend

    require Logger

    alias Google.Protobuf.FieldMask
    alias Temporal.Api.Common.V1.WorkflowExecution
    alias Temporal.Api.Deployment.V1.WorkerDeploymentVersion
    alias Temporal.Api.History.V1.History
    alias Temporal.Api.Query.V1.WorkflowQuery
    alias Temporal.Api.Update.V1, as: Update
    alias Temporal.Api.Workflow.V1.VersioningOverride
    alias Temporal.Api.Workflow.V1.WorkflowExecutionOptions
    alias Temporal.Api.Workflowservice.V1, as: WS
    alias Temporalex.Backend.Grpc.Connection
    alias Temporalex.Backend.Grpc.Payloads
    alias Temporalex.Backend.Grpc.Results
    alias Temporalex.Backend.Grpc.Rpc
    alias Temporalex.Backend.Grpc.StartOptions
    alias Temporalex.Backend.TemporalCore.Codec
    alias Temporalex.Backend.TlsOptions
    alias Temporalex.Error

    defmodule ClientState do
      @moduledoc false

      defstruct [
        :conn,
        :namespace,
        :task_queue,
        :target,
        :identity,
        :connect_timeout,
        :start_timeout,
        :completion_timeout,
        :shutdown_timeout,
        :workflow_result_timeout,
        payload_codec: :etf
      ]
    end

    @default_target "http://127.0.0.1:7233"
    @default_namespace "default"
    @default_task_queue "default"
    @default_connect_timeout 10_000
    @default_start_timeout 10_000
    @default_completion_timeout 10_000
    @default_shutdown_timeout 10_000
    @default_workflow_result_timeout 60_000

    ## Client lifecycle

    @impl Temporalex.Backend
    def start_client(opts, owner_pid) when is_list(opts) and is_pid(owner_pid) do
      payload_codec = payload_codec_from_opts(opts)
      target = target(opts)
      connect_timeout = Keyword.get(opts, :connect_timeout, @default_connect_timeout)

      with :ok <- ensure_grpc_started(),
           {:ok, tls} <- TlsOptions.read(opts),
           {:ok, conn} <- Connection.open(target, tls, opts, owner_pid),
           :ok <- check_server(conn, connect_timeout) do
        {:ok,
         %ClientState{
           conn: conn,
           namespace: Keyword.get(opts, :namespace, @default_namespace),
           task_queue: Keyword.get(opts, :task_queue, @default_task_queue),
           target: target,
           identity: Keyword.get(opts, :identity, "temporalex-#{System.pid()}"),
           connect_timeout: connect_timeout,
           start_timeout: Keyword.get(opts, :start_timeout, @default_start_timeout),
           completion_timeout:
             Keyword.get(opts, :completion_timeout, @default_completion_timeout),
           shutdown_timeout: Keyword.get(opts, :shutdown_timeout, @default_shutdown_timeout),
           workflow_result_timeout:
             Keyword.get(opts, :workflow_result_timeout, @default_workflow_result_timeout),
           payload_codec: payload_codec
         }}
      else
        {:error, reason} ->
          {:error, Error.normalize_client_reason(reason, operation: :connect_client)}
      end
    end

    @impl Temporalex.Backend
    def shutdown_client(%ClientState{conn: conn}), do: Connection.close(conn)

    defp payload_codec_from_opts(opts) do
      case Keyword.get(opts, :payload_codec, :etf) do
        codec when codec in [:etf, :json] ->
          codec

        other ->
          raise ArgumentError, "invalid :payload_codec #{inspect(other)}; expected :etf or :json"
      end
    end

    defp ensure_grpc_started do
      case Application.ensure_all_started(:grpc) do
        {:ok, _apps} -> :ok
        {:error, reason} -> {:error, {:connect_error, "cannot start :grpc: #{inspect(reason)}"}}
      end
    end

    # The NIF's client calls GetSystemInfo while connecting, so a wrong address
    # or a refused credential fails the client's start, not its first call.
    defp check_server(conn, timeout) do
      case Rpc.call(conn, :get_system_info, %WS.GetSystemInfoRequest{}, :connect, timeout) do
        {:ok, _info} ->
          :ok

        {:error, reason} ->
          Connection.close(conn)
          {:error, connect_failure(reason, timeout)}
      end
    end

    defp connect_failure({:connect, :timeout, _}, timeout), do: {:connect_timeout, timeout}
    defp connect_failure(%GRPC.RPCError{} = error, _), do: {:connect_error, rpc_message(error)}
    defp connect_failure({:rpc, message}, _), do: {:connect_error, message}

    defp rpc_message(error) do
      {:rpc, message} = Rpc.plain_reason(error)
      message
    end

    ## Worker callbacks: not this backend's business.

    @client_only "Temporalex.Backend.Grpc is a client-only backend; workers run on " <>
                   "Temporal's Rust core through Temporalex.Backend.TemporalCore — start " <>
                   "the worker's client with backend: Temporalex.Backend.TemporalCore"

    @impl Temporalex.Backend
    def start_worker(_client_state, _opts, _owner_pid), do: {:error, {:unsupported, @client_only}}

    @impl Temporalex.Backend
    def complete_workflow_activation(_worker_state, _completion),
      do: {:error, {:unsupported, @client_only}}

    @impl Temporalex.Backend
    def complete_activity_task(_worker_state, _completion),
      do: {:error, {:unsupported, @client_only}}

    @impl Temporalex.Backend
    def record_activity_heartbeat(_worker_state, _task_token, _details),
      do: {:error, {:unsupported, @client_only}}

    @impl Temporalex.Backend
    def shutdown_worker(_worker_state), do: {:error, {:unsupported, @client_only}}

    ## Start

    @impl Temporalex.Backend
    def start_workflow(%ClientState{} = state, workflow_type, input, opts)
        when is_binary(workflow_type) and is_list(opts) do
      state = monitored(state, opts)
      workflow_id = workflow_id(workflow_type, opts)
      task_queue = Keyword.get(opts, :task_queue, state.task_queue)
      timeout = Keyword.get(opts, :timeout, state.start_timeout)

      with {:ok, {kind, request}} <-
             StartOptions.build(
               state.namespace,
               workflow_id,
               workflow_type,
               task_queue,
               input,
               opts,
               state
             ),
           {:ok, response} <- start_call(state, kind, request, timeout) do
        {:ok, %{workflow_id: workflow_id, workflow_type: workflow_type, run_id: response.run_id}}
      end
    end

    defp start_call(state, :start, request, timeout),
      do: start_rpc(state, :start_workflow_execution, request, timeout)

    defp start_call(state, :signal_with_start, request, timeout),
      do: start_rpc(state, :signal_with_start_workflow_execution, request, timeout)

    defp start_rpc(state, fun, request, timeout) do
      case Rpc.call(state.conn, fun, request, :workflow_started, timeout) do
        {:ok, response} -> {:ok, response}
        {:error, reason} -> {:error, Rpc.start_reason(reason)}
      end
    end

    defp workflow_id(workflow_type, opts) do
      Keyword.get_lazy(opts, :workflow_id, fn -> Keyword.get(opts, :id) end) ||
        (
          Logger.warning(
            "starting #{workflow_type} with a silently generated workflow id — " <>
              "the id is Temporal's idempotency key. Derive it (id/1 on the " <>
              "workflow module) or opt out explicitly with id: :generate; " <>
              "this fallback raises in a future release"
          )

          "temporalex-#{System.system_time(:millisecond)}-#{System.unique_integer([:positive])}"
        )
    end

    ## Result

    # As the NIF's client (sdk-rust get_result, follow_runs defaulting to true):
    # long-poll the close event; a run that completed, failed or timed out with
    # a successor (cron, retry), or continued as new, is followed to the next
    # run. Cancelled and terminated runs are final. `follow_runs: false` stops
    # at the first run and reports :continued_as_new.
    @impl Temporalex.Backend
    def get_workflow_result(%ClientState{} = state, workflow_id, run_id, opts)
        when is_binary(workflow_id) and is_list(opts) do
      state = monitored(state, opts)
      timeout = Keyword.get(opts, :timeout, state.workflow_result_timeout)

      await = %{
        state: state,
        workflow_id: workflow_id,
        deadline: deadline(timeout),
        timeout: timeout,
        follow?: Keyword.get(opts, :follow_runs, true)
      }

      await_close(await, run_id || "", "")
    end

    defp await_close(await, run_id, token) do
      with {:ok, remaining} <- remaining(await.deadline, :workflow_result, await.timeout) do
        request = %WS.GetWorkflowExecutionHistoryRequest{
          namespace: await.state.namespace,
          execution: %WorkflowExecution{workflow_id: await.workflow_id, run_id: run_id},
          wait_new_event: true,
          skip_archival: true,
          history_event_filter_type: :HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT,
          next_page_token: token
        }

        case Rpc.call(
               await.state.conn,
               :get_workflow_execution_history,
               request,
               :workflow_result,
               remaining
             ) do
          {:ok, %{history: %History{events: [_ | _] = events}}} ->
            close_event(await, List.last(events).attributes)

          {:ok, %{next_page_token: next}} when next not in [nil, ""] ->
            await_close(await, run_id, next)

          {:ok, _empty} ->
            await_close(await, run_id, "")

          {:error, {:workflow_result, :timeout, _}} ->
            {:error, {:workflow_result, :timeout, await.timeout}}

          {:error, reason} ->
            {:error, Rpc.interaction_reason(reason)}
        end
      end
    end

    defp close_event(await, {:workflow_execution_completed_event_attributes, attrs}) do
      follow_or(await, attrs.new_execution_run_id, fn -> Payloads.decode_first(attrs.result) end)
    end

    defp close_event(await, {:workflow_execution_failed_event_attributes, attrs}) do
      follow_or(await, attrs.new_execution_run_id, fn ->
        case Payloads.failure(attrs.failure || %Temporal.Api.Failure.V1.Failure{}) do
          {:ok, failure} -> {:error, {:failed, failure}}
          {:error, reason} -> {:error, reason}
        end
      end)
    end

    defp close_event(_await, {:workflow_execution_canceled_event_attributes, attrs}) do
      case Payloads.decode_list(attrs.details) do
        {:ok, details} -> {:error, {:cancelled, details}}
        {:error, reason} -> {:error, reason}
      end
    end

    defp close_event(await, {:workflow_execution_timed_out_event_attributes, attrs}),
      do: follow_or(await, attrs.new_execution_run_id, fn -> {:error, :timed_out} end)

    defp close_event(_await, {:workflow_execution_terminated_event_attributes, attrs}) do
      case Payloads.decode_list(attrs.details) do
        {:ok, details} -> {:error, {:terminated, details}}
        {:error, reason} -> {:error, reason}
      end
    end

    defp close_event(
           %{follow?: false},
           {:workflow_execution_continued_as_new_event_attributes, _}
         ),
         do: {:error, :continued_as_new}

    defp close_event(await, {:workflow_execution_continued_as_new_event_attributes, attrs}) do
      case attrs.new_execution_run_id do
        "" -> {:error, {:rpc, "New execution run id was empty in continue as new event!"}}
        next_run_id -> await_close(await, next_run_id, "")
      end
    end

    defp close_event(_await, other) do
      {:error,
       {:rpc,
        "Server returned an event that didn't match the CloseEvent filter: #{inspect(elem_name(other))}"}}
    end

    defp elem_name({name, _attrs}), do: name
    defp elem_name(other), do: other

    defp follow_or(%{follow?: true} = await, next_run_id, _final)
         when next_run_id not in [nil, ""],
         do: await_close(await, next_run_id, "")

    defp follow_or(_await, _next_run_id, final), do: final.()

    ## Signal, query, update

    @impl Temporalex.Backend
    def signal_workflow(%ClientState{} = state, workflow_id, run_id, signal_name, args, opts)
        when is_binary(workflow_id) and is_binary(signal_name) and is_list(opts) do
      state = monitored(state, opts)
      timeout = Keyword.get(opts, :timeout, state.completion_timeout)

      with {:ok, input} <- Payloads.encode_list(List.wrap(args), state.payload_codec),
           {:ok, header} <- Payloads.header(Keyword.get(opts, :headers), state.payload_codec) do
        request = %WS.SignalWorkflowExecutionRequest{
          namespace: state.namespace,
          workflow_execution: execution(workflow_id, run_id),
          signal_name: signal_name,
          input: input,
          identity: state.identity,
          request_id: StartOptions.request_id(opts),
          header: header
        }

        interaction(state, :signal_workflow_execution, request, :workflow_signalled, timeout)
      end
    end

    @impl Temporalex.Backend
    def query_workflow(%ClientState{} = state, workflow_id, run_id, query_name, args, opts)
        when is_binary(workflow_id) and is_binary(query_name) and is_list(opts) do
      state = monitored(state, opts)
      timeout = Keyword.get(opts, :timeout, state.completion_timeout)

      with {:ok, condition} <- reject_condition(opts),
           {:ok, query_args} <- Payloads.encode_list(List.wrap(args), state.payload_codec),
           {:ok, header} <- Payloads.header(Keyword.get(opts, :headers), state.payload_codec) do
        request = %WS.QueryWorkflowRequest{
          namespace: state.namespace,
          execution: execution(workflow_id, run_id),
          query: %WorkflowQuery{query_type: query_name, query_args: query_args, header: header},
          query_reject_condition: condition
        }

        case Rpc.call(state.conn, :query_workflow, request, :workflow_queried, timeout) do
          {:ok, %{query_rejected: %{status: status}}} ->
            {:error, {:rejected, Results.status(status)}}

          {:ok, %{query_result: result}} ->
            Payloads.decode_first(result)

          {:error, reason} ->
            {:error, Rpc.interaction_reason(reason)}
        end
      end
    end

    defp reject_condition(opts) do
      case Keyword.get(opts, :query_reject_condition) || Keyword.get(opts, :reject_condition) do
        nil -> {:ok, :QUERY_REJECT_CONDITION_UNSPECIFIED}
        :unspecified -> {:ok, :QUERY_REJECT_CONDITION_UNSPECIFIED}
        :none -> {:ok, :QUERY_REJECT_CONDITION_NONE}
        :not_open -> {:ok, :QUERY_REJECT_CONDITION_NOT_OPEN}
        :not_completed_cleanly -> {:ok, :QUERY_REJECT_CONDITION_NOT_COMPLETED_CLEANLY}
        _other -> {:error, {:invalid_options, "unsupported query reject condition"}}
      end
    end

    # Execute-update, as sdk-rust does it: request with the Accepted wait stage,
    # then poll for the Completed outcome until the server has one (its own
    # long-poll can expire first and answer with no outcome).
    @impl Temporalex.Backend
    def update_workflow(%ClientState{} = state, workflow_id, run_id, update_name, args, opts)
        when is_binary(workflow_id) and is_binary(update_name) and is_list(opts) do
      state = monitored(state, opts)
      timeout = Keyword.get(opts, :timeout, state.workflow_result_timeout)
      deadline = deadline(timeout)
      update_id = to_string(Keyword.get(opts, :update_id) || StartOptions.uuid4())

      with {:ok, update_args} <- Payloads.encode_list(List.wrap(args), state.payload_codec),
           {:ok, header} <- Payloads.header(Keyword.get(opts, :headers), state.payload_codec),
           {:ok, remaining} <- remaining(deadline, :workflow_updated, timeout) do
        request = %WS.UpdateWorkflowExecutionRequest{
          namespace: state.namespace,
          workflow_execution: execution(workflow_id, run_id),
          wait_policy: %Update.WaitPolicy{
            lifecycle_stage: :UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED
          },
          request: %Update.Request{
            meta: %Update.Meta{update_id: update_id, identity: state.identity},
            input: %Update.Input{header: header, name: update_name, args: update_args}
          }
        }

        case Rpc.call(
               state.conn,
               :update_workflow_execution,
               request,
               :workflow_updated,
               remaining
             ) do
          {:ok, %{outcome: %Update.Outcome{} = outcome}} ->
            update_outcome(outcome)

          {:ok, response} ->
            ref =
              response.update_ref ||
                %Update.UpdateRef{
                  workflow_execution: execution(workflow_id, run_id),
                  update_id: update_id
                }

            poll_update(state, ref, deadline, timeout)

          {:error, {:workflow_updated, :timeout, _}} ->
            {:error, {:workflow_updated, :timeout, timeout}}

          {:error, reason} ->
            {:error, Rpc.interaction_reason(reason)}
        end
      end
    end

    defp poll_update(state, ref, deadline, timeout) do
      with {:ok, remaining} <- remaining(deadline, :workflow_updated, timeout) do
        request = %WS.PollWorkflowExecutionUpdateRequest{
          namespace: state.namespace,
          update_ref: ref,
          identity: state.identity,
          wait_policy: %Update.WaitPolicy{
            lifecycle_stage: :UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED
          }
        }

        case Rpc.call(
               state.conn,
               :poll_workflow_execution_update,
               request,
               :workflow_updated,
               remaining
             ) do
          {:ok, %{outcome: %Update.Outcome{} = outcome}} ->
            update_outcome(outcome)

          {:ok, _no_outcome_yet} ->
            poll_update(state, ref, deadline, timeout)

          {:error, {:workflow_updated, :timeout, _}} ->
            {:error, {:workflow_updated, :timeout, timeout}}

          {:error, reason} ->
            {:error, Rpc.interaction_reason(reason)}
        end
      end
    end

    defp update_outcome(%Update.Outcome{value: {:success, payloads}}),
      do: Payloads.decode_first(payloads)

    defp update_outcome(%Update.Outcome{value: {:failure, failure}}) do
      case Payloads.failure(failure) do
        {:ok, failure} -> {:error, {:failed, failure}}
        {:error, reason} -> {:error, reason}
      end
    end

    defp update_outcome(%Update.Outcome{value: nil}),
      do: {:error, {:rpc, "Update returned no outcome value"}}

    ## Cancel, terminate, describe

    @impl Temporalex.Backend
    def cancel_workflow(%ClientState{} = state, workflow_id, run_id, opts)
        when is_binary(workflow_id) and is_list(opts) do
      state = monitored(state, opts)
      timeout = Keyword.get(opts, :timeout, state.completion_timeout)

      request = %WS.RequestCancelWorkflowExecutionRequest{
        namespace: state.namespace,
        workflow_execution: execution(workflow_id, run_id),
        identity: state.identity,
        request_id: StartOptions.request_id(opts),
        first_execution_run_id: run_id || "",
        reason: to_string(Keyword.get(opts, :reason, ""))
      }

      interaction(
        state,
        :request_cancel_workflow_execution,
        request,
        :workflow_cancelled,
        timeout
      )
    end

    # TerminateWorkflowExecutionRequest has no request id, so `:request_id` is
    # carried in the request's identity and made good on resend: a terminate
    # that finds the run already closed answers :ok if the close event is a
    # termination recorded with this same request id.
    @impl Temporalex.Backend
    def terminate_workflow(%ClientState{} = state, workflow_id, run_id, opts)
        when is_binary(workflow_id) and is_list(opts) do
      state = monitored(state, opts)
      timeout = Keyword.get(opts, :timeout, state.completion_timeout)
      identity = terminate_identity(state.identity, Keyword.get(opts, :request_id))

      with {:ok, details} <- terminate_details(Keyword.get(opts, :details), state.payload_codec) do
        request = %WS.TerminateWorkflowExecutionRequest{
          namespace: state.namespace,
          workflow_execution: execution(workflow_id, run_id),
          reason: to_string(Keyword.get(opts, :reason, "")),
          details: details,
          identity: identity,
          first_execution_run_id: run_id || ""
        }

        case Rpc.call(
               state.conn,
               :terminate_workflow_execution,
               request,
               :workflow_terminated,
               timeout
             ) do
          {:ok, _} ->
            :ok

          {:error, reason} ->
            if Rpc.not_found?(reason) and Keyword.get(opts, :request_id) != nil,
              do: already_terminated_by(state, workflow_id, run_id, identity, timeout),
              else: {:error, Rpc.interaction_reason(reason)}
        end
      end
    end

    defp terminate_identity(identity, nil), do: identity
    defp terminate_identity(identity, request_id), do: "#{identity} request_id=#{request_id}"

    defp terminate_details(nil, _codec), do: {:ok, nil}
    defp terminate_details(details, codec), do: Payloads.encode_list([details], codec)

    defp already_terminated_by(state, workflow_id, run_id, identity, timeout) do
      request = %WS.GetWorkflowExecutionHistoryRequest{
        namespace: state.namespace,
        execution: execution(workflow_id, run_id),
        history_event_filter_type: :HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT,
        skip_archival: true
      }

      case Rpc.call(
             state.conn,
             :get_workflow_execution_history,
             request,
             :workflow_terminated,
             timeout
           ) do
        {:ok, %{history: %History{events: [_ | _] = events}}} ->
          case List.last(events).attributes do
            {:workflow_execution_terminated_event_attributes, %{identity: ^identity}} -> :ok
            _other -> {:error, :not_found}
          end

        {:ok, _} ->
          {:error, :not_found}

        {:error, reason} ->
          {:error, Rpc.interaction_reason(reason)}
      end
    end

    @impl Temporalex.Backend
    def describe_workflow(%ClientState{} = state, workflow_id, run_id, opts)
        when is_binary(workflow_id) and is_list(opts) do
      state = monitored(state, opts)
      timeout = Keyword.get(opts, :timeout, state.completion_timeout)

      request = %WS.DescribeWorkflowExecutionRequest{
        namespace: state.namespace,
        execution: execution(workflow_id, run_id)
      }

      case Rpc.call(
             state.conn,
             :describe_workflow_execution,
             request,
             :workflow_described,
             timeout
           ) do
        {:ok, %{workflow_execution_info: info}} when info != nil -> Results.execution(info)
        {:ok, _} -> {:error, {:rpc, "describe returned no workflow execution info"}}
        {:error, reason} -> {:error, Rpc.interaction_reason(reason)}
      end
    end

    ## History

    # Histories can be large and paginate server-side, so this borrows the
    # (longer) workflow-result timeout, as the NIF backend does. All pages are
    # read and re-encoded as one History, then decoded by the same codec the
    # NIF backend uses, so `raw: false` results are identical.
    @impl Temporalex.Backend
    def fetch_workflow_history(%ClientState{} = state, workflow_id, run_id, opts)
        when is_binary(workflow_id) and is_list(opts) do
      state = monitored(state, opts)
      timeout = Keyword.get(opts, :timeout, state.workflow_result_timeout)

      with {:ok, events} <-
             all_events(state, execution(workflow_id, run_id), "", [], deadline(timeout), timeout) do
        bytes = History.encode(%History{events: events})

        if Keyword.get(opts, :raw, false),
          do: {:ok, bytes},
          else: Codec.history_from_bytes(bytes)
      end
    end

    defp all_events(state, execution, token, acc, deadline, timeout) do
      with {:ok, remaining} <- remaining(deadline, :workflow_history_fetched, timeout) do
        request = %WS.GetWorkflowExecutionHistoryRequest{
          namespace: state.namespace,
          execution: execution,
          next_page_token: token,
          history_event_filter_type: :HISTORY_EVENT_FILTER_TYPE_ALL_EVENT
        }

        case Rpc.call(
               state.conn,
               :get_workflow_execution_history,
               request,
               :workflow_history_fetched,
               remaining
             ) do
          {:ok, response} ->
            acc = [events_of(response) | acc]

            case response.next_page_token do
              next when next in [nil, ""] -> {:ok, acc |> Enum.reverse() |> Enum.concat()}
              next -> all_events(state, execution, next, acc, deadline, timeout)
            end

          {:error, {:workflow_history_fetched, :timeout, _}} ->
            {:error, {:workflow_history_fetched, :timeout, timeout}}

          {:error, reason} ->
            {:error, Rpc.interaction_reason(reason)}
        end
      end
    end

    defp events_of(%{history: %History{events: events}}), do: events
    defp events_of(_response), do: []

    @impl Temporalex.Backend
    def fetch_history_page(%ClientState{} = state, workflow_id, run_id, opts)
        when is_binary(workflow_id) and is_list(opts) do
      state = monitored(state, opts)
      timeout = Keyword.get(opts, :timeout, state.workflow_result_timeout)

      with {:ok, filter} <- event_filter(Keyword.get(opts, :event_filter, :all)) do
        request = %WS.GetWorkflowExecutionHistoryRequest{
          namespace: state.namespace,
          execution: execution(workflow_id, run_id),
          maximum_page_size: Keyword.get(opts, :maximum_page_size, 0),
          next_page_token: Keyword.get(opts, :page_token) || "",
          wait_new_event: Keyword.get(opts, :wait_new_event, false),
          history_event_filter_type: filter
        }

        case Rpc.call(
               state.conn,
               :get_workflow_execution_history,
               request,
               :fetch_history_page,
               timeout
             ) do
          {:ok, response} ->
            {:ok,
             %{
               history: History.encode(%History{events: events_of(response)}),
               next_page_token: empty_to_nil(response.next_page_token)
             }}

          {:error, reason} ->
            {:error, Rpc.interaction_reason(reason)}
        end
      end
    end

    defp event_filter(:all), do: {:ok, :HISTORY_EVENT_FILTER_TYPE_ALL_EVENT}
    defp event_filter(:close), do: {:ok, :HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT}

    defp event_filter(other),
      do:
        {:error,
         {:invalid_options, ":event_filter must be :all or :close, got: #{inspect(other)}"}}

    ## Visibility

    @impl Temporalex.Backend
    def list_workflows(%ClientState{} = state, query, opts)
        when is_binary(query) and is_list(opts) do
      state = monitored(state, opts)
      timeout = Keyword.get(opts, :timeout, state.completion_timeout)

      request = %WS.ListWorkflowExecutionsRequest{
        namespace: state.namespace,
        query: query,
        page_size: Keyword.get(opts, :page_size, 0),
        next_page_token: Keyword.get(opts, :page_token) || ""
      }

      with {:ok, response} <-
             plain(state, :list_workflow_executions, request, :list_workflows, timeout),
           {:ok, executions} <- executions(response.executions) do
        {:ok, %{executions: executions, next_page_token: empty_to_nil(response.next_page_token)}}
      end
    end

    defp executions(infos) do
      infos
      |> Enum.reduce_while({:ok, []}, fn info, {:ok, acc} ->
        case Results.execution(info) do
          {:ok, execution} -> {:cont, {:ok, [execution | acc]}}
          {:error, _} = error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, list} -> {:ok, Enum.reverse(list)}
        error -> error
      end
    end

    ## Versioning and reset

    @impl Temporalex.Backend
    def update_workflow_options(%ClientState{} = state, workflow_id, run_id, opts)
        when is_binary(workflow_id) and is_list(opts) do
      state = monitored(state, opts)
      timeout = Keyword.get(opts, :timeout, state.completion_timeout)

      with {:ok, override} <- versioning_override(Keyword.fetch(opts, :versioning_override)) do
        request = %WS.UpdateWorkflowExecutionOptionsRequest{
          namespace: state.namespace,
          workflow_execution: execution(workflow_id, run_id),
          workflow_execution_options: %WorkflowExecutionOptions{versioning_override: override},
          update_mask: %FieldMask{paths: ["versioning_override"]},
          identity: state.identity
        }

        case Rpc.call(
               state.conn,
               :update_workflow_execution_options,
               request,
               :update_workflow_options,
               timeout
             ) do
          {:ok, response} ->
            options = response.workflow_execution_options

            {:ok,
             %{
               versioning_override:
                 Results.versioning_override(options && options.versioning_override)
             }}

          {:error, reason} ->
            {:error, Rpc.interaction_reason(reason)}
        end
      end
    end

    defp versioning_override({:ok, :unset}), do: {:ok, nil}

    defp versioning_override({:ok, :auto_upgrade}),
      do: {:ok, %VersioningOverride{override: {:auto_upgrade, true}}}

    defp versioning_override({:ok, {:pinned, deployment_name, build_id}})
         when is_binary(deployment_name) and is_binary(build_id) do
      {:ok,
       %VersioningOverride{
         override:
           {:pinned,
            %VersioningOverride.PinnedOverride{
              behavior: :PINNED_OVERRIDE_BEHAVIOR_PINNED,
              version: %WorkerDeploymentVersion{
                deployment_name: deployment_name,
                build_id: build_id
              }
            }}
       }}
    end

    defp versioning_override(other) do
      {:error,
       {:invalid_options,
        "update_workflow_options needs versioning_override: {:pinned, deployment_name, build_id}, " <>
          ":auto_upgrade or :unset, got: #{inspect(other)}"}}
    end

    @impl Temporalex.Backend
    def reset_workflow(%ClientState{} = state, workflow_id, run_id, opts)
        when is_binary(workflow_id) and is_list(opts) do
      state = monitored(state, opts)
      timeout = Keyword.get(opts, :timeout, state.completion_timeout)

      case Keyword.get(opts, :event_id) do
        event_id when is_integer(event_id) and event_id > 0 ->
          request = %WS.ResetWorkflowExecutionRequest{
            namespace: state.namespace,
            workflow_execution: execution(workflow_id, run_id),
            reason: to_string(Keyword.get(opts, :reason, "")),
            workflow_task_finish_event_id: event_id,
            request_id: StartOptions.request_id(opts),
            identity: state.identity
          }

          case Rpc.call(state.conn, :reset_workflow_execution, request, :reset_workflow, timeout) do
            {:ok, response} -> {:ok, %{run_id: response.run_id}}
            {:error, reason} -> {:error, Rpc.interaction_reason(reason)}
          end

        other ->
          {:error,
           {:invalid_options,
            "reset_workflow needs event_id: the positive id of a workflow-task-finished event, got: " <>
              inspect(other)}}
      end
    end

    ## Worker Deployments

    @impl Temporalex.Backend
    def set_worker_deployment_current_version(
          %ClientState{} = state,
          deployment_name,
          build_id,
          opts
        )
        when is_binary(deployment_name) and is_list(opts) do
      state = monitored(state, opts)
      timeout = Keyword.get(opts, :timeout, state.completion_timeout)

      request = %WS.SetWorkerDeploymentCurrentVersionRequest{
        namespace: state.namespace,
        deployment_name: deployment_name,
        build_id: build_id || "",
        conflict_token: Keyword.get(opts, :conflict_token) || "",
        identity: state.identity,
        ignore_missing_task_queues: Keyword.get(opts, :ignore_missing_task_queues, false),
        allow_no_pollers: Keyword.get(opts, :allow_no_pollers, false)
      }

      with {:ok, response} <-
             plain(
               state,
               :set_worker_deployment_current_version,
               request,
               :set_worker_deployment_current_version,
               timeout
             ) do
        {:ok,
         %{
           conflict_token: response.conflict_token,
           previous_version: Results.version(response.previous_deployment_version)
         }}
      end
    end

    @impl Temporalex.Backend
    def describe_worker_deployment(%ClientState{} = state, deployment_name, opts)
        when is_binary(deployment_name) and is_list(opts) do
      state = monitored(state, opts)
      timeout = Keyword.get(opts, :timeout, state.completion_timeout)

      request = %WS.DescribeWorkerDeploymentRequest{
        namespace: state.namespace,
        deployment_name: deployment_name
      }

      with {:ok, response} <-
             plain(
               state,
               :describe_worker_deployment,
               request,
               :describe_worker_deployment,
               timeout
             ) do
        {:ok, Results.deployment(response)}
      end
    end

    ## Helpers

    defp interaction(state, fun, request, tag, timeout) do
      case Rpc.call(state.conn, fun, request, tag, timeout) do
        {:ok, _response} -> :ok
        {:error, reason} -> {:error, Rpc.interaction_reason(reason)}
      end
    end

    defp plain(state, fun, request, tag, timeout) do
      case Rpc.call(state.conn, fun, request, tag, timeout) do
        {:ok, response} -> {:ok, response}
        {:error, reason} -> {:error, Rpc.plain_reason(reason)}
      end
    end

    defp monitored(%ClientState{conn: conn} = state, opts),
      do: %{state | conn: %{conn | monitor: Keyword.get(opts, :client_monitor)}}

    defp execution(workflow_id, run_id),
      do: %WorkflowExecution{workflow_id: workflow_id, run_id: run_id || ""}

    defp deadline(:infinity), do: :infinity
    defp deadline(timeout), do: System.monotonic_time(:millisecond) + timeout

    defp remaining(:infinity, _tag, _timeout), do: {:ok, :infinity}

    defp remaining(deadline, tag, timeout) do
      case deadline - System.monotonic_time(:millisecond) do
        left when left > 0 -> {:ok, left}
        _expired -> {:error, {tag, :timeout, timeout}}
      end
    end

    defp target(opts) do
      Keyword.get(opts, :target) ||
        Keyword.get(opts, :url) ||
        Keyword.get(opts, :address) ||
        @default_target
    end

    defp empty_to_nil(nil), do: nil
    defp empty_to_nil(""), do: nil
    defp empty_to_nil(value), do: value
  end
end
