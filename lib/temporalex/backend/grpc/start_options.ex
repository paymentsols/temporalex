if Code.ensure_loaded?(Temporal.Api.Workflowservice.V1.StartWorkflowExecutionRequest) do
  defmodule Temporalex.Backend.Grpc.StartOptions do
    @moduledoc false

    # Builds StartWorkflowExecutionRequest / SignalWithStartWorkflowExecutionRequest
    # from Client.start_workflow options, accepting the same spellings and
    # refusing the same mistakes as the NIF (workflow_start_options and friends in
    # lib.rs), so a start behaves alike on either backend. Errors are
    # {:invalid_options, message} or {:payload_conversion, message}, as there.

    alias Temporal.Api.Common.V1.Priority
    alias Temporal.Api.Common.V1.RetryPolicy
    alias Temporal.Api.Common.V1.SearchAttributes
    alias Temporal.Api.Common.V1.WorkflowType
    alias Temporal.Api.Sdk.V1.UserMetadata
    alias Temporal.Api.Taskqueue.V1.TaskQueue
    alias Temporal.Api.Workflowservice.V1, as: WS
    alias Temporalex.Backend.Grpc.Payloads
    alias Temporalex.Backend.TemporalCore.PayloadConverter

    @doc """
    The start request: `{:ok, {:start, request}}` or, with `:start_signal`,
    `{:ok, {:signal_with_start, request}}`.
    """
    def build(namespace, workflow_id, workflow_type, task_queue, input, opts, context) do
      codec = context.payload_codec

      with {:ok, input} <- Payloads.encode_list([input], codec),
           {:ok, fields} <- common_fields(opts, codec),
           {:ok, signal} <- start_signal(opts, codec) do
        base =
          Map.merge(fields, %{
            namespace: namespace,
            workflow_id: workflow_id,
            workflow_type: %WorkflowType{name: workflow_type},
            task_queue: %TaskQueue{name: task_queue, kind: :TASK_QUEUE_KIND_NORMAL},
            input: input,
            identity: context.identity,
            request_id: request_id(opts)
          })

        case signal do
          nil ->
            {:ok, {:start, struct!(WS.StartWorkflowExecutionRequest, base)}}

          {name, signal_input} ->
            request =
              WS.SignalWithStartWorkflowExecutionRequest
              |> struct!(base)
              |> Map.merge(%{signal_name: name, signal_input: signal_input})

            {:ok, {:signal_with_start, request}}
        end
      end
    end

    @doc false
    def request_id(opts) do
      case Keyword.get(opts, :request_id) do
        nil -> uuid4()
        id -> to_string(id)
      end
    end

    @doc false
    def uuid4 do
      <<a::48, _::4, b::12, _::2, c::62>> = :crypto.strong_rand_bytes(16)
      hex = Base.encode16(<<a::48, 4::4, b::12, 2::2, c::62>>, case: :lower)

      <<p1::binary-8, p2::binary-4, p3::binary-4, p4::binary-4, p5::binary-12>> = hex
      Enum.join([p1, p2, p3, p4, p5], "-")
    end

    defp common_fields(opts, codec) do
      with {:ok, {reuse, conflict}} <- id_policies(opts),
           {:ok, execution_timeout} <-
             duration(opts, [:execution_timeout, :workflow_execution_timeout]),
           {:ok, run_timeout} <- duration(opts, [:run_timeout, :workflow_run_timeout]),
           {:ok, task_timeout} <- duration(opts, [:task_timeout, :workflow_task_timeout]),
           {:ok, cron} <- optional_string(opts, :cron_schedule),
           {:ok, search_attributes} <- search_attributes(opts),
           {:ok, retry_policy} <- retry_policy(Keyword.get(opts, :retry_policy)),
           {:ok, priority} <- priority(Keyword.get(opts, :priority)),
           {:ok, header} <- Payloads.header(Keyword.get(opts, :headers), codec),
           {:ok, memo} <- Payloads.memo(Keyword.get(opts, :memo), codec),
           {:ok, summary} <- optional_string(opts, :static_summary),
           {:ok, details} <- optional_string(opts, :static_details) do
        {:ok,
         %{
           workflow_id_reuse_policy: reuse,
           workflow_id_conflict_policy: conflict,
           workflow_execution_timeout: execution_timeout,
           workflow_run_timeout: run_timeout,
           workflow_task_timeout: task_timeout,
           cron_schedule: cron || "",
           search_attributes: search_attributes,
           retry_policy: retry_policy,
           priority: priority,
           header: header,
           memo: memo,
           user_metadata: user_metadata(summary, details)
         }}
      end
    end

    defp user_metadata(nil, nil), do: nil

    defp user_metadata(summary, details),
      do: %UserMetadata{
        summary: Payloads.json_string(summary),
        details: Payloads.json_string(details)
      }

    defp start_signal(opts, codec) do
      case Keyword.get(opts, :start_signal) do
        empty when empty in [nil, []] ->
          {:ok, nil}

        signal when is_list(signal) or is_map(signal) ->
          signal_from(signal[:name], signal[:args], codec)

        _other ->
          {:error, {:invalid_options, "start_signal must be a keyword list with :name and :args"}}
      end
    end

    defp signal_from(nil, _args, _codec),
      do: {:error, {:invalid_options, "start_signal requires a name"}}

    defp signal_from(name, args, codec)
         when is_binary(name) and (is_list(args) or is_nil(args)) do
      with {:ok, input} <- Payloads.encode_list(args || [], codec) do
        {:ok, {name, input}}
      end
    end

    defp signal_from(_name, _args, _codec),
      do: {:error, {:invalid_options, "start_signal :name must be a string and :args a list"}}

    ## Workflow id policies

    # The client API dropped the deprecated reuse policy TerminateIfRunning; its
    # server-side equivalent is conflict policy TerminateExisting with reuse
    # policy AllowDuplicate, exactly as the NIF sends it.
    defp id_policies(opts) do
      with {:ok, reuse} <-
             reuse_policy(first_present(opts, [:workflow_id_reuse_policy, :id_reuse_policy])),
           {:ok, conflict} <-
             conflict_policy(
               first_present(opts, [:workflow_id_conflict_policy, :id_conflict_policy])
             ) do
        combine_policies(reuse, conflict)
      end
    end

    defp combine_policies(:terminate_if_running, conflict)
         when conflict in [
                :WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED,
                :WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING
              ],
         do:
           {:ok,
            {:WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
             :WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING}}

    defp combine_policies(:terminate_if_running, _conflict),
      do:
        {:error,
         {:invalid_options,
          "id_reuse_policy :terminate_if_running terminates the running workflow, " <>
            "which contradicts the given id_conflict_policy"}}

    defp combine_policies(reuse, conflict), do: {:ok, {reuse, conflict}}

    defp reuse_policy(nil), do: {:ok, :WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED}
    defp reuse_policy(:allow_duplicate), do: {:ok, :WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE}

    defp reuse_policy(:allow_duplicate_failed_only),
      do: {:ok, :WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY}

    defp reuse_policy(:reject_duplicate), do: {:ok, :WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE}
    defp reuse_policy(:terminate_if_running), do: {:ok, :terminate_if_running}
    defp reuse_policy(:unspecified), do: {:ok, :WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED}

    defp reuse_policy(_other),
      do: {:error, {:invalid_options, "unsupported workflow id reuse policy"}}

    defp conflict_policy(nil), do: {:ok, :WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED}
    defp conflict_policy(:fail), do: {:ok, :WORKFLOW_ID_CONFLICT_POLICY_FAIL}
    defp conflict_policy(:use_existing), do: {:ok, :WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING}

    defp conflict_policy(:terminate_existing),
      do: {:ok, :WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING}

    defp conflict_policy(:unspecified), do: {:ok, :WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED}

    defp conflict_policy(_other),
      do: {:error, {:invalid_options, "unsupported workflow id conflict policy"}}

    ## Scalars

    defp duration(opts, keys) do
      case first_present(opts, keys) do
        nil ->
          {:ok, nil}

        ms when is_integer(ms) and ms >= 0 ->
          {:ok, duration_from_ms(ms)}

        ms when is_integer(ms) ->
          {:error, {:invalid_options, "duration option must be non-negative"}}

        _other ->
          {:error, {:invalid_options, "#{hd(keys)} must be an integer number of milliseconds"}}
      end
    end

    @doc false
    def duration_from_ms(ms),
      do: %Google.Protobuf.Duration{seconds: div(ms, 1000), nanos: rem(ms, 1000) * 1_000_000}

    defp optional_string(opts, key) do
      case Keyword.get(opts, key) do
        nil -> {:ok, nil}
        value when is_binary(value) -> {:ok, value}
        _other -> {:error, {:invalid_options, "#{key} must be a string"}}
      end
    end

    # An explicit nil counts as absent, as keyword_get does in the NIF.
    defp first_present(opts, keys), do: Enum.find_value(keys, &Keyword.get(opts, &1))

    ## Search attributes

    defp search_attributes(opts) do
      case Keyword.get(opts, :search_attributes) do
        nil ->
          {:ok, nil}

        attributes ->
          case PayloadConverter.search_attributes_to_payload_map(attributes) do
            {:ok, fields} ->
              {:ok, %SearchAttributes{indexed_fields: Map.new(fields, &to_proto_payload/1)}}

            {:error, message} ->
              {:error, {:invalid_options, message}}
          end
      end
    end

    defp to_proto_payload({key, %{metadata: metadata, data: data}}),
      do: {key, %Temporal.Api.Common.V1.Payload{metadata: metadata, data: data}}

    ## Retry policy

    defp retry_policy(nil), do: {:ok, nil}

    defp retry_policy(policy) when is_list(policy) or is_map(policy) do
      with {:ok, backoff} <- backoff(policy[:backoff_coefficient]),
           {:ok, attempts} <- maximum_attempts(policy[:maximum_attempts]),
           {:ok, initial} <- interval(policy[:initial_interval], "retry_policy.initial_interval"),
           {:ok, maximum} <- interval(policy[:maximum_interval], "retry_policy.maximum_interval"),
           {:ok, types} <- string_list(policy[:non_retryable_error_types]) do
        {:ok,
         %RetryPolicy{
           initial_interval: initial,
           backoff_coefficient: backoff,
           maximum_interval: maximum,
           maximum_attempts: attempts,
           non_retryable_error_types: types
         }}
      end
    end

    defp retry_policy(_other),
      do: {:error, {:invalid_options, "retry_policy must be a keyword list"}}

    defp backoff(nil), do: {:ok, 0.0}
    defp backoff(value) when is_number(value) and value >= 1.0, do: {:ok, value * 1.0}

    defp backoff(_value),
      do: {:error, {:invalid_options, "retry_policy.backoff_coefficient must be 1.0 or larger"}}

    @max_i32 2_147_483_647
    defp maximum_attempts(nil), do: {:ok, 0}
    defp maximum_attempts(n) when is_integer(n) and n >= 0 and n <= @max_i32, do: {:ok, n}

    defp maximum_attempts(_n),
      do:
        {:error,
         {:invalid_options, "retry_policy.maximum_attempts must fit in a non-negative i32"}}

    defp interval(nil, _name), do: {:ok, nil}
    defp interval(ms, _name) when is_integer(ms) and ms >= 0, do: {:ok, duration_from_ms(ms)}
    defp interval(_ms, name), do: {:error, {:invalid_options, "#{name} must be non-negative"}}

    defp string_list(nil), do: {:ok, []}

    defp string_list(list) when is_list(list) do
      if Enum.all?(list, &is_binary/1),
        do: {:ok, list},
        else:
          {:error, {:invalid_options, "retry_policy.non_retryable_error_types must be strings"}}
    end

    defp string_list(_other),
      do: {:error, {:invalid_options, "retry_policy.non_retryable_error_types must be a list"}}

    ## Priority

    # The two hard limits Temporal documents, as the NIF validates them:
    # priority_key is 1-based, fairness_key at most 64 bytes; a non-positive
    # weight is obviously wrong (the server clamps the rest).
    defp priority(empty) when empty in [nil, []], do: {:ok, nil}

    defp priority(priority) when is_list(priority) or is_map(priority) do
      with {:ok, key} <- priority_key(priority[:priority_key]),
           {:ok, fairness_key} <- fairness_key(priority[:fairness_key]),
           {:ok, weight} <- fairness_weight(priority[:fairness_weight]) do
        {:ok, %Priority{priority_key: key, fairness_key: fairness_key, fairness_weight: weight}}
      end
    end

    defp priority(_other), do: {:error, {:invalid_options, "priority must be a keyword list"}}

    defp priority_key(nil), do: {:ok, 0}
    defp priority_key(key) when is_integer(key) and key >= 1, do: {:ok, key}

    defp priority_key(key),
      do:
        {:error,
         {:invalid_options,
          "priority.priority_key must be 1 or larger (smaller is higher priority), got #{inspect(key)}"}}

    defp fairness_key(nil), do: {:ok, ""}

    defp fairness_key(key) when is_binary(key) and byte_size(key) <= 64, do: {:ok, key}

    defp fairness_key(key) when is_binary(key),
      do:
        {:error,
         {:invalid_options, "priority.fairness_key is limited to 64 bytes, got #{byte_size(key)}"}}

    defp fairness_key(_key),
      do: {:error, {:invalid_options, "priority.fairness_key must be a string"}}

    defp fairness_weight(nil), do: {:ok, 0.0}
    defp fairness_weight(w) when is_number(w) and w > 0, do: {:ok, w * 1.0}

    defp fairness_weight(w),
      do:
        {:error,
         {:invalid_options, "priority.fairness_weight must be greater than 0, got #{inspect(w)}"}}
  end
end
