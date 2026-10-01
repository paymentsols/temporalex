if Code.ensure_loaded?(Temporal.Api.Common.V1.Payload) do
  defmodule Temporalex.Backend.Grpc.Payloads do
    @moduledoc false

    # Client-side payload conversion for the gRPC backend: terms to Temporal
    # payloads and back, and server Failure trees to Temporalex.Failure.* structs
    # shaped exactly as the NIF builds them (failure_to_term in lib.rs).
    #
    # Encoding follows the client's :payload_codec. Unlike the NIF client, which
    # always sends ETF, :json here means json/plain or an error — a value JSON
    # cannot represent is refused, never sent as ETF behind the caller's back.
    # Decoding accepts both json/plain and binary/erlang-eterm whatever the
    # codec, and decodes ETF with :safe so a payload cannot mint atoms.

    alias Temporal.Api.Common.V1.Header
    alias Temporal.Api.Common.V1.Memo
    alias Temporal.Api.Common.V1.Payload
    alias Temporal.Api.Common.V1.Payloads, as: PayloadList
    alias Temporal.Api.Failure.V1.Failure

    alias Temporalex.Failure.ActivityError
    alias Temporalex.Failure.ApplicationError
    alias Temporalex.Failure.CancelledError
    alias Temporalex.Failure.TimeoutError
    alias Temporalex.Failure.UnknownError
    alias Temporalex.Failure.WorkflowExecutionError

    @etf "binary/erlang-eterm"
    @json "json/plain"
    @plain "binary/plain"
    @null "binary/null"

    ## Encoding

    @doc "Encodes one term as a payload under `codec` (`:etf` | `:json`)."
    def encode(term, :etf),
      do: {:ok, %Payload{metadata: %{"encoding" => @etf}, data: :erlang.term_to_binary(term)}}

    def encode(term, :json) do
      case Jason.encode(term) do
        {:ok, data} ->
          {:ok, %Payload{metadata: %{"encoding" => @json}, data: data}}

        {:error, _reason} ->
          # The value is deliberately not inspected into the message: payloads
          # carry business data, and errors end up in logs.
          {:error,
           {:payload_conversion,
            "payload_codec: :json cannot represent #{describe(term)}; send " <>
              "JSON-compatible data (maps, lists, strings, numbers, booleans, nil) " <>
              "or start the client with payload_codec: :etf"}}
      end
    end

    @doc "Encodes each term of a list as one payload."
    def encode_list(terms, codec) when is_list(terms) do
      terms
      |> Enum.reduce_while({:ok, []}, fn term, {:ok, acc} ->
        case encode(term, codec) do
          {:ok, payload} -> {:cont, {:ok, [payload | acc]}}
          {:error, _} = error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, payloads} -> {:ok, %PayloadList{payloads: Enum.reverse(payloads)}}
        error -> error
      end
    end

    @doc "Encodes a map of terms as a map of payloads with string keys."
    def encode_map(nil, _codec), do: {:ok, %{}}

    def encode_map(map, codec) when is_map(map) do
      Enum.reduce_while(map, {:ok, %{}}, fn {key, value}, {:ok, acc} ->
        case encode(value, codec) do
          {:ok, payload} -> {:cont, {:ok, Map.put(acc, to_string(key), payload)}}
          {:error, _} = error -> {:halt, error}
        end
      end)
    end

    def encode_map(_other, _codec), do: {:error, {:invalid_options, "expected a map"}}

    @doc "Header from the `:headers` option; nil when there are none."
    def header(headers, codec) do
      case encode_map(headers, codec) do
        {:ok, fields} when map_size(fields) == 0 ->
          {:ok, nil}

        {:ok, fields} ->
          {:ok, %Header{fields: fields}}

        {:error, {:invalid_options, _}} ->
          {:error, {:invalid_options, "headers option must be a map"}}

        error ->
          error
      end
    end

    @doc "Memo from the `:memo` option; nil when there is none."
    def memo(memo, codec) do
      case encode_map(memo, codec) do
        {:ok, fields} when map_size(fields) == 0 ->
          {:ok, nil}

        {:ok, fields} ->
          {:ok, %Memo{fields: fields}}

        {:error, {:invalid_options, _}} ->
          {:error, {:invalid_options, "memo option must be a map"}}

        error ->
          error
      end
    end

    @doc "A string as a json/plain payload — how Temporal SDKs encode user metadata."
    def json_string(nil), do: nil

    def json_string(text) when is_binary(text),
      do: %Payload{metadata: %{"encoding" => @json}, data: Jason.encode!(text)}

    defp describe(term) when is_tuple(term), do: "a tuple"
    defp describe(term) when is_pid(term), do: "a pid"
    defp describe(term) when is_reference(term), do: "a reference"
    defp describe(term) when is_function(term), do: "a function"
    defp describe(term) when is_port(term), do: "a port"
    defp describe(%{__struct__: module}), do: "a #{inspect(module)} struct"
    defp describe(term) when is_binary(term), do: "a binary that is not valid UTF-8"

    defp describe(term) when is_list(term),
      do: "a list holding a non-JSON value (a keyword list?)"

    defp describe(term) when is_map(term), do: "a map holding a non-JSON key or value"
    defp describe(_term), do: "this value"

    ## Decoding

    @doc "Decodes one payload to a term."
    def decode(%Payload{data: data}) when data in [nil, ""], do: {:ok, nil}

    def decode(%Payload{metadata: %{"encoding" => @json}, data: data}) do
      case Jason.decode(data) do
        {:ok, term} ->
          {:ok, term}

        {:error, error} ->
          {:error, {:payload_conversion, "json/plain decode: #{Exception.message(error)}"}}
      end
    end

    def decode(%Payload{metadata: %{"encoding" => @null}}), do: {:ok, nil}
    def decode(%Payload{metadata: %{"encoding" => @plain}, data: data}), do: {:ok, data}

    def decode(%Payload{data: data}) do
      {:ok, :erlang.binary_to_term(data, [:safe])}
    rescue
      ArgumentError ->
        {:error,
         {:payload_conversion,
          "payload is not ETF encoded, or names atoms this node does not know " <>
            "(results are decoded with binary_to_term(data, [:safe]))"}}
    end

    @doc "Decodes a payload list to a list of terms."
    def decode_list(nil), do: {:ok, []}
    def decode_list(%PayloadList{payloads: payloads}), do: decode_all(payloads)

    @doc "First payload's term, or nil when there is none — the NIF's result rule."
    def decode_first(nil), do: {:ok, nil}
    def decode_first(%PayloadList{payloads: []}), do: {:ok, nil}
    def decode_first(%PayloadList{payloads: [payload | _]}), do: decode(payload)

    @doc "Decodes a map of payloads (a memo's fields)."
    def decode_map(nil), do: {:ok, %{}}

    def decode_map(fields) when is_map(fields) do
      Enum.reduce_while(fields, {:ok, %{}}, fn {key, payload}, {:ok, acc} ->
        case decode(payload) do
          {:ok, term} -> {:cont, {:ok, Map.put(acc, key, term)}}
          {:error, _} = error -> {:halt, error}
        end
      end)
    end

    defp decode_all(payloads) do
      payloads
      |> Enum.reduce_while({:ok, []}, fn payload, {:ok, acc} ->
        case decode(payload) do
          {:ok, term} -> {:cont, {:ok, [term | acc]}}
          {:error, _} = error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, terms} -> {:ok, Enum.reverse(terms)}
        error -> error
      end
    end

    ## Failures

    @doc "A server Failure tree as Temporalex.Failure.* structs, as the NIF builds it."
    def failure(nil), do: {:ok, nil}

    def failure(%Failure{} = failure) do
      with {:ok, cause} <- failure(failure.cause) do
        failure_struct(failure.failure_info, failure, cause)
      end
    end

    defp failure_struct({:application_failure_info, info}, failure, cause) do
      with {:ok, details} <- decode_list(info.details) do
        {:ok,
         %ApplicationError{
           message: failure.message,
           source: failure.source,
           stack_trace: failure.stack_trace,
           type: info.type,
           details: details,
           retryable?: not info.non_retryable,
           cause: cause
         }}
      end
    end

    defp failure_struct({:canceled_failure_info, info}, failure, cause) do
      with {:ok, details} <- decode_list(info.details) do
        {:ok,
         %CancelledError{
           message: failure.message,
           source: failure.source,
           stack_trace: failure.stack_trace,
           identity: info.identity,
           details: details,
           cause: cause
         }}
      end
    end

    defp failure_struct({:timeout_failure_info, info}, failure, cause) do
      with {:ok, details} <- decode_list(info.last_heartbeat_details) do
        {:ok,
         %TimeoutError{
           message: failure.message,
           source: failure.source,
           stack_trace: failure.stack_trace,
           timeout_type: timeout_type(info.timeout_type),
           last_heartbeat_details: details,
           cause: cause
         }}
      end
    end

    defp failure_struct({:activity_failure_info, info}, failure, cause) do
      {:ok,
       %ActivityError{
         message: failure.message,
         source: failure.source,
         stack_trace: failure.stack_trace,
         identity: info.identity,
         activity_id: info.activity_id,
         activity_type: name_of(info.activity_type),
         retry_state: retry_state(info.retry_state),
         cause: cause
       }}
    end

    defp failure_struct({:child_workflow_execution_failure_info, info}, failure, cause) do
      execution = info.workflow_execution

      {:ok,
       %WorkflowExecutionError{
         message: failure.message,
         source: failure.source,
         stack_trace: failure.stack_trace,
         namespace: info.namespace,
         workflow_id: if(execution, do: execution.workflow_id, else: ""),
         run_id: if(execution, do: execution.run_id, else: ""),
         workflow_type: name_of(info.workflow_type),
         retry_state: retry_state(info.retry_state),
         cause: cause
       }}
    end

    defp failure_struct(other, failure, cause) do
      {:ok,
       %UnknownError{
         message: failure.message,
         source: failure.source,
         stack_trace: failure.stack_trace,
         failure_type: failure_type(other),
         cause: cause
       }}
    end

    defp name_of(nil), do: ""
    defp name_of(%{name: name}), do: name

    defp failure_type({:timeout_failure_info, _}), do: :timeout_failure
    defp failure_type({:canceled_failure_info, _}), do: :cancelled_failure
    defp failure_type({:terminated_failure_info, _}), do: :terminated_failure
    defp failure_type({:server_failure_info, _}), do: :server_failure
    defp failure_type({:reset_workflow_failure_info, _}), do: :reset_workflow_failure
    defp failure_type({:activity_failure_info, _}), do: :activity_failure
    defp failure_type({:child_workflow_execution_failure_info, _}), do: :child_workflow_failure
    defp failure_type({:nexus_operation_execution_failure_info, _}), do: :nexus_operation_failure
    defp failure_type({:nexus_handler_failure_info, _}), do: :nexus_handler_failure
    defp failure_type({:application_failure_info, _}), do: :failed
    defp failure_type(_none), do: :unknown_failure

    @retry_states %{
      RETRY_STATE_IN_PROGRESS: :in_progress,
      RETRY_STATE_NON_RETRYABLE_FAILURE: :non_retryable_failure,
      RETRY_STATE_TIMEOUT: :timeout,
      RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED: :maximum_attempts_reached,
      RETRY_STATE_RETRY_POLICY_NOT_SET: :retry_policy_not_set,
      RETRY_STATE_INTERNAL_SERVER_ERROR: :internal_server_error,
      RETRY_STATE_CANCEL_REQUESTED: :cancel_requested
    }

    defp retry_state(state), do: Map.get(@retry_states, state, :unspecified)

    @timeout_types %{
      TIMEOUT_TYPE_START_TO_CLOSE: :start_to_close,
      TIMEOUT_TYPE_SCHEDULE_TO_START: :schedule_to_start,
      TIMEOUT_TYPE_SCHEDULE_TO_CLOSE: :schedule_to_close,
      TIMEOUT_TYPE_HEARTBEAT: :heartbeat
    }

    defp timeout_type(type), do: Map.get(@timeout_types, type, :unspecified)
  end
end
