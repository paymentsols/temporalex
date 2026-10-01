if Code.ensure_loaded?(Temporal.Api.Workflowservice.V1.WorkflowService.Stub) do
  defmodule Temporalex.Backend.Grpc.Rpc do
    @moduledoc false

    # One unary WorkflowService call, and the mapping of its failures onto the
    # reasons the NIF reports, so Temporalex.Error normalizes both backends to the
    # same public errors:
    #
    #   NOT_FOUND           -> :not_found            (interaction calls)
    #   ALREADY_EXISTS      -> {:already_started, run_id}   (start calls)
    #   DEADLINE_EXCEEDED   -> {tag, :timeout, ms}   (the NIF's await timeout)
    #   anything else       -> {:rpc, message}

    alias Temporal.Api.Errordetails.V1.WorkflowExecutionAlreadyStartedFailure
    alias Temporal.Api.Workflowservice.V1.WorkflowService.Stub

    @not_found 5
    @already_exists 6
    @deadline_exceeded 4

    @already_started_type "type.googleapis.com/temporal.api.errordetails.v1.WorkflowExecutionAlreadyStartedFailure"

    # Grace on top of the call's own timeout before the isolating process is
    # abandoned: the gRPC deadline normally fires first and reports itself.
    @grace 5_000

    @doc """
    Calls `fun` on the WorkflowService with `request`.

    The call runs in its own monitored process. grpc's Mint adapter links a
    response process to whoever makes the call, so a caller that traps exits
    (a GenServer, the client itself) would otherwise collect stray
    `{:EXIT, pid, :normal}` messages the NIF backend never sends. Isolating it
    also lets the wait watch `conn.monitor` — the `:client_monitor` the client
    stamps on each operation — and answer `{:client_down, reason}` as the NIF
    backend's await does.

    Returns `{:ok, response}` or `{:error, %GRPC.RPCError{} | {tag, :timeout, ms} |
    {:rpc, message} | {:client_down, reason}}`.
    """
    def call(conn, fun, request, tag, timeout) do
      {pid, ref} =
        spawn_monitor(fn -> exit({:rpc_result, unary(conn, fun, request, tag, timeout)}) end)

      await(pid, ref, tag, timeout, conn.monitor)
    end

    defp await(pid, ref, tag, timeout, monitor) do
      receive do
        {:DOWN, ^ref, :process, ^pid, {:rpc_result, result}} ->
          result

        {:DOWN, ^ref, :process, ^pid, reason} ->
          {:error, {:rpc, "gRPC call crashed: #{inspect(reason)}"}}

        {:DOWN, client_ref, :process, client_pid, reason}
        when monitor == {client_pid, client_ref} ->
          abandon(pid, ref)
          {:error, {:client_down, reason}}
      after
        wait_limit(timeout) ->
          abandon(pid, ref)
          {:error, {tag, :timeout, timeout}}
      end
    end

    defp wait_limit(:infinity), do: :infinity
    defp wait_limit(timeout), do: timeout + @grace

    defp abandon(pid, ref) do
      Process.demonitor(ref, [:flush])
      Process.exit(pid, :kill)
    end

    defp unary(conn, fun, request, tag, timeout) do
      opts = [metadata: conn.metadata, timeout: timeout]

      case apply(Stub, fun, [conn.channel, request, opts]) do
        {:ok, response} -> {:ok, response}
        {:error, %GRPC.RPCError{status: @deadline_exceeded}} -> {:error, {tag, :timeout, timeout}}
        {:error, %GRPC.RPCError{} = error} -> {:error, error}
        {:error, :timeout} -> {:error, {tag, :timeout, timeout}}
        {:error, other} -> {:error, {:rpc, message(other)}}
      end
    rescue
      error -> {:error, {:rpc, Exception.message(error)}}
    catch
      :exit, reason -> {:error, {:rpc, "gRPC call exited: #{inspect(reason)}"}}
    end

    @doc "Failure reason for a call on an existing workflow: NOT_FOUND is :not_found."
    def interaction_reason(%GRPC.RPCError{status: @not_found}), do: :not_found
    def interaction_reason(%GRPC.RPCError{} = error), do: {:rpc, message(error)}
    def interaction_reason(reason), do: reason

    @doc "Failure reason for a start: ALREADY_EXISTS is {:already_started, run_id}."
    def start_reason(%GRPC.RPCError{status: @already_exists} = error),
      do: {:already_started, already_started_run_id(error.details)}

    def start_reason(%GRPC.RPCError{} = error), do: {:rpc, message(error)}
    def start_reason(reason), do: reason

    @doc "Failure reason with no special statuses."
    def plain_reason(%GRPC.RPCError{} = error), do: {:rpc, message(error)}
    def plain_reason(reason), do: reason

    @doc false
    def not_found?(%GRPC.RPCError{status: @not_found}), do: true
    def not_found?(_), do: false

    defp already_started_run_id(details) when is_list(details) do
      Enum.find_value(details, fn
        %{type_url: @already_started_type, value: value} ->
          case WorkflowExecutionAlreadyStartedFailure.decode(value) do
            %{run_id: run_id} when run_id != "" -> run_id
            _ -> nil
          end

        _ ->
          nil
      end)
    rescue
      _ -> nil
    end

    defp already_started_run_id(_details), do: nil

    defp message(%GRPC.RPCError{status: status, message: message}),
      do: "#{status_name(status)}: #{message}"

    defp message(other) when is_binary(other), do: other
    defp message(other), do: inspect(other)

    defp status_name(status) when is_integer(status) do
      GRPC.Status.code_name(status)
    rescue
      _ -> "status #{status}"
    end

    defp status_name(status), do: to_string(status)
  end
end
