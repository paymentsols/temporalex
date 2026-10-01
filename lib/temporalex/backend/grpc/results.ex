if Code.ensure_loaded?(Temporal.Api.Workflow.V1.WorkflowExecutionInfo) do
  defmodule Temporalex.Backend.Grpc.Results do
    @moduledoc false

    # Server responses as the plain maps Temporalex.Client returns, keyed and
    # typed as the NIF's (workflow_description_to_term, workflow_status_atom).

    alias Temporal.Api.Deployment.V1.WorkerDeploymentVersion
    alias Temporal.Api.Workflow.V1.VersioningOverride
    alias Temporal.Api.Workflow.V1.WorkflowExecutionInfo
    alias Temporalex.Backend.Grpc.Payloads

    @doc "A WorkflowExecutionInfo as the describe map."
    def execution(%WorkflowExecutionInfo{} = info) do
      with {:ok, memo} <- Payloads.decode_map(info.memo && info.memo.fields) do
        {:ok,
         %{
           workflow_id: info.execution && info.execution.workflow_id,
           run_id: info.execution && info.execution.run_id,
           workflow_type: info.type && info.type.name,
           status: status(info.status),
           task_queue: info.task_queue,
           history_length: info.history_length,
           start_time_ms: millis(info.start_time),
           execution_time_ms: millis(info.execution_time),
           close_time_ms: millis(info.close_time),
           memo: memo
         }}
      end
    end

    @statuses %{
      WORKFLOW_EXECUTION_STATUS_RUNNING: :running,
      WORKFLOW_EXECUTION_STATUS_COMPLETED: :completed,
      WORKFLOW_EXECUTION_STATUS_FAILED: :failed,
      WORKFLOW_EXECUTION_STATUS_CANCELED: :cancelled,
      WORKFLOW_EXECUTION_STATUS_TERMINATED: :terminated,
      WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW: :continued_as_new,
      WORKFLOW_EXECUTION_STATUS_TIMED_OUT: :timed_out,
      WORKFLOW_EXECUTION_STATUS_PAUSED: :paused
    }

    @doc "Execution status enum as an atom; anything unknown is :unspecified, as in the NIF."
    def status(status), do: Map.get(@statuses, status, :unspecified)

    @doc "A Timestamp as Unix milliseconds, or nil."
    def millis(nil), do: nil
    def millis(%Google.Protobuf.Timestamp{seconds: s, nanos: n}), do: s * 1000 + div(n, 1_000_000)

    @doc "A Versioning Override as `{:pinned, deployment, build_id}`, `:auto_upgrade` or `:unset`."
    def versioning_override(nil), do: :unset

    def versioning_override(%VersioningOverride{override: {:pinned, pinned}}) do
      case pinned.version do
        %WorkerDeploymentVersion{deployment_name: name, build_id: build_id} ->
          {:pinned, name, build_id}

        nil ->
          {:pinned, nil, nil}
      end
    end

    def versioning_override(%VersioningOverride{override: {:auto_upgrade, true}}),
      do: :auto_upgrade

    def versioning_override(%VersioningOverride{}), do: :unset

    @doc "A WorkerDeploymentVersion as a map, or nil."
    def version(nil), do: nil
    def version(%WorkerDeploymentVersion{build_id: ""}), do: nil

    def version(%WorkerDeploymentVersion{} = version),
      do: %{deployment_name: version.deployment_name, build_id: version.build_id}

    @doc "DescribeWorkerDeploymentResponse as a map."
    def deployment(response) do
      info = response.worker_deployment_info
      routing = info && info.routing_config

      %{
        name: info && info.name,
        conflict_token: response.conflict_token,
        current_version: routing && version(routing.current_deployment_version),
        ramping_version: routing && version(routing.ramping_deployment_version),
        ramping_percentage: routing && routing.ramping_version_percentage,
        create_time_ms: info && millis(info.create_time),
        last_modifier_identity: info && info.last_modifier_identity,
        versions: Enum.map((info && info.version_summaries) || [], &version_summary/1)
      }
    end

    defp version_summary(summary) do
      %{
        version: version(summary.deployment_version),
        status: version_status(summary.status),
        drainage_status: drainage_status(summary.drainage_status),
        create_time_ms: millis(summary.create_time),
        current_since_ms: millis(summary.current_since_time),
        ramping_since_ms: millis(summary.ramping_since_time)
      }
    end

    @version_statuses %{
      WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE: :inactive,
      WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT: :current,
      WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING: :ramping,
      WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING: :draining,
      WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED: :drained
    }

    defp version_status(status), do: Map.get(@version_statuses, status, :unspecified)

    @drainage_statuses %{
      VERSION_DRAINAGE_STATUS_DRAINING: :draining,
      VERSION_DRAINAGE_STATUS_DRAINED: :drained
    }

    defp drainage_status(status), do: Map.get(@drainage_statuses, status, :unspecified)
  end
end
