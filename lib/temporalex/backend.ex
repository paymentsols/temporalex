defmodule Temporalex.Backend do
  @moduledoc """
  Backend boundary for Temporal client and worker transport.

  Backends own client resources, worker resources, and protocol translation.
  They deliver decoded core structs to `Temporalex.Server`, accept core
  completions from it, and execute public client operations for
  `Temporalex.Client`. Backend-specific transport, protobuf, native resources,
  and worker handles must stay behind this behaviour.
  """

  @type client_state :: term()
  @type worker_state :: term()

  @callback start_client(opts :: keyword(), owner_pid :: pid()) ::
              {:ok, client_state()} | {:error, term()}

  @callback shutdown_client(client_state()) :: :ok | {:error, term()}

  @callback start_worker(client_state(), opts :: keyword(), owner_pid :: pid()) ::
              {:ok, worker_state()} | {:error, term()}

  @callback start_workflow(
              client_state(),
              workflow_type :: binary(),
              input :: term(),
              opts :: keyword()
            ) :: {:ok, map()} | {:error, term()}

  @callback get_workflow_result(
              client_state(),
              workflow_id :: binary(),
              run_id :: binary() | nil,
              opts :: keyword()
            ) :: {:ok, term()} | {:error, term()}

  @callback signal_workflow(
              client_state(),
              workflow_id :: binary(),
              run_id :: binary() | nil,
              signal_name :: binary(),
              args :: list(),
              opts :: keyword()
            ) :: :ok | {:error, term()}

  @callback query_workflow(
              client_state(),
              workflow_id :: binary(),
              run_id :: binary() | nil,
              query_name :: binary(),
              args :: list(),
              opts :: keyword()
            ) :: {:ok, term()} | {:error, term()}

  @callback update_workflow(
              client_state(),
              workflow_id :: binary(),
              run_id :: binary() | nil,
              update_name :: binary(),
              args :: list(),
              opts :: keyword()
            ) :: {:ok, term()} | {:error, term()}

  @callback cancel_workflow(
              client_state(),
              workflow_id :: binary(),
              run_id :: binary() | nil,
              opts :: keyword()
            ) :: :ok | {:error, term()}

  @callback terminate_workflow(
              client_state(),
              workflow_id :: binary(),
              run_id :: binary() | nil,
              opts :: keyword()
            ) :: :ok | {:error, term()}

  @callback describe_workflow(
              client_state(),
              workflow_id :: binary(),
              run_id :: binary() | nil,
              opts :: keyword()
            ) :: {:ok, term()} | {:error, term()}

  @doc """
  Fetches a workflow's history as an opaque binary.

  The binary is encoded protobuf, but callers must treat it as opaque: feed it
  to a replay worker or persist it as a replay fixture. It is deliberately not
  decoded into core structs — the only consumer is the replayer, which wants the
  encoded form, so decoding and re-encoding would be waste. Backend transport
  detail stays out of executor and workflow semantics either way.
  """
  @callback fetch_workflow_history(
              client_state(),
              workflow_id :: binary(),
              run_id :: binary() | nil,
              opts :: keyword()
            ) :: {:ok, binary()} | {:error, term()}

  # Optional client operations. `Temporalex.Backend.Grpc` implements them all;
  # `Temporalex.Backend.TemporalCore` answers `{:error, {:unsupported, message}}`
  # because the NIF's client has no call for them. A backend that does not
  # define one gets the same answer from `Temporalex.Client`.

  @doc """
  Fetches one page of a workflow's history.

  Options: `:page_token` (from a previous page), `:wait_new_event` (long-poll
  for events after the token; for an open run the server returns a token at
  the end of history only when this is set), `:maximum_page_size`, and
  `:event_filter` (`:all`, the default, or `:close`).

  Returns `{:ok, %{history: bytes, next_page_token: binary | nil}}`, where
  `history` is an encoded `temporal.api.history.v1.History`.
  """
  @callback fetch_history_page(
              client_state(),
              workflow_id :: binary(),
              run_id :: binary() | nil,
              opts :: keyword()
            ) :: {:ok, %{history: binary(), next_page_token: binary() | nil}} | {:error, term()}

  @doc """
  Lists workflow executions matching a visibility query.

  Options: `:page_size`, `:page_token`. Returns
  `{:ok, %{executions: [map()], next_page_token: binary | nil}}`; each
  execution has the shape `describe_workflow/4` returns.
  """
  @callback list_workflows(client_state(), query :: binary(), opts :: keyword()) ::
              {:ok, %{executions: [map()], next_page_token: binary() | nil}} | {:error, term()}

  @doc """
  Updates a workflow's execution options — today its Versioning Override.

  `opts[:versioning_override]` is `{:pinned, deployment_name, build_id}`,
  `:auto_upgrade`, or `:unset`.
  """
  @callback update_workflow_options(
              client_state(),
              workflow_id :: binary(),
              run_id :: binary() | nil,
              opts :: keyword()
            ) :: {:ok, map()} | {:error, term()}

  @doc """
  Resets a workflow to a workflow-task-finished event, starting a new run.

  `opts[:event_id]` (required) is the event to reset to; `:reason` and
  `:request_id` are optional. Returns `{:ok, %{run_id: new_run_id}}`.
  """
  @callback reset_workflow(
              client_state(),
              workflow_id :: binary(),
              run_id :: binary() | nil,
              opts :: keyword()
            ) :: {:ok, %{run_id: binary()}} | {:error, term()}

  @doc """
  Makes `build_id` the Current Version of a Worker Deployment (`nil` unsets
  it, routing new work to unversioned workers).
  """
  @callback set_worker_deployment_current_version(
              client_state(),
              deployment_name :: binary(),
              build_id :: binary() | nil,
              opts :: keyword()
            ) :: {:ok, map()} | {:error, term()}

  @doc "Describes a Worker Deployment: its routing and its versions."
  @callback describe_worker_deployment(
              client_state(),
              deployment_name :: binary(),
              opts :: keyword()
            ) :: {:ok, map()} | {:error, term()}

  @optional_callbacks fetch_history_page: 4,
                      list_workflows: 3,
                      update_workflow_options: 4,
                      reset_workflow: 4,
                      set_worker_deployment_current_version: 4,
                      describe_worker_deployment: 3

  @callback complete_workflow_activation(
              worker_state(),
              Temporalex.Core.Completion.t()
            ) :: :ok | {:error, term()}

  @callback complete_activity_task(
              worker_state(),
              Temporalex.Core.ActivityCompletion.t()
            ) :: :ok | {:error, term()}

  @callback record_activity_heartbeat(
              worker_state(),
              task_token :: binary(),
              details :: term()
            ) :: :ok | {:error, term()}

  @callback shutdown_worker(worker_state()) :: :ok | {:error, term()}
end
