defmodule Temporalex.CoreReplayIntegrationTest do
  @moduledoc """
  `Temporalex.Replay.replay_histories/2` against histories recorded from a
  real server: sdk-core's replayer drives the production executor, so the
  history kinds the Elixir-only replayer refuses (patch markers, child
  workflows, updates, continue-as-new) replay, and a changed workflow is
  reported as nondeterministic.
  """

  use ExUnit.Case, async: false

  @moduletag :external

  alias Temporalex.Client
  alias Temporalex.Replay
  alias Temporalex.TestSupport.TemporalDevServer
  alias Temporalex.Workflow.API

  defmodule Activities do
    use Temporalex.Activity, start_to_close_timeout: 10_000

    defactivity echo(value), name: "core_replay.echo" do
      {:ok, value}
    end

    defactivity other(value), name: "core_replay.other" do
      {:ok, value}
    end
  end

  defmodule Child do
    use Temporalex.Workflow, name: "CoreReplay.Child"

    def run(n), do: {:ok, n * 2}
  end

  defmodule Rich do
    use Temporalex.Workflow, name: "CoreReplay.Rich"

    def run(n) do
      patched = API.patched?("core-replay-v2")
      {:ok, echoed} = Activities.echo(n)
      :ok = API.sleep(100)

      {:ok, doubled} =
        API.execute_child_workflow(Child, [n], workflow_id: "core-replay-child-" <> API.uuid4())

      total =
        API.phase!(doubled,
          update: %{"add" => fn [k], total -> {:reply, total + k, total + k} end},
          signal: %{"done" => fn _args, total -> {:stop, total} end}
        )

      {:ok, %{patched: patched, echoed: echoed, total: total}}
    end
  end

  # The same workflow type with a changed first step: replaying Rich's
  # history against it must diverge.
  defmodule RichChanged do
    use Temporalex.Workflow, name: "CoreReplay.Rich"

    def run(n) do
      _patched = API.patched?("core-replay-v2")
      {:ok, _other} = Activities.other(n)
      {:ok, :changed}
    end
  end

  defmodule Continuing do
    use Temporalex.Workflow, name: "CoreReplay.Continuing"

    def run({0, task_queue}), do: API.continue_as_new!({1, task_queue}, task_queue: task_queue)

    def run({1, _task_queue}) do
      {:ok, :continued} = Activities.echo(:continued)
      {:ok, :continued}
    end
  end

  setup_all do
    temporal = TemporalDevServer.start!()
    client = Module.concat(__MODULE__, :"Client#{System.unique_integer([:positive])}")
    worker = Module.concat(__MODULE__, :"Worker#{System.unique_integer([:positive])}")
    task_queue = "core-replay-#{System.unique_integer([:positive])}"

    {:ok, client_pid} =
      Client.start_link(
        name: client,
        backend: Temporalex.Backend.TemporalCore,
        target: temporal.target,
        namespace: "default",
        task_queue: task_queue
      )

    {:ok, worker_pid} =
      Temporalex.Worker.start_link(
        name: worker,
        client: client,
        task_queue: task_queue,
        workflows: [Rich, Child, Continuing],
        activities: [Activities]
      )

    histories = record_histories(client, task_queue)

    on_exit(fn ->
      try do
        if Process.alive?(worker_pid), do: Supervisor.stop(worker_pid, :normal, 15_000)
        if Process.alive?(client_pid), do: GenServer.stop(client_pid, :normal, 15_000)
      catch
        :exit, _ -> :ok
      end

      TemporalDevServer.stop(temporal)
    end)

    {:ok, histories: histories}
  end

  test "unchanged code replays every recorded history", %{histories: histories} do
    assert {:ok, results} =
             Replay.replay_histories(histories, workflows: [Rich, Child, Continuing])

    assert Enum.map(results, & &1.workflow_id) == Enum.map(histories, &elem(&1, 0))
    assert Enum.all?(results, &(&1.result == :ok)), inspect(results, pretty: true)
    assert Enum.all?(results, &is_binary(&1.run_id))
  end

  test "a changed workflow is reported as nondeterministic, per history and in order",
       %{histories: histories} do
    [{rich_id, _} = rich | _] = histories
    continued = List.keyfind(histories, "core-replay-continuing:run-2", 0)

    assert {:ok, [first, second]} =
             Replay.replay_histories([rich, continued], workflows: [RichChanged, Continuing])

    assert first.workflow_id == rich_id
    assert {:error, {:nondeterminism, message}} = first.result
    assert is_binary(message)
    assert second.result == :ok
  end

  test "bytes that are not a history are refused before replay" do
    assert {:error, {:replay_push_failed, 1, _reason}} =
             Replay.replay_histories(["not a history"], workflows: [Rich])
  end

  defp record_histories(client, task_queue) do
    rich =
      for n <- [1, 2] do
        workflow_id = "core-replay-rich-#{n}-#{System.unique_integer([:positive])}"
        {:ok, handle} = Client.start_workflow(client, Rich, n, workflow_id: workflow_id)

        {:ok, _total} =
          eventually(fn -> Client.update_workflow(handle, "add", [3], timeout: 10_000) end)

        :ok = Client.signal_workflow(handle, "done", [], timeout: 10_000)
        {:ok, %{patched: true}} = Client.get_result(handle, timeout: 30_000)
        {workflow_id, raw_history(handle)}
      end

    workflow_id = "core-replay-continuing-#{System.unique_integer([:positive])}"

    {:ok, first_run} =
      Client.start_workflow(client, Continuing, {0, task_queue}, workflow_id: workflow_id)

    latest_run = %{first_run | run_id: nil}
    {:ok, :continued} = Client.get_result(latest_run, timeout: 30_000)

    rich ++
      [
        {"core-replay-continuing:run-1", raw_history(first_run)},
        {"core-replay-continuing:run-2", raw_history(latest_run)}
      ]
  end

  defp raw_history(handle) do
    {:ok, bytes} = Client.fetch_workflow_history(handle, raw: true, timeout: 15_000)
    bytes
  end

  defp eventually(fun, attempts \\ 50) do
    case fun.() do
      {:ok, _} = ok ->
        ok

      error when attempts == 0 ->
        error

      _not_yet ->
        Process.sleep(100)
        eventually(fun, attempts - 1)
    end
  end
end
