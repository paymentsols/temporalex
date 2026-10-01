defmodule Temporalex.TestSupport.Backends do
  @moduledoc false

  # The client integration suites run once per client backend that is compiled
  # in: the NIF backend (Temporalex.Backend.TemporalCore) always, the pure-Elixir
  # gRPC backend (Temporalex.Backend.Grpc) when its optional deps are present.
  #
  # Workers run on the NIF only, so a suite starts one client per backend: the
  # :nif client hosts the worker, and every client sends the operations under
  # test to the same namespace and task queue. Tests then read the client for
  # their backend from the context, set by a describe-level tag:
  #
  #     setup_all do
  #       clients = Backends.start_clients(__MODULE__, target: ..., task_queue: tq)
  #       {:ok, _} = Temporalex.Worker.start_link(client: clients.nif, ...)
  #       {:ok, clients: clients}
  #     end
  #
  #     setup %{clients: clients, backend: backend}, do: {:ok, client: clients[backend]}
  #
  #     for backend <- Backends.all() do
  #       describe "#{backend} backend" do
  #         @describetag backend: backend
  #         test "...", %{client: client} do ... end
  #       end
  #     end

  @doc "Backends to run client suites against."
  def all do
    if Code.ensure_loaded?(Temporalex.Backend.Grpc), do: [:nif, :grpc], else: [:nif]
  end

  def module(:nif), do: Temporalex.Backend.TemporalCore
  def module(:grpc), do: Temporalex.Backend.Grpc

  @doc """
  Starts one client per backend with `opts`, registered under unique names.

  Returns `%{nif: name, grpc: name}` (grpc only when compiled in); the clients
  are stopped when the calling test process — or `setup_all` — exits.
  """
  def start_clients(prefix, opts) do
    for backend <- all(), into: %{} do
      name = Module.concat(prefix, :"Client#{backend}#{System.unique_integer([:positive])}")

      {:ok, pid} =
        Temporalex.Client.start_link([name: name, backend: module(backend)] ++ opts)

      ExUnit.Callbacks.on_exit(fn -> stop(pid) end)
      {backend, name}
    end
  end

  defp stop(pid) do
    if Process.alive?(pid), do: GenServer.stop(pid, :normal, 5_000)
  catch
    :exit, _ -> :ok
  end
end
