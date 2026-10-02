defmodule Temporalex.ClientTlsIntegrationTest do
  @moduledoc """
  The `:tls` option against a real server. The dev server only speaks
  plaintext, so these show that TLS is actually attempted and that a
  contradictory target is refused; a full mTLS handshake needs a server with
  TLS configured and is outside this suite.
  """

  use ExUnit.Case, async: false

  @moduletag :external

  alias Temporalex.TestSupport.Backends
  alias Temporalex.TestSupport.TemporalDevServer

  setup_all do
    temporal = TemporalDevServer.start!()
    on_exit(fn -> TemporalDevServer.stop(temporal) end)
    {:ok, temporal: temporal, address: String.replace_prefix(temporal.target, "http://", "")}
  end

  # Both client backends read :tls the same way and refuse the same mistakes.
  for backend <- Backends.all() do
    describe "#{backend} backend" do
      @describetag backend: backend

      test "without :tls the plaintext server accepts the connection", %{
        address: address,
        backend: backend
      } do
        assert {:ok, _client} = Backends.module(backend).start_client([target: address], self())
      end

      test "tls: true makes a bare host:port target use TLS, which a plaintext server fails",
           %{address: address, backend: backend} do
        assert {:error, %Temporalex.TransportError{category: category}} =
                 Backends.module(backend).start_client(
                   [target: address, tls: true, connect_timeout: 5_000],
                   self()
                 )

        assert category in [:connect, :connect_timeout]
      end

      test ":tls options with an explicit http:// target are refused", %{
        temporal: temporal,
        backend: backend
      } do
        assert {:error, %Temporalex.TransportError{category: :connect, message: message}} =
                 Backends.module(backend).start_client(
                   [target: temporal.target, tls: [domain: "localhost"]],
                   self()
                 )

        assert message =~ "https"
      end
    end
  end
end
