defmodule Temporalex.ClientTlsOptionsTest do
  @moduledoc """
  The `:tls` client option is read and checked in Elixir before anything
  crosses the NIF boundary, so these need no Temporal server.
  """

  use ExUnit.Case, async: true

  alias Temporalex.TestSupport.Backends

  # Both client backends read :tls the same way and refuse the same mistakes.
  for backend <- Backends.all() do
    describe "#{backend} backend" do
      @describetag backend: backend

      @tag :tmp_dir
      test "a missing PEM file is an invalid option, named with its path", %{
        tmp_dir: dir,
        backend: backend
      } do
        path = Path.join(dir, "missing.pem")

        assert {:error, %Temporalex.TransportError{category: :invalid_options, message: message}} =
                 Backends.module(backend).start_client([tls: [client_cert_file: path]], self())

        assert message =~ ":client_cert_file"
        assert message =~ path
      end

      test "a PEM given both inline and as a file is refused", %{backend: backend} do
        assert {:error, %Temporalex.TransportError{category: :invalid_options, message: message}} =
                 Backends.module(backend).start_client(
                   [tls: [server_root_ca_cert: "pem", server_root_ca_cert_file: "ca.pem"]],
                   self()
                 )

        assert message =~ ":server_root_ca_cert"
      end

      test "a :tls value that is not a keyword list or boolean is refused", %{backend: backend} do
        assert {:error, %Temporalex.TransportError{category: :invalid_options}} =
                 Backends.module(backend).start_client([tls: "yes"], self())
      end
    end
  end
end
