defmodule Temporalex.ClientTlsOptionsTest do
  @moduledoc """
  The `:tls` client option is read and checked in Elixir before anything
  crosses the NIF boundary, so these need no Temporal server.
  """

  use ExUnit.Case, async: true

  alias Temporalex.Backend.TemporalCore

  @tag :tmp_dir
  test "a missing PEM file is an invalid option, named with its path", %{tmp_dir: dir} do
    path = Path.join(dir, "missing.pem")

    assert {:error, %Temporalex.TransportError{category: :invalid_options, message: message}} =
             TemporalCore.start_client([tls: [client_cert_file: path]], self())

    assert message =~ ":client_cert_file"
    assert message =~ path
  end

  test "a PEM given both inline and as a file is refused" do
    assert {:error, %Temporalex.TransportError{category: :invalid_options, message: message}} =
             TemporalCore.start_client(
               [tls: [server_root_ca_cert: "pem", server_root_ca_cert_file: "ca.pem"]],
               self()
             )

    assert message =~ ":server_root_ca_cert"
  end

  test "a :tls value that is not a keyword list or boolean is refused" do
    assert {:error, %Temporalex.TransportError{category: :invalid_options}} =
             TemporalCore.start_client([tls: "yes"], self())
  end
end
