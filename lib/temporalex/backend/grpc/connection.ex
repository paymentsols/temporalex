if Code.ensure_loaded?(GRPC.Stub) do
  defmodule Temporalex.Backend.Grpc.Connection do
    @moduledoc false

    # Opens the gRPC channel for Temporalex.Backend.Grpc: target parsing with the
    # NIF's rules, TLS from the shared :tls reader, the bearer token and custom
    # headers as call metadata, and a watcher that ties the channel's lifetime to
    # the owning client process.

    @default_port 7233

    # `monitor` is per operation: the {pid, ref} the client stamps as
    # :client_monitor, copied in by the backend so Rpc.call can watch it.
    defstruct [:channel, :watcher, :metadata, :monitor]

    @doc """
    Connects to `target` with the already-read `tls` option.

    Returns `{:ok, %Connection{}}` or `{:error, {:connect_error, message}}`.
    """
    def open(target, tls, opts, owner_pid) do
      with {:ok, address, scheme} <- endpoint(target, tls),
           {:ok, cred} <- credential(scheme, tls, address),
           {:ok, channel} <- connect(address, cred, opts) do
        {:ok,
         %__MODULE__{
           channel: channel,
           watcher: watch(owner_pid, channel),
           metadata: metadata(opts)
         }}
      end
    end

    @doc "Closes the channel and stops the watcher."
    def close(%__MODULE__{channel: channel, watcher: watcher}) do
      send(watcher, :stop)
      disconnect(channel)
      :ok
    end

    # The channel lives under GRPC.Client.Supervisor, not under the client
    # process, so a client killed without running terminate/2 would leak it —
    # and leave callers waiting out their timeouts on a channel nobody owns.
    defp watch(owner_pid, channel) do
      spawn(fn ->
        ref = Process.monitor(owner_pid)

        receive do
          {:DOWN, ^ref, :process, _pid, _reason} -> disconnect(channel)
          :stop -> :ok
        end
      end)
    end

    defp disconnect(channel) do
      GRPC.Stub.disconnect(channel)
    catch
      _kind, _reason -> :ok
    end

    ## Target

    # The NIF's rules (target_url in lib.rs): a bare host:port is https when TLS
    # options are given and http otherwise; an explicit http:// target with TLS
    # options is a contradiction and is refused.
    @doc false
    def endpoint(target, tls) when is_binary(target) do
      {scheme, rest} =
        case String.split(target, "://", parts: 2) do
          [scheme, rest] -> {String.downcase(scheme), rest}
          [rest] -> {if(tls, do: "https", else: "http"), rest}
        end

      authority = rest |> String.split("/", parts: 2) |> hd()

      cond do
        scheme == "http" and tls != nil ->
          {:error, {:connect_error, ":tls options need an https target, got #{inspect(target)}"}}

        scheme not in ["http", "https"] ->
          {:error, {:connect_error, "unsupported target scheme in #{inspect(target)}"}}

        true ->
          host_port(authority, scheme, target)
      end
    end

    defp host_port(authority, scheme, target) do
      uri = URI.parse("#{scheme}://#{authority}")

      case uri do
        %URI{host: host} when host in [nil, ""] ->
          {:error, {:connect_error, "invalid target #{inspect(target)}"}}

        %URI{host: host, port: port} ->
          port = if String.contains?(authority, ":"), do: port, else: @default_port
          {:ok, "#{host}:#{port}", scheme_atom(scheme)}
      end
    end

    defp scheme_atom("http"), do: :http
    defp scheme_atom("https"), do: :https

    ## TLS

    defp credential(:http, _tls, _address), do: {:ok, nil}

    defp credential(:https, tls, address) do
      tls = tls || []
      host = address |> String.split(":") |> hd()

      with {:ok, ca} <- ca_certs(tls[:server_root_ca_cert]),
           {:ok, client} <- client_identity(tls[:client_cert], tls[:client_private_key]) do
        server_name = String.to_charlist(tls[:domain] || host)

        ssl =
          [
            verify: :verify_peer,
            depth: 99,
            cacerts: ca,
            server_name_indication: server_name,
            customize_hostname_check: [
              match_fun: :public_key.pkix_verify_hostname_match_fun(:https)
            ]
          ] ++ client

        {:ok, GRPC.Credential.new(ssl: ssl)}
      end
    end

    defp ca_certs(nil) do
      {:ok, :public_key.cacerts_get()}
    rescue
      error ->
        {:error, {:connect_error, "no system CA certificates: #{Exception.message(error)}"}}
    end

    defp ca_certs(pem) do
      case for {:Certificate, der, _} <- :public_key.pem_decode(pem), do: der do
        [] -> {:error, {:connect_error, ":tls :server_root_ca_cert holds no PEM certificate"}}
        ders -> {:ok, ders}
      end
    end

    defp client_identity(nil, nil), do: {:ok, []}

    defp client_identity(cert_pem, key_pem) when is_binary(cert_pem) and is_binary(key_pem) do
      certs = for {:Certificate, der, _} <- :public_key.pem_decode(cert_pem), do: der

      keys =
        for {type, der, _} <- :public_key.pem_decode(key_pem),
            type in [:RSAPrivateKey, :ECPrivateKey, :PrivateKeyInfo, :DSAPrivateKey],
            do: {type, der}

      case {certs, keys} do
        {[cert | _chain], [key | _]} ->
          {:ok, [cert: cert, key: key]}

        {[], _} ->
          {:error, {:connect_error, ":tls :client_cert holds no PEM certificate"}}

        {_, []} ->
          {:error,
           {:connect_error, ":tls :client_private_key holds no unencrypted PEM private key"}}
      end
    end

    defp client_identity(_cert, _key),
      do: {:error, {:connect_error, ":tls needs :client_cert and :client_private_key together"}}

    ## Channel

    defp connect(address, cred, opts) do
      connect_opts =
        [
          adapter: GRPC.Client.Adapters.Mint,
          adapter_opts: [retry: Keyword.get(opts, :reconnect_attempts, 10)],
          connect_timeout: Keyword.get(opts, :connect_timeout, 10_000)
        ] ++ if(cred, do: [cred: cred], else: [])

      case GRPC.Stub.connect(address, connect_opts) do
        {:ok, channel} -> {:ok, channel}
        {:error, reason} -> {:error, {:connect_error, to_message(reason)}}
      end
    rescue
      error -> {:error, {:connect_error, Exception.message(error)}}
    catch
      :exit, reason -> {:error, {:connect_error, to_message(reason)}}
    end

    defp to_message(reason) when is_binary(reason), do: reason
    defp to_message(reason), do: inspect(reason)

    ## Metadata

    # `:api_key` becomes the bearer token, as the NIF's client does; `:headers`
    # ride on every call, after the client name and version the NIF also sends.
    # gRPC metadata keys are lowercase.
    @client_version Mix.Project.config()[:version]

    defp metadata(opts) do
      headers =
        opts
        |> Keyword.get(:headers, %{})
        |> Kernel.||(%{})
        |> Map.new(fn {key, value} -> {String.downcase(to_string(key)), to_string(value)} end)

      headers =
        Map.merge(%{"client-name" => "temporalex", "client-version" => @client_version}, headers)

      case Keyword.get(opts, :api_key) do
        nil -> headers
        key -> Map.put(headers, "authorization", "Bearer " <> key)
      end
    end
  end
end
