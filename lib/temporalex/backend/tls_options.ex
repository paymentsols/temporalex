defmodule Temporalex.Backend.TlsOptions do
  @moduledoc false

  # The `:tls` client option, shared by every client backend so they accept
  # the same spellings and refuse bad ones with the same errors.
  #
  # `:tls` is `true` for TLS against the system roots, or a keyword list of PEM
  # material, each given inline or as a `_file` path, plus `:domain`, the
  # server name to verify. Files are read here so backends only ever see bytes.

  @pem_keys [:server_root_ca_cert, :client_cert, :client_private_key]

  @doc """
  Reads `opts[:tls]`.

  Returns `{:ok, nil}` for no TLS, `{:ok, []}` for TLS against the system
  roots, or `{:ok, keyword}` holding the PEM binaries that were given and
  `:domain`.
  """
  def read(opts) when is_list(opts) do
    case Keyword.get(opts, :tls) do
      nil ->
        {:ok, nil}

      false ->
        {:ok, nil}

      true ->
        {:ok, []}

      tls when is_list(tls) ->
        read_tls(tls)

      other ->
        {:error,
         {:invalid_options, ":tls must be true, false or a keyword list, got: #{inspect(other)}"}}
    end
  end

  defp read_tls(tls) do
    Enum.reduce_while(@pem_keys, {:ok, Keyword.take(tls, [:domain])}, fn key, {:ok, acc} ->
      case pem(tls, key) do
        {:ok, nil} -> {:cont, {:ok, acc}}
        {:ok, pem} -> {:cont, {:ok, Keyword.put(acc, key, pem)}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp pem(tls, key) do
    file_key = :"#{key}_file"

    case {Keyword.get(tls, key), Keyword.get(tls, file_key)} do
      {nil, nil} ->
        {:ok, nil}

      {pem, nil} when is_binary(pem) ->
        {:ok, pem}

      {nil, path} when is_binary(path) ->
        read_file(file_key, path)

      _both_or_invalid ->
        {:error, {:invalid_options, ":tls takes one of :#{key} or :#{file_key}, as a binary"}}
    end
  end

  defp read_file(file_key, path) do
    case File.read(path) do
      {:ok, pem} ->
        {:ok, pem}

      {:error, reason} ->
        {:error, {:invalid_options, ":tls :#{file_key} #{path}: #{:file.format_error(reason)}"}}
    end
  end
end
