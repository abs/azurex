defmodule Azurex.Blob do
  @moduledoc """
  Implementation of Azure Blob Storage.

  In the functions below set container as nil to use the one configured in `Azurex.Blob.Config`.
  """

  alias Azurex.Authorization.Auth
  alias Azurex.Blob.{Block, Config}

  @typep optional_string :: String.t() | nil

  @spec list_containers(Config.config_overrides()) ::
          {:ok, String.t()}
          | {:error, term()}
  def list_containers(overrides \\ []) do
    connection_params = Config.get_connection_params(overrides)

    Req.new(
      url: Config.api_url(connection_params) <> "/",
      params: [comp: "list"]
    )
    |> Auth.authorize_request(connection_params)
    |> Req.request()
    |> case do
      {:ok, %{status: 200, body: xml}} -> {:ok, xml}
      {:ok, response} -> {:error, response}
      {:error, exception} -> {:error, exception}
    end
  end

  @doc """
  Upload a blob.

  ## The `blob` Argument

  The blob argument may be either a `binary` or a tuple of
  `{:stream, Stream.t()}`.

  ## The `content_type` Argument

  This argument can be either a valid string, or `nil`. A `content_type`
  argument of `nil` will result in the blob being assigned the default content
  type `"application/octet-stream"`.

  ## Examples

      iex> put_blob("filename.txt", "file contents", "text/plain")
      :ok

      iex> {:ok, io_device} = StringIO.open("file contents as a stream")
      byte_length = 8_000_000
      bitstream = IO.binstream(io_device, byte_length)
      put_blob("filename.txt", {:stream, bitstream}, nil)
      :ok

      iex> put_blob("filename.txt", "file contents", "text/plain", "container")
      :ok

      iex> put_blob("filename.txt", "file contents", "text/plain", [container: "container"])
      :ok

      iex> put_blob("filename.txt", "file contents", "text/plain", [storage_account_name: "name", storage_account_key: "key"])
      :ok

      iex> put_blob("filename.txt", "file contents", "text/plain", [storage_account_connection_string: "AccountName=name;AccountKey=key", container: "container"])
      :ok

      iex> put_blob("filename.txt", "file contents", "text/plain", nil, timeout: 10)
      :ok

      iex> put_blob("filename.txt", "file contents", "text/plain")
      {:error, %Req.Response{}}

  """
  @spec put_blob(
          String.t(),
          binary() | {:stream, Enumerable.t()},
          optional_string,
          Config.config_overrides(),
          keyword
        ) ::
          :ok
          | {:error, term()}
  def put_blob(name, blob, content_type, overrides \\ [], params \\ [])

  def put_blob(name, {:stream, bitstream}, content_type, overrides, params) do
    content_type = content_type || "application/octet-stream"

    bitstream
    |> Stream.transform(
      fn -> {:ok, []} end,
      fn chunk, {:ok, acc} ->
        case Block.put_block(overrides, chunk, name, params) do
          {:ok, block_id} -> {[], {:ok, [block_id | acc]}}
          {:error, error} -> {[], {:error, error}}
        end
      end,
      fn
        {:ok, block_ids} -> Block.put_block_list(block_ids, overrides, name, content_type, params)
        {:error, _error} -> :ok
      end
    )
    |> Stream.run()
  end

  def put_blob(name, blob, content_type, overrides, params) do
    content_type = content_type || "application/octet-stream"
    connection_params = Config.get_connection_params(overrides)

    Req.new(
      method: :put,
      url: get_url(name, connection_params),
      params: params,
      body: blob,
      headers: [
        {"x-ms-blob-type", "BlockBlob"}
      ],
      # Blob storage only answers when the whole file has been uploaded
      receive_timeout: :infinity
    )
    |> Auth.authorize_request(connection_params, content_type)
    |> Req.request()
    |> case do
      {:ok, %{status: 201}} -> :ok
      {:ok, response} -> {:error, response}
      {:error, exception} -> {:error, exception}
    end
  end

  @doc """
  Download a blob

  ## Examples

      iex> get_blob("filename.txt")
      {:ok, "file contents"}

      iex> get_blob("filename.txt", "container")
      {:ok, "file contents"}

      iex> get_blob("filename.txt", [storage_account_name: "name", storage_account_key: "key", container: "container"])
      {:ok, "file contents"}

      iex> get_blob("filename.txt", [storage_account_connection_string: "AccountName=name;AccountKey=key"])
      {:ok, "file contents"}

      iex> get_blob("filename.txt", nil, timeout: 10)
      {:ok, "file contents"}

      iex> get_blob("filename.txt")
      {:error, %Req.Response{}}

  """
  @spec get_blob(String.t(), Config.config_overrides(), keyword) ::
          {:ok, binary()}
          | {:error, term()}
  def get_blob(name, overrides \\ [], params \\ []) do
    blob_request(name, overrides, :get, params)
    |> Req.request()
    |> case do
      {:ok, %{status: 200, body: blob}} -> {:ok, blob}
      {:ok, response} -> {:error, response}
      {:error, exception} -> {:error, exception}
    end
  end

  @doc """
  Checks if a blob exists, and returns metadata for the blob if it does
  """
  @spec head_blob(String.t(), Config.config_overrides(), keyword) ::
          {:ok, list}
          | {:error, :not_found | term()}
  def head_blob(name, overrides \\ [], params \\ []) do
    blob_request(name, overrides, :head, params)
    |> Req.request()
    |> case do
      {:ok, %{status: 200, headers: headers}} -> {:ok, headers}
      {:ok, %{status: 404}} -> {:error, :not_found}
      {:ok, response} -> {:error, response}
      {:error, exception} -> {:error, exception}
    end
  end

  @doc """
  Copies a blob to a destination.

  The same configuration options (connection string, container, ...) are applied to both source and destination.

  Note: Azure's '[Copy Blob from URL](https://learn.microsoft.com/en-us/rest/api/storageservices/copy-blob-from-url)'
  operation has a maximum size of 256 MiB.
  """
  @spec copy_blob(String.t(), String.t(), Config.config_overrides()) ::
          {:ok, term()} | {:error, term()}
  def copy_blob(source_name, destination_name, overrides \\ []) do
    content_type = "application/octet-stream"
    connection_params = Config.get_connection_params(overrides)
    source_url = get_url(source_name, connection_params)

    Req.new(
      method: :put,
      url: get_url(destination_name, connection_params),
      headers: [
        {"x-ms-copy-source", source_url},
        {"content-type", content_type}
      ]
    )
    |> Auth.authorize_request(connection_params, content_type)
    |> Req.request()
    |> case do
      {:ok, %{status: 202} = resp} -> {:ok, resp}
      {:ok, response} -> {:error, response}
      {:error, exception} -> {:error, exception}
    end
  end

  @spec delete_blob(String.t(), Config.config_overrides(), keyword) ::
          :ok | {:error, :not_found | term()}
  def delete_blob(name, overrides \\ [], params \\ []) do
    blob_request(name, overrides, :delete, params)
    |> Req.request()
    |> case do
      {:ok, %{status: 202}} -> :ok
      {:ok, %{status: 404}} -> {:error, :not_found}
      {:ok, response} -> {:error, response}
      {:error, exception} -> {:error, exception}
    end
  end

  defp blob_request(name, overrides, method, params) do
    connection_params = Config.get_connection_params(overrides)

    Req.new(
      method: method,
      url: get_url(name, connection_params),
      params: params,
      decode_body: false
    )
    |> Auth.authorize_request(connection_params)
  end

  @doc """
  Lists all blobs in a container

  ## Examples

      iex> Azurex.Blob.list_blobs()
      {:ok, "\uFEFF<?xml ...."}

      iex> Azurex.Blob.list_blobs(storage_account_name: "name", storage_account_key: "key", container: "container")
      {:ok, "\uFEFF<?xml ...."}

      iex> Azurex.Blob.list_blobs()
      {:error, %Req.Response{}}
  """
  @spec list_blobs(Config.config_overrides(), keyword()) ::
          {:ok, binary()}
          | {:error, term()}
  def list_blobs(overrides \\ [], params \\ []) do
    connection_params = Config.get_connection_params(overrides)

    Req.new(
      url: get_url(connection_params),
      params:
        [
          comp: "list",
          restype: "container"
        ] ++ params
    )
    |> Auth.authorize_request(connection_params)
    |> Req.request()
    |> case do
      {:ok, %{status: 200, body: xml}} -> {:ok, xml}
      {:ok, response} -> {:error, response}
      {:error, exception} -> {:error, exception}
    end
  end

  @doc """
  Returns the url for a container (defaults to the one in `Azurex.Blob.Config`)
  """
  @spec get_url(keyword) :: String.t()
  def get_url(connection_params) do
    "#{Config.api_url(connection_params)}/#{get_container(connection_params)}"
  end

  @doc """
  Returns the url for a file in a container (defaults to the one in `Azurex.Blob.Config`)
  """
  @spec get_url(String.t(), keyword) :: String.t()
  def get_url(blob_name, connection_params) do
    "#{get_url(connection_params)}/#{blob_name}"
  end

  defp get_container(connection_params) do
    Keyword.get(connection_params, :container) || Config.default_container()
  end
end
