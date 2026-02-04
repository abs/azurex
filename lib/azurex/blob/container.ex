defmodule Azurex.Blob.Container do
  @moduledoc """
  Implementation of Azure Blob Storage container operations
  """

  alias Azurex.Authorization.SharedKey
  alias Azurex.Blob.Config

  @doc """
  Checks if a container exists and returns its metadata if it does
  """
  @spec head_container(String.t(), Config.config_overrides()) ::
          {:ok, list} | {:error, :not_found | term()}
  def head_container(container, overrides \\ []) do
    connection_params = Config.get_connection_params(overrides)

    Req.new(
      url: Config.api_url(connection_params) <> "/" <> container,
      params: [restype: "container"],
      method: :head
    )
    |> SharedKey.sign(
      storage_account_name: Config.storage_account_name(connection_params),
      storage_account_key: Config.storage_account_key(connection_params)
    )
    |> Req.request()
    |> case do
      {:ok, %{status: 200, headers: headers}} -> {:ok, headers}
      {:ok, %{status: 404}} -> {:error, :not_found}
      {:ok, response} -> {:error, response}
      {:error, exception} -> {:error, exception}
    end
  end

  @doc """
  Creates a new container
  """
  @spec create(String.t(), Config.config_overrides()) ::
          {:ok, String.t()} | {:error, :already_exists | term()}
  def create(container, overrides \\ []) do
    connection_params = Config.get_connection_params(overrides)

    Req.new(
      url: Config.api_url(connection_params) <> "/" <> container,
      params: [restype: "container"],
      method: :put
    )
    |> SharedKey.sign(
      storage_account_name: Config.storage_account_name(connection_params),
      storage_account_key: Config.storage_account_key(connection_params),
      content_type: "application/octet-stream"
    )
    |> Req.request()
    |> case do
      {:ok, %{status: 201}} -> {:ok, container}
      {:ok, %{status: 409}} -> {:error, :already_exists}
      {:ok, response} -> {:error, response}
      {:error, exception} -> {:error, exception}
    end
  end
end
