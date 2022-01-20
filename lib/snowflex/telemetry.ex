defmodule Snowflex.Telemetry do
  @moduledoc """
  Shared telemetry module for registering events
  """

  @sql_start [:snowflex, :sql_query, :start]
  @sql_stop [:snowflex, :sql_query, :stop]
  @param_start [:snowflex, :param_query, :start]
  @param_stop [:snowflex, :param_query, :stop]

  @default_metadata %{
    transport: Application.compile_env(:snowflex, :transport, :odbc)
  }

  @spec sql_start(map(), map()) :: integer()
  def sql_start(metadata \\ %{}, measurements \\ %{}) do
    start_time = System.monotonic_time()

    measurements = Map.merge(measurements, %{system_time: System.system_time()})

    emit(@sql_start, measurements, metadata)

    start_time
  end

  @spec sql_stop(integer(), map(), map()) :: :ok
  def sql_stop(start_time, metadata \\ %{}, measurements \\ %{}) do
    end_time = System.monotonic_time()

    measurements = Map.merge(measurements, %{duration: end_time - start_time})

    emit(@sql_stop, measurements, metadata)
  end

  @spec param_start(map(), map()) :: integer()
  def param_start(metadata \\ %{}, measurements \\ %{}) do
    start_time = System.monotonic_time()

    measurements = Map.merge(measurements, %{system_time: System.system_time()})

    emit(@param_start, measurements, metadata)

    start_time
  end

  @spec param_stop(integer(), map(), map()) :: :ok
  def param_stop(start_time, metadata \\ %{}, measurements \\ %{}) do
    end_time = System.monotonic_time()

    measurements = Map.merge(measurements, %{duration: end_time - start_time})

    emit(@param_stop, measurements, metadata)
  end

  defp emit(event, measurements, metadata) do
    metadata = Map.merge(metadata, @default_metadata)
    :telemetry.execute(event, measurements, metadata)
  end
end
