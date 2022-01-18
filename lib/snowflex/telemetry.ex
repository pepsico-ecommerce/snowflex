defmodule Snowflex.Telemetry do
  @moduledoc """
  Shared telemetry module for registering events
  """

  @sql_start [:snowflex, :sql_query, :start]
  @sql_stop [:snowflex, :sql_query, :stop]
  @param_start [:snowflex, :param_query, :start]
  @param_stop [:snowflex, :param_query, :stop]

  @spec sql_start(map(), map()) :: integer()
  def sql_start(metadata \\ %{}, measurements \\ %{}) do
    start_time = System.monotonic_time()

    measurements = Map.merge(measurements, %{system_time: System.system_time()})

    :telemetry.execute(@sql_start, measurements, metadata)

    start_time
  end

  @spec sql_stop(integer(), map(), map()) :: :ok
  def sql_stop(start_time, metadata \\ %{}, measurements \\ %{}) do
    end_time = System.monotonic_time()

    measurements = Map.merge(measurements, %{duration: end_time - start_time})

    :telemetry.execute(@sql_stop, measurements, metadata)
  end

  @spec param_start(map(), map()) :: integer()
  def param_start(metadata \\ %{}, measurements \\ %{}) do
    start_time = System.monotonic_time()

    measurements = Map.merge(measurements, %{system_time: System.system_time()})

    :telemetry.execute(@param_start, measurements, metadata)

    start_time
  end

  @spec param_stop(integer(), map(), map()) :: :ok
  def param_stop(start_time, metadata \\ %{}, measurements \\ %{}) do
    end_time = System.monotonic_time()

    measurements = Map.merge(measurements, %{duration: end_time - start_time})

    :telemetry.execute(@param_stop, measurements, metadata)
  end
end
