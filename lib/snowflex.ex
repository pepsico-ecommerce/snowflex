defmodule Snowflex do
  @doc_header """
  Snowflex is an Ecto adapter for [Snowflake](https://www.snowflake.com/) using Snowflake's [SQL API](https://docs.snowflake.com/en/developer-guide/sql-api/reference).
  """
  @readme Path.join([__DIR__, "../README.md"])

  @doc_footer @readme
              |> File.read!()
              |> String.split("<!-- MDOC -->")
              |> Enum.fetch!(1)

  @moduledoc @doc_header <> @doc_footer

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Queryable
  @behaviour Ecto.Adapter.Schema

  alias Ecto.Adapters.SQL
  alias Ecto.UUID
  alias Snowflex.Ecto.Adapter.Stream, as: AdapterStream
  alias Snowflex.Query
  alias String.Chars

  require Logger

  @conn __MODULE__.Ecto.Adapter.Connection

  @impl Ecto.Adapter
  defmacro __before_compile__(env) do
    SQL.__before_compile__(:snowflex, env)
  end

  @impl Ecto.Adapter
  def ensure_all_started(config, type) do
    SQL.ensure_all_started(:snowflex, config, type)
  end

  @impl Ecto.Adapter
  def init(config) do
    SQL.init(@conn, :snowflex, config)
  end

  @impl Ecto.Adapter
  def checkout(meta, opts, fun) do
    SQL.checkout(meta, opts, fun)
  end

  @impl Ecto.Adapter
  def checked_out?(meta) do
    SQL.checked_out?(meta)
  end

  @impl Ecto.Adapter
  def loaders(:integer, type), do: [&int_decode/1, type]
  def loaders(:decimal, type), do: [&decimal_decode/1, type]
  def loaders(:float, type), do: [&float_decode/1, type]
  def loaders(:date, type), do: [&date_decode/1, type]
  def loaders(:id, type), do: [&int_decode/1, type]
  def loaders(:time, type), do: [&time_decode/1, type]
  def loaders(:time_usec, type), do: [&time_decode/1, type]
  def loaders(_, type), do: [type]

  @impl Ecto.Adapter
  def dumpers(:binary, type), do: [type, &binary_encode/1]
  def dumpers(_, type), do: [type]

  defp binary_encode(raw), do: {:ok, Base.encode16(raw)}

  defp decimal_decode(nil), do: {:ok, nil}
  defp decimal_decode(dec) when is_binary(dec), do: {:ok, Decimal.new(dec)}
  defp decimal_decode(dec) when is_float(dec), do: {:ok, Decimal.from_float(dec)}

  defp int_decode(nil), do: {:ok, nil}
  defp int_decode(int) when is_binary(int), do: {:ok, String.to_integer(int)}
  defp int_decode(int), do: {:ok, int}

  defp time_decode(nil), do: {:ok, nil}
  defp time_decode(time), do: Time.from_iso8601(time)

  defp float_decode(nil), do: {:ok, nil}
  defp float_decode(float) when is_float(float), do: float

  defp float_decode(float) do
    {val, _} = Float.parse(float)
    {:ok, val}
  end

  defp date_decode(nil), do: {:ok, nil}
  defp date_decode(date), do: Date.from_iso8601(date)

  ## Query

  @impl Ecto.Adapter.Queryable
  def prepare(:all, query) do
    {:cache, {System.unique_integer([:positive]), IO.iodata_to_binary(@conn.all(query))}}
  end

  def prepare(:update_all, query) do
    {:cache, {System.unique_integer([:positive]), IO.iodata_to_binary(@conn.update_all(query))}}
  end

  def prepare(:delete_all, query) do
    {:cache, {System.unique_integer([:positive]), IO.iodata_to_binary(@conn.delete_all(query))}}
  end

  @impl Ecto.Adapter.Queryable
  def execute(adapter_meta, query_meta, query, params, opts) do
    SQL.execute(:named, adapter_meta, query_meta, query, params, opts)
  end

  @impl Ecto.Adapter.Queryable
  def stream(adapter_meta, query_meta, prepared, params, opts) do
    do_stream(adapter_meta, prepared, params, put_source(opts, query_meta))
  end

  defp do_stream(adapter_meta, {:cache, _, {_, prepared}}, params, opts) do
    prepare_stream(adapter_meta, prepared, params, opts)
  end

  defp do_stream(adapter_meta, {:cached, _, _, {_, cached}}, params, opts) do
    prepare_stream(adapter_meta, Chars.to_string(cached), params, opts)
  end

  defp do_stream(adapter_meta, {:nocache, {_id, prepared}}, params, opts) do
    prepare_stream(adapter_meta, prepared, params, opts)
  end

  defp prepare_stream(adapter_meta, prepared, params, opts) do
    adapter_meta
    |> AdapterStream.build(prepared, params, opts)
    |> Stream.map(fn row -> {1, [row]} end)
  end

  ## Schema

  @impl Ecto.Adapter.Schema
  def autogenerate(:id), do: nil
  def autogenerate(:embed_id), do: UUID.generate()
  def autogenerate(:binary_id), do: UUID.bingenerate()

  @impl Ecto.Adapter.Schema
  def insert_all(
        adapter_meta,
        schema_meta,
        header,
        rows,
        on_conflict,
        returning,
        placeholders,
        opts
      ) do
    SQL.insert_all(
      adapter_meta,
      schema_meta,
      @conn,
      header,
      rows,
      on_conflict,
      returning,
      placeholders,
      opts
    )
  end

  @impl Ecto.Adapter.Schema
  def insert(adapter_meta, schema_meta, params, on_conflict, returning, opts) do
    %{source: source, prefix: prefix} = schema_meta
    {kind, conflict_params, _} = on_conflict
    {fields, values} = :lists.unzip(params)
    sql = @conn.insert(prefix, source, fields, [fields], on_conflict, returning, [])

    SQL.struct(
      adapter_meta,
      @conn,
      sql,
      :insert,
      source,
      [],
      values ++ conflict_params,
      kind,
      returning,
      opts
    )
  end

  @impl Ecto.Adapter.Schema
  def update(adapter_meta, schema_meta, fields, params, returning, opts) do
    %{source: source, prefix: prefix} = schema_meta
    {fields, field_values} = :lists.unzip(fields)
    filter_values = Keyword.values(params)
    sql = @conn.update(prefix, source, fields, params, returning)

    SQL.struct(
      adapter_meta,
      @conn,
      sql,
      :update,
      source,
      params,
      field_values ++ filter_values,
      :raise,
      returning,
      opts
    )
  end

  @impl Ecto.Adapter.Schema
  def delete(adapter_meta, schema_meta, params, returning, opts) do
    %{source: source, prefix: prefix} = schema_meta
    filter_values = Keyword.values(params)
    sql = @conn.delete(prefix, source, params, returning)

    SQL.struct(
      adapter_meta,
      @conn,
      sql,
      :delete,
      source,
      params,
      filter_values,
      :raise,
      returning,
      opts
    )
  end

  @doc false
  @spec reduce(
          adapter_meta :: map(),
          statement :: String.t(),
          params :: list(),
          opts :: Keyword.t(),
          acc :: Enumerable.acc(),
          fun :: Enumerable.reducer()
        ) :: Enumerable.result()
  def reduce(adapter_meta, statement, params, opts, acc, fun) do
    %{pid: pid, telemetry: telemetry, opts: default_opts} = adapter_meta
    opts = with_log(telemetry, params, opts ++ default_opts)

    query = Query.new(statement: statement)

    case DBConnection.execute(pid, query, params, opts) do
      {:ok, _query, %{rows: rows}} ->
        Enumerable.reduce(rows, acc, fun)

      {:error, err} ->
        raise err
    end
  end

  @doc false
  @spec into(
          adapter_meta :: map(),
          statement :: String.t(),
          params :: list(),
          opts :: Keyword.t()
        ) :: {list(), (list(), :done | {:cont, any()} -> {list(), list()})}
  def into(adapter_meta, statement, params, opts) do
    %{pid: pid, telemetry: telemetry, opts: default_opts} = adapter_meta
    opts = with_log(telemetry, params, opts ++ default_opts)

    query = Query.new(statement: statement)

    case DBConnection.execute(pid, query, params, opts) do
      {:ok, _query, %{rows: rows}} ->
        {[],
         fn
           [], {:cont, row} -> {[row | rows], []}
           acc, {:cont, row} -> {[row | acc], []}
           acc, :done -> {acc, rows}
           _, _ -> raise "not implemented"
         end}

      {:error, err} ->
        raise err
    end
  end

  defp with_log(telemetry, params, opts) do
    [log: &log(telemetry, params, &1, opts)] ++ opts
  end

  defp log({repo, log, event_name}, params, entry, opts) do
    %{
      connection_time: query_time,
      decode_time: decode_time,
      pool_time: queue_time,
      idle_time: idle_time,
      result: result,
      query: query
    } = entry

    source = Keyword.get(opts, :source)
    query = Chars.to_string(query)
    result = with {:ok, _query, res} <- result, do: {:ok, res}
    stacktrace = Keyword.get(opts, :stacktrace)
    log_params = opts[:cast_params] || params

    acc = if idle_time, do: [idle_time: idle_time], else: []

    measurements =
      log_measurements(
        [query_time: query_time, decode_time: decode_time, queue_time: queue_time],
        0,
        acc
      )

    metadata = %{
      type: :ecto_sql_query,
      repo: repo,
      result: result,
      params: params,
      cast_params: opts[:cast_params],
      query: query,
      source: source,
      stacktrace: stacktrace,
      options: Keyword.get(opts, :telemetry_options, [])
    }

    if event_name = Keyword.get(opts, :telemetry_event, event_name) do
      :telemetry.execute(event_name, measurements, metadata)
    end

    case {opts[:log], log} do
      {false, _level} ->
        :ok

      {opts_level, false} when opts_level in [nil, true] ->
        :ok

      {true, level} ->
        Logger.log(
          level,
          fn -> log_iodata(measurements, repo, source, query, log_params, result, stacktrace) end,
          ansi_color: sql_color(query)
        )

      {opts_level, args_level} ->
        Logger.log(
          opts_level || args_level,
          fn -> log_iodata(measurements, repo, source, query, log_params, result, stacktrace) end,
          ansi_color: sql_color(query)
        )
    end

    :ok
  end

  defp log_measurements([{_, nil} | rest], total, acc),
    do: log_measurements(rest, total, acc)

  defp log_measurements([{key, value} | rest], total, acc),
    do: log_measurements(rest, total + value, [{key, value} | acc])

  defp log_measurements([], total, acc),
    do: Map.new([total_time: total] ++ acc)

  defp log_iodata(measurements, repo, source, query, params, result, stacktrace) do
    [
      "QUERY",
      ?\s,
      log_ok_error(result),
      log_ok_source(source),
      log_time("db", measurements, :query_time, true),
      log_time("decode", measurements, :decode_time, false),
      log_time("queue", measurements, :queue_time, false),
      log_time("idle", measurements, :idle_time, true),
      ?\n,
      query,
      ?\s,
      inspect(params, charlists: false),
      log_stacktrace(stacktrace, repo)
    ]
  end

  defp log_ok_error({:ok, _res}), do: "OK"
  defp log_ok_error({:error, _err}), do: "ERROR"

  defp log_ok_source(nil), do: ""
  defp log_ok_source(source), do: " source=#{inspect(source)}"

  defp log_time(label, measurements, key, force) do
    case measurements do
      %{^key => time} ->
        us = System.convert_time_unit(time, :native, :microsecond)
        ms = div(us, 100) / 10

        if force or ms > 0 do
          [?\s, label, ?=, :io_lib_format.fwrite_g(ms), ?m, ?s]
        else
          []
        end

      %{} ->
        []
    end
  end

  defp log_stacktrace(stacktrace, repo) do
    with [_ | _] <- stacktrace,
         {module, function, arity, info} <- last_non_ecto(Enum.reverse(stacktrace), repo, nil) do
      [
        ?\n,
        IO.ANSI.light_black(),
        "↳ ",
        Exception.format_mfa(module, function, arity),
        log_stacktrace_info(info),
        IO.ANSI.reset()
      ]
    else
      _ -> []
    end
  end

  defp log_stacktrace_info([file: file, line: line] ++ _) do
    [", at: ", file, ?:, Integer.to_string(line)]
  end

  defp log_stacktrace_info(_) do
    []
  end

  @repo_modules [Ecto.Repo.Queryable, Ecto.Repo.Schema, Ecto.Repo.Transaction]

  defp last_non_ecto([{mod, _, _, _} | _stacktrace], repo, last)
       when mod == repo or mod in @repo_modules,
       do: last

  defp last_non_ecto([last | stacktrace], repo, _last),
    do: last_non_ecto(stacktrace, repo, last)

  defp last_non_ecto([], _repo, last),
    do: last

  defp sql_color("SELECT" <> _), do: :cyan
  defp sql_color("ROLLBACK" <> _), do: :red
  defp sql_color("LOCK" <> _), do: :white
  defp sql_color("INSERT" <> _), do: :green
  defp sql_color("UPDATE" <> _), do: :yellow
  defp sql_color("DELETE" <> _), do: :red
  defp sql_color("begin" <> _), do: :magenta
  defp sql_color("commit" <> _), do: :magenta
  defp sql_color(_), do: nil

  defp put_source(opts, %{sources: sources}) when is_binary(elem(elem(sources, 0), 0)) do
    {source, _, _} = elem(sources, 0)
    [source: source] ++ opts
  end

  defp put_source(opts, _) do
    opts
  end
end
