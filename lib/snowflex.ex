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

  alias Ecto.Adapter
  alias Ecto.Adapters.SQL
  alias Ecto.UUID
  alias Snowflex.Ecto.Adapter.Stream, as: AdapterStream
  alias Snowflex.Query
  alias Snowflex.Result
  alias Snowflex.VariantField
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
  def loaders(:map, type), do: [&json_decode/1, type]
  def loaders({:map, _}, type), do: [&json_decode/1, type]
  def loaders({:array, _}, type), do: [&json_decode/1, type]
  def loaders(_, type), do: [type]

  @impl Ecto.Adapter
  def dumpers(:binary, type), do: [type, &binary_encode/1]
  def dumpers(:map, _type), do: [&json_encode/1]
  def dumpers({:map, _}, _type), do: [&json_encode/1]
  def dumpers({:array, _}, _type), do: [&json_encode/1]
  def dumpers(_, type), do: [type]

  defp binary_encode(raw), do: {:ok, Base.encode16(raw)}

  defp json_encode(nil), do: {:ok, nil}
  defp json_encode(value), do: Jason.encode(value)

  defp json_decode(nil), do: {:ok, nil}
  defp json_decode(value) when is_binary(value), do: Jason.decode(value)
  defp json_decode(value), do: {:ok, value}

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
  defp float_decode(%Decimal{} = decimal), do: {:ok, Decimal.to_float(decimal)}

  defp float_decode(float) do
    {val, _} = Float.parse(float)
    {:ok, val}
  end

  defp date_decode(nil), do: {:ok, nil}
  defp date_decode(%Date{} = date), do: {:ok, date}
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
    opts = Keyword.put_new(opts, :json_fields, json_fields_from_schema_meta(schema_meta))

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
    json_fields = json_fields_from_schema_meta(schema_meta)

    sql =
      @conn.insert(prefix, source, fields, [fields], on_conflict, returning, [],
        json_fields: json_fields
      )

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

  defp json_fields_from_schema_meta(%{schema: nil}), do: []

  defp json_fields_from_schema_meta(%{schema: schema}) do
    Enum.filter(schema.__schema__(:fields), fn field ->
      schema.__schema__(:type, field) |> VariantField.variant_field?()
    end)
  end

  @impl Ecto.Adapter.Schema
  def update(adapter_meta, schema_meta, fields, params, returning, opts) do
    %{source: source, prefix: prefix} = schema_meta
    {field_names, field_values} = :lists.unzip(fields)
    filter_values = Keyword.values(params)
    json_fields = json_fields_from_schema_meta(schema_meta)
    sql = @conn.update(prefix, source, field_names, params, returning, json_fields: json_fields)

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

  ## Raw SQL streaming

  @doc """
  Lazily streams the result of a raw SQL statement, one Snowflake partition at
  a time, and passes that stream to `fun`.

  This is the raw-SQL counterpart of `Ecto.Repo.stream/2` (which this adapter
  also serves lazily via the same cursor machinery): it declares a cursor and
  fetches partitions on demand, so only one partition of the result set is
  held in memory at a time.

  Each element of the stream is a `Snowflex.Result` holding exactly one
  partition of rows.

  When called inside `Ecto.Repo.checkout/2`, the stream runs on the
  already-held connection; otherwise a connection is checked out for the
  duration of `fun`. Either way the stream is only valid inside `fun`, so
  consume it before returning.

  ## Options

  Options are merged over the repo's configured connection options and passed
  to `DBConnection.run/3` and every cursor operation.

    * `:timeout` - bounds statement execution (the cursor declare), each
      partition fetch, and the total time the connection may be held while
      `fun` consumes the stream. For long statements or large result sets pass
      a generous value or `:infinity`.

  ## Examples

      Snowflex.stream_query(MyRepo, "SELECT * FROM big_table", [], [timeout: :timer.minutes(30)], fn stream ->
        stream
        |> Stream.flat_map(fn %Snowflex.Result{rows: rows} -> rows || [] end)
        |> Enum.each(&process_row/1)
      end)

  """
  @spec stream_query(
          repo :: Ecto.Repo.t() | pid(),
          statement :: String.t(),
          params :: list(),
          opts :: Keyword.t(),
          fun :: (Enumerable.t() -> result)
        ) :: result
        when result: var
  def stream_query(repo, statement, params \\ [], opts \\ [], fun)
      when (is_atom(repo) or is_pid(repo)) and is_function(fun, 1) do
    %{pid: pool, opts: default_opts} = Adapter.lookup_meta(repo)
    opts = opts ++ default_opts
    query = Query.new(statement: statement)

    run_with_conn(pool, opts, fn conn ->
      conn
      |> DBConnection.prepare_stream(query, params, opts)
      |> fun.()
    end)
  end

  # Runs `fun` on the connection held by an enclosing `Ecto.Repo.checkout/2`
  # when there is one, so stream_query composes with checkout instead of
  # checking out a second connection (a deadlock at pool_size: 1); otherwise
  # the connection is scoped to this call, which is safe for stream_query
  # because consumption happens inside `fun`.
  #
  # Repo.checkout delegates to Ecto.Adapters.SQL.checkout/3, which stashes the
  # connection in the process dictionary under `{Ecto.Adapters.SQL, pool}`;
  # ecto_sql exposes no public getter for it, so we read the (long-stable) key
  # directly — the checkout tests pin this integration.
  defp run_with_conn(pool, opts, fun) do
    case Process.get({SQL, pool}) do
      nil -> DBConnection.run(pool, fun, opts)
      %DBConnection{} = conn -> fun.(conn)
    end
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
    %{pid: pool, telemetry: telemetry, opts: default_opts} = adapter_meta

    # The enumeration is driven by the caller (Ecto composes streams with
    # suspend/resume), so the connection must stay checked out for the
    # consumer-controlled lifetime of the stream. Only an enclosing
    # Repo.checkout/2 can provide that scope — a run/3 opened here would check
    # the connection back in on the first suspension.
    case Process.get({SQL, pool}) do
      nil ->
        raise """
        cannot reduce stream outside of Ecto.Repo.checkout/2.

        Snowflake has no transactions, so the connection scope for a stream \
        is established with checkout/2 instead:

            MyRepo.checkout(fn ->
              query |> MyRepo.stream() |> Enum.each(...)
            end, timeout: :timer.minutes(30))
        """

      %DBConnection{} = conn ->
        opts = with_log(telemetry, params, opts ++ default_opts)
        query = Query.new(statement: statement)

        conn
        |> DBConnection.prepare_stream(query, params, opts)
        |> Stream.flat_map(fn %Result{rows: rows} -> rows || [] end)
        |> Enumerable.reduce(acc, fun)
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

  # Cursor operations (declare/fetch) report {:ok, query, cursor} and
  # {:cont | :halt, result} shapes; normalize them all to {:ok, res}.
  defp normalize_log_result({:ok, _query, res}), do: {:ok, res}
  defp normalize_log_result({status, res}) when status in [:cont, :halt], do: {:ok, res}
  defp normalize_log_result(other), do: other

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
    result = normalize_log_result(result)
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
