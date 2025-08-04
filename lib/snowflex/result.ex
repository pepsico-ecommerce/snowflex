defmodule Snowflex.Result do
  @moduledoc """
  Represents the result of a Snowflake query execution.

  This struct contains all relevant information returned from a Snowflake query:

  * `:columns` - List of column names returned in the result set
  * `:rows` - List of tuples containing the row data
  * `:num_rows` - Number of rows in the result set
  * `:metadata` - Additional metadata about the query execution
  * `:messages` - Any messages returned by Snowflake during query execution
  * `:query` - The SQL statement that was executed
  * `:query_id` - The ID of the query that was executed
  * `:request_id` - The ID of the request that was executed
  * `:sql_state` - The SQL state of the query that was executed
  """
  defstruct columns: nil,
            rows: nil,
            num_rows: 0,
            metadata: [],
            messages: [],
            query: nil,
            query_id: nil,
            request_id: nil,
            sql_state: nil

  @type t :: %__MODULE__{
          columns: [String.t()] | nil,
          rows: [tuple()] | nil,
          num_rows: integer(),
          metadata: [map()],
          messages: [map()],
          query: Snowflex.Query.t() | nil,
          query_id: String.t() | nil,
          request_id: String.t() | nil,
          sql_state: String.t() | nil
        }
end
