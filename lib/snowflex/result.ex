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
  """
  defstruct columns: nil,
            rows: nil,
            num_rows: 0,
            metadata: [],
            messages: [],
            query: nil

  @type t :: %__MODULE__{
          columns: [String.t()] | nil,
          rows: [tuple()] | nil,
          num_rows: integer(),
          metadata: [map()],
          messages: [map()],
          query: Snowflex.Query.t() | nil
        }
end
