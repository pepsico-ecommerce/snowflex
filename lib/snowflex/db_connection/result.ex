defmodule Snowflex.DBConnection.Result do
  defstruct columns: nil,
            rows: nil,
            num_rows: 0,
            metadata: [],
            messages: [],
            statement: nil,
            success: false

  @type t :: %__MODULE__{
          columns: [String.t()] | nil,
          rows: [tuple()] | nil,
          num_rows: integer(),
          metadata: [map()],
          messages: [map()],
          statement: String.t() | nil,
          success: boolean()
        }
end
