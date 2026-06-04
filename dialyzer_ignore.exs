[
  # Pre-existing improper list warnings in the Ecto SQL adapter (iodata construction patterns)
  %{file: "lib/snowflex/ecto/adapter/connection.ex", code: :warn_non_proper_list},
  # Pattern match on order_by direction that Dialyzer thinks is unreachable
  %{file: "lib/snowflex/ecto/adapter/connection.ex", code: :warn_matching},
  # :public_key functions removed from export in newer OTP but still available
  %{file: "lib/snowflex/transport/http.ex", message: "Unknown function public_key:pem_decode/1"},
  %{file: "lib/snowflex/transport/http.ex", message: "Unknown function public_key:pem_entry_decode/2"}
]
