defmodule SpandexOTLP.Adapter do
  @behaviour Spandex.Adapter

  require Logger

  alias Spandex.{
    SpanContext,
    Tracer
  }

  @impl true
  def span_id, do: random_binary(64)

  @impl true
  def trace_id, do: random_binary(128)

  defp random_binary(bits) do
    1..div(bits, 8)
    |> Enum.map(fn _ -> :rand.uniform(255) end)
    |> :binary.list_to_bin()
  end

  @impl Spandex.Adapter
  def now(), do: :os.system_time(:nano_seconds)

  @impl true
  def default_sender, do: SpandexOTLP.Sender

  @doc """
  """
  @impl Spandex.Adapter
  @spec distributed_context(conn :: Plug.Conn.t(), Tracer.opts()) ::
          {:ok, SpanContext.t()}
          | {:error, :no_distributed_trace}
  def distributed_context(%Plug.Conn{} = conn, _opts) do
    trace_id = get_first_header(conn, "x-b3-traceid")
    parent_id = get_first_header(conn, "x-b3-parentspanid")
    priority = get_first_header(conn, "x-b3-sampled")

    if is_nil(trace_id) || is_nil(parent_id) do
      {:error, :no_distributed_trace}
    else
      {:ok, %SpanContext{trace_id: trace_id, parent_id: parent_id, priority: priority}}
    end
  end

  @impl Spandex.Adapter
  @spec distributed_context(headers :: Spandex.headers(), Tracer.opts()) ::
          {:ok, SpanContext.t()}
          | {:error, :no_distributed_trace}
  def distributed_context(headers, _opts) do
    trace_id = get_header(headers, "x-b3-traceid")
    parent_id = get_header(headers, "x-b3-parentspanid")
    priority = get_header(headers, "x-b3-sampled")

    if is_nil(trace_id) || is_nil(parent_id) do
      {:error, :no_distributed_trace}
    else
      {:ok, %SpanContext{trace_id: trace_id, parent_id: parent_id, priority: priority}}
    end
  end

  @doc """
  Injects XXXX-specific HTTP headers to represent the specified SpanContext
  """
  @impl Spandex.Adapter
  @spec inject_context([{term(), term()}], SpanContext.t(), Tracer.opts()) :: [{term(), term()}]
  def inject_context(headers, %SpanContext{} = span_context, _opts) when is_list(headers) do
    span_context
    |> tracing_headers()
    |> Kernel.++(headers)
  end

  def inject_context(headers, %SpanContext{} = span_context, _opts) when is_map(headers) do
    span_context
    |> tracing_headers()
    |> Enum.into(%{})
    |> Map.merge(headers)
  end

  # Private Helpers

  @spec get_first_header(Plug.Conn.t(), String.t()) :: String.t() | nil
  defp get_first_header(conn, header_name) do
    conn
    |> Plug.Conn.get_req_header(header_name)
    |> List.first()
  end

  @spec get_header(%{}, String.t()) :: integer() | nil
  defp get_header(headers, key) when is_map(headers) do
    Map.get(headers, key, nil)
  end

  @spec get_header([], String.t()) :: String.t() | nil
  defp get_header(headers, key) when is_list(headers) do
    Enum.find_value(headers, fn {k, v} -> if k == key, do: v end)
  end

  defp tracing_headers(%SpanContext{trace_id: trace_id, parent_id: parent_id}) do
    [
      {"x-b3-trace-id", to_string(trace_id)},
      {"x-b3-parentspanid", to_string(parent_id)}
    ]
  end
end
