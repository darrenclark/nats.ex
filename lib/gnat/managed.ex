defmodule Gnat.Managed do
  @doc """

  ## States
  
  #### `:connected`

    - connection is good
    - pubs/subs/unsubs are sent immediately

  #### `:reconnecting`
  
    - not connected, but actively retrying to reconnect
    - pubs/subs/unsubs are queued up for when we reconnect

  """
  @behaviour :gen_statem

  require Logger

  def child_spec(init_arg) do
    default = %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [init_arg]}
    }

    Supervisor.child_spec(default, [])
  end

  def start_link(opts \\ []) do
    gen_opts = Keyword.take(opts, [:timeout, :debug, :spawn_opt])
    name = opts[:name]

    if name do
      name = if is_atom(name) do {:local, name} else name end
      :gen_statem.start_link(name, __MODULE__, opts, gen_opts)
    else
      :gen_statem.start_link(__MODULE__, opts, gen_opts)
    end
  end

  @impl :gen_statem
  def callback_mode(), do: :handle_event_function

  @impl :gen_statem
  def init(opts) do
    connection_settings = opts[:connection_settings] || %{}

    data = %{
      gnat: nil,
      subs: :ets.new(:subscriptions, [:private, keypos: 2]),
      connection_settings: Map.new(connection_settings),
      retries: 0
    }

    case Gnat.start_link(connection_settings) do
      {:ok, gnat} ->
        {:ok, :connected, %{data | gnat: gnat}}

      {:error, reason} ->
        Logger.error("Failed to connect: #{reason}, retrying...")
        {:ok, :reconnecting, %{data | retries: 1}}
        throw :uh_oh
    end
  catch
    reason ->
      {:stop, reason}
  end

  @impl :gen_statem
  def handle_event(event, event_data, state, data)

  def handle_event({:call, from}, :stop, :connected, data) do
    do_call(:stop, data)
    {:stop_and_reply, :normal, [{:reply, from, :ok}]}
  end

  def handle_event({:call, from}, :stop, :disconnected, data) do
    {:stop_and_reply, :normal, [{:reply, from, :ok}]}
  end

  def handle_event({:call, from}, content, _state, data) do
    send(data.gnat, {:"$gen_call", from, content})
    :keep_state_and_data
  end

  defp do_call(request, %{gnat: gnat}) do
    :gen_statem.call(gnat, request)
  end
end
