defmodule Gnat.Managed do
  @moduledoc """

  ## States
  
  #### `:connected`

    - connection is good
    - pubs/subs/unsubs are sent immediately

  """
  @behaviour :gen_statem

  require Logger
  import Record, only: [defrecordp: 2]
  defrecordp(:sub_data, sid: nil, esid: nil, receiver: nil, unsub_after: :infinity)


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
  def callback_mode(), do: :state_functions

  @impl :gen_statem
  def init(opts) do
    connection_settings = opts[:connection_settings] || %{}

    {:ok, gnat} = Gnat.start_link(connection_settings)

    data = %{
      gnat: gnat,
      subs: :ets.new(:subscriptions, [:private, keypos: 2]),
      connection_settings: Map.new(connection_settings),
      retries: 0
    }

    {:ok, :connected, %{data | gnat: gnat}}
  end

  def connected({:call, from}, :stop, data) do
    :gen_statem.call(data.gnat, :stop)
    {:stop_and_reply, :normal, [{:reply, from, :ok}]}
  end

  def connected({:call, from}, {:sub, receiver, topic, opts}, data) do
    {:ok, sid} = :gen_statem.call(data.gnat, {:sub, self(), topic, opts})
    esid = :erlang.unique_integer()

    sub = sub_data(sid: sid, esid: esid, receiver: receiver)
    true = :ets.insert_new(data.subs, sub)

    {:keep_state_and_data, [{:reply, from, {:ok, esid}}]}
  end

  def connected({:call, from}, {:unsub, topic, opts}, data) when is_binary(topic) do
    result = :gen_statem.call(data.gnat, {:unsub, topic, opts})
    {:keep_state_and_data, [{:reply, from, result}]}
  end

  def connected({:call, from}, {:unsub, esid, opts}, data) do
    with [sub_data(sid: sid)] <- :ets.match_object(data.subs, sub_data(esid: esid, _: :_)) do
      :gen_statem.call(data.gnat, {:unsub, sid, opts})

      case opts[:max_messages] do
        nil ->
          :ets.delete(data.subs, sid)
        max ->
          :ets.update_element(data.subs, sid, {sub_data(:unsub_after) + 1, max})
      end
    end

    {:keep_state_and_data, [{:reply, from, :ok}]}
  end

  def connected({:call, from}, {:pub, topic, message, opts}, data) do
    result = :gen_statem.call(data.gnat, {:pub, topic, message, opts})
    {:keep_state_and_data, [{:reply, from, result}]}
  end

  def connected({:call, from}, {:request, request}, data) do
    result = :gen_statem.call(data.gnat, {:request, request})
    {:keep_state_and_data, [{:reply, from, result}]}
  end

  def connected({:call, from}, :active_subscriptions, data) do
    info = :ets.info(data.subs)
    result = {:ok, info[:size]}
    {:keep_state_and_data, [{:reply, from, result}]}
  end

  #def connected({:call, from}, request, data) do
  #  send(data.gnat, {:"$gen_call", from, request})
  #  :keep_state_and_data
  #end

  def connected(:info, {:msg, message}, data) do
    data.subs
    |> :ets.lookup(message.sid)
    |> Enum.each(&handle_message(message, &1, data))

    :keep_state_and_data
  end

  defp handle_message(msg, sub_data(unsub_after: :infinity) = sub, _data) do
    send_message(msg, sub)
  end

  defp handle_message(msg, sub_data(sid: sid, unsub_after: 1) = sub, data) do
    send_message(msg, sub)
    :ets.delete(data.subs, sid)
  end

  defp handle_message(msg, sub_data(sid: sid, unsub_after: _) = sub, data) do
    send_message(msg, sub)
    :ets.update_counter(data.subs, sid, {sub_data(:unsub_after) + 1, -1})
  end

  defp send_message(msg, sub_data(esid: esid, receiver: pid)) do
    message = {:msg, %{msg | sid: esid, gnat: self()}}
    send(pid, message)
  end
end
