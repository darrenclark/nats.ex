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
  defrecordp(:sub_data, sid: nil, esid: nil, receiver: nil, topic: "", sub_opts: [], unsub_after: :infinity)


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
  def callback_mode(), do: [:state_functions, :state_enter]

  @impl :gen_statem
  def init(opts) do
    Process.flag(:trap_exit, true)

    connection_settings = opts[:connection_settings] || %{}

    data = %{
      gnat: nil,
      subs: :ets.new(:subscriptions, [:private, keypos: 2]),
      connection_settings: Map.new(connection_settings),
      retries: 0
    }

    # require initial connection - probably got worse issues if we can't connect
    {:ok, data} = connect(data)

    {:ok, :connected, data}
  end

  defp connect(data) do
    case Gnat.start_link(data.connection_settings) do
      {:ok, gnat} ->
        IO.inspect(gnat, label: "gnat")
        {:ok, %{data | gnat: gnat, retries: 0}}

      {:error, reason} ->
        IO.inspect(reason, label: "gnat :error -> reason")
        {:error, :reason, %{data | gnat: nil, retries: data.retries + 1}}
    end
  end

  def reconnecting(:enter, _previous, data) do
    duration = data.retries * 1000
    {:keep_state_and_data, {:state_timeout, duration, :retry}}
  end
  def reconnecting(:state_timeout, :retry, data) do
    case connect(data) do
      {:ok, data} ->
        {:next_state, :syncing, data, {:next_event, :internal, :sync_subs}}

      {:error, _reason, data} ->
        {:repeat_state, data}
    end
  end
  def reconnecting({:call, _from}, _request, _data) do
    {:keep_state_and_data, :postpone}
  end
  def reconnecting(:info, _request, _data) do
    {:keep_state_and_data, :postpone}
  end

  def syncing(:enter, _previous, _data) do
    :keep_state_and_data
  end
  def syncing(:internal, :sync_subs, data) do
    subs = :ets.tab2list(data.subs)

    subs
    |> Enum.sort_by(fn sub_data(sid: sid) -> sid end)
    |> Enum.each(fn sub ->
      sub_data(topic: topic, sub_opts: opts) = sub
      {:ok, sid} = try_call(data, {:sub, self(), topic, opts})

      old_sid = sub_data(sub, :sid)
      :ets.delete(data.subs, old_sid)
      new_sub = sub_data(sub, sid: sid)
      :ets.insert_new(data.subs, new_sub)

      case sub_data(new_sub, :unsub_after) do
        :infinity -> :ok
        count -> try_call(data.gnat, {:unsub, sid, [max_messages: count]})
      end
    end)

    {:next_state, :connected, data}
  end

  def connected(:enter, _, _data) do
    # TODO: sync subscriptions
    :keep_state_and_data
  end
  def connected({:call, from}, :stop, data) do
    :gen_statem.call(data.gnat, :stop)
    {:stop_and_reply, :normal, [{:reply, from, :ok}]}
  end
  def connected({:call, from}, {:sub, receiver, topic, opts}, data) do
    {:ok, sid} = try_call(data, {:sub, self(), topic, opts})
    esid = :erlang.unique_integer()

    sub = sub_data(sid: sid, esid: esid, receiver: receiver, topic: topic, sub_opts: opts)
    true = :ets.insert_new(data.subs, sub)

    {:keep_state_and_data, [{:reply, from, {:ok, esid}}]}
  end
  def connected({:call, from}, {:unsub, topic, opts}, data) when is_binary(topic) do
    result = try_call(data, {:unsub, topic, opts})
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
    result = try_call(data, {:pub, topic, message, opts})
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
  def connected(:info, {:msg, message}, data) do
    data.subs
    |> :ets.lookup(message.sid)
    |> Enum.each(&handle_message(message, &1, data))

    :keep_state_and_data
  end
  def connected(:info, {:EXIT, gnat, _}, %{gnat: gnat} = data) do
    {:next_state, :reconnecting, %{data | gnat: nil}}
  end
  def connected(:info, {:EXIT, _pid, _reason}, _data) do
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

  defp try_call(data, request) do
    try do
      :gen_statem.call(data.gnat, request)
    catch
      :exit, {:noproc, _} ->
        throw {:next_state, :reconnecting, %{data | gnat: nil}, :postpone}

      :exit, {reason, _} when is_binary(reason) ->
        throw {:next_state, :reconnecting, %{data | gnat: nil}, :postpone}

      :exit, reason when is_binary(reason) ->
        IO.inspect(reason, label: "try_call EXIT - reason")
        exit(reason)
    end
  end
end
