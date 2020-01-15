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
  defrecordp(:sub_data, sid: nil, esid: nil, receiver: nil)


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
    # TODO: handle max_messages

    {:ok, sid} = :gen_statem.call(data.gnat, {:sub, self(), topic, opts})
    esid = :erlang.unique_integer()

    sub = sub_data(sid: sid, esid: esid, receiver: receiver)
    true = :ets.insert_new(data.subs, sub)

    {:keep_state_and_data, [{:reply, from, {:ok, esid}}]}
  end

  def connected({:call, from}, {:pub, topic, message, opts}, data) do
    result = :gen_statem.call(data.gnat, {:pub, topic, message, opts})
    {:keep_state_and_data, [{:reply, from, result}]}
  end

  #def connected({:call, from}, request, data) do
  #  send(data.gnat, {:"$gen_call", from, request})
  #  :keep_state_and_data
  #end

  def connected(:info, {:msg, message}, data) do
    with [sub_data(esid: esid, receiver: pid)] <- :ets.lookup(data.subs, message.sid) do
      message = {:msg, %{message | sid: esid, gnat: self()}}
      send(pid, message)
    end

    :keep_state_and_data
  end
end
