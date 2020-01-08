defmodule Stan do
  use GenServer

  def start_link(opts) do
    genserver_opts = Keyword.take(opts, [:name])
    GenServer.start_link(__MODULE__, Map.new(opts), genserver_opts)
  end

  def pub(stan, subject, body) do
    GenServer.call(stan, {:pub, subject, body})
  end

  def sub(stan, subscriber, subject, opts \\ []) do
    opts =
      [
        queue_group: nil,
        max_in_flight: 1,
        ack_wait_in_secs: 60_000,
        durable_name: nil,
        start_position: :new_only,
        start_sequence: nil,
        start_time_delta: nil
      ]
      |> Keyword.merge(opts)
      |> Map.new()

    GenServer.call(stan, {:sub, subscriber, subject, opts})
  end

  def unsub(stan, subscription) do
    GenServer.call(stan, {:unsub, :unsub, subscription})
  end

  def close(stan, subscription) do
    GenServer.call(stan, {:unsub, :close, subscription})
  end

  def ack(stan, ack) do
    GenServer.call(stan, {:ack, ack})
  end

  @impl true
  def init(%{connection_settings: connection_settings} = opts) do
    {:ok, gnat} = Gnat.start_link(connection_settings)

    opts =
      opts
      |> Map.delete(:connection_settings)
      |> Map.put(:gnat, gnat)

    init(opts)
  end

  @impl true
  def init(%{gnat: gnat} = opts) do
    conn_req = Pb.ConnectRequest.new(
      clientID: opts[:client_id] || nuid(),
      heartbeatInbox: "_HEARTBEAT.#{nuid()}",
      protocol: 1,
      connID: nuid(),
      pingInterval: 120,
      pingMaxOut: 3
    )

    {:ok, _} = Gnat.sub(gnat, self(), conn_req.heartbeatInbox)
    {:ok, conn_resp} = request(gnat, "_STAN.discover.test-cluster", conn_req)

    state = %{
      gnat: gnat,
      client_id: conn_req.clientID,
      conn_id: conn_req.connID,
      pub_prefix: conn_resp.pubPrefix,
      topic_heartbeat: conn_req.heartbeatInbox,
      topic_close: conn_resp.closeRequests,
      topic_ping: conn_resp.pingRequests,
      topic_sub_close: conn_resp.subCloseRequests,
      topic_sub: conn_resp.subRequests,
      topic_unsub: conn_resp.unsubRequests,
      subscriptions: %{}
    }

    Process.flag(:trap_exit, true)

    {:ok, state}
  end

  @impl true
  def terminate(_reason, state) do
    request(state.gnat, state.topic_close, Pb.CloseRequest.new(clientID: state.client_id))
    :ok
  end

  @impl true
  def handle_call({:pub, subject, body}, _from, state) do
    msg = Pb.PubMsg.new(
      clientId: state.client_id,
      guid: nuid(),
      subject: subject,
      reply: nil,
      data: body,
      connID: state.conn_id,
      sha256: nil
    )

    {:ok, _} = request(state.gnat, state.pub_prefix <> "." <> subject, msg)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:sub, subscriber, subject, opts}, _from, state) do
    inbox = "_INBOX.#{nuid()}"

    {:ok, gnat_sub} = Gnat.sub(state.gnat, self(), inbox)

    req = Pb.SubscriptionRequest.new(
      clientID: state.client_id,
      subject: subject,
      qGroup: opts.queue_group,
      inbox: inbox,
      maxInFlight: opts.max_in_flight,
      ackWaitInSecs: opts.ack_wait_in_secs,
      durableName: opts.durable_name,
      startPosition: convert_start_position(opts.start_position),
      startSequence: opts.start_sequence,
      startTimeDelta: opts.start_time_delta
    )

    {:ok, resp} = request(state.gnat, state.topic_sub, req)
    sub = Stan.Sub.new(req, resp, gnat_sub, subscriber)

    state = %{state | subscriptions: Map.put(state.subscriptions, inbox, sub)}

    {:reply, {:ok, sub}, state}
  end

  @impl true
  def handle_call({:unsub, kind, sub}, _from, state) do
    topic =
      case kind do
        :close -> state.topic_sub_close
        :unsub -> state.topic_unsub
      end

    req = Pb.UnsubscribeRequest.new(
      clientID: state.client_id,
      subject: sub.subject,
      inbox: sub.ack_inbox,
      durableName: sub.durable_name
    )

    {:ok, resp} = request(state.gnat, topic, req)
    :ok = Gnat.unsub(state.gnat, sub.gnat_sub)

    state = %{state | subscriptions: Map.delete(state.subscriptions, sub.inbox)}
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:ack, {ack_inbox, subject, sequence}}, _from, state) do
    msg = Pb.Ack.new(subject: subject, sequence: sequence)
    :ok = gnat_pub(state.gnat, ack_inbox, msg)
    {:reply, :ok, state}
  end

  @impl true
  def handle_info({:msg, %{topic: topic} = msg}, %{topic_heartbeat: topic} = state) do
    IO.inspect(msg, label: "#{topic} <<<")
    :ok = gnat_pub(state.gnat, msg.reply_to, Pb.Ping.new(connID: state.conn_id))
    {:noreply, state}
  end

  @impl true
  def handle_info({:msg, msg}, state) do
    case state.subscriptions[msg.topic] do
      nil ->
        IO.inspect(msg, label: "unexpected message!")

      sub ->
        dispatch_msg(sub, msg)
    end

    {:noreply, state}
  end

  defp nuid(), do: :crypto.strong_rand_bytes(12) |> Base.encode16

  defp request(gnat, topic, %req_module{} = req) do
    IO.inspect(req, label: ">>> #{topic}")
    resp_module = response_module(req_module)

    {:ok, %{body: body}} = Gnat.request(gnat, topic, req_module.encode(req))

    result = resp_module.decode(body)

    IO.inspect(result, label: "<<<")

    result
    |> case do
      %{error: ""} = result -> {:ok, result}
      %{error: error} = result -> {:error, error}
    end
  end

  defp gnat_pub(gnat, topic, %msg_module{} = msg) do
    IO.inspect(msg, label: ">>> #{topic} (no response expected)")
    :ok = Gnat.pub(gnat, topic, msg_module.encode(msg))
  end

  defp response_module(request_type)
  defp response_module(Pb.ConnectRequest), do: Pb.ConnectResponse
  defp response_module(Pb.CloseRequest), do: Pb.CloseResponse
  defp response_module(Pb.SubscriptionRequest), do: Pb.SubscriptionResponse
  defp response_module(Pb.UnsubscribeRequest), do: Pb.SubscriptionResponse
  defp response_module(Pb.PubMsg), do: Pb.PubAck
  defp response_module(Pb.Ping), do: Pb.PingResponse

  defp convert_start_position(:new_only), do: :NewOnly
  defp convert_start_position(:last_received), do: :LastReceived
  defp convert_start_position(:time_delta_start), do: :TimeDeltaStart
  defp convert_start_position(:sequence_start), do: :SequenceStart
  defp convert_start_position(:first), do: :First

  defp dispatch_msg(sub, %{body: body} = _msg) do
    msg_proto = Pb.MsgProto.decode(body)

    ack = {sub.ack_inbox, msg_proto.subject, msg_proto.sequence}

    msg = 
      msg_proto
      |> Map.from_struct()
      |> Map.take([:sequence, :subject, :data, :timestamp, :redelivered])

    send(sub.pid, {:msg, ack, msg})
  end
end
