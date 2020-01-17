defmodule Gnat.ManagedTest do
  use ExUnit.Case, async: true
  doctest Gnat

  setup context do
    CheckForExpectedNatsServers.check(Map.keys(context))
    :ok
  end

  test "connect to a server" do
    {:ok, pid} = Gnat.Managed.start_link()
    assert Process.alive?(pid)
    :ok = Gnat.stop(pid)
  end

  test "subscribe to topic and receive a message" do
    {:ok, pid} = Gnat.Managed.start_link()
    {:ok, _ref} = Gnat.sub(pid, self(), "test")
    :ok = Gnat.pub(pid, "test", "yo dawg")

    assert_receive {:msg, %{topic: "test", body: "yo dawg", reply_to: nil}}, 1000
    :ok = Gnat.stop(pid)
  end

  test "subscribe receive a message with a reply_to" do
    {:ok, pid} = Gnat.Managed.start_link()
    {:ok, _ref} = Gnat.sub(pid, self(), "with_reply")
    :ok = Gnat.pub(pid, "with_reply", "yo dawg", reply_to: "me")

    assert_receive {:msg, %{topic: "with_reply", reply_to: "me", body: "yo dawg"}}, 1000
    :ok = Gnat.stop(pid)
  end

  test "receive multiple messages" do
    {:ok, pid} = Gnat.Managed.start_link()
    {:ok, _ref} = Gnat.sub(pid, self(), "test")
    :ok = Gnat.pub(pid, "test", "message 1")
    :ok = Gnat.pub(pid, "test", "message 2")
    :ok = Gnat.pub(pid, "test", "message 3")

    assert_receive {:msg, %{topic: "test", body: "message 1", reply_to: nil}}, 500
    assert_receive {:msg, %{topic: "test", body: "message 2", reply_to: nil}}, 500
    assert_receive {:msg, %{topic: "test", body: "message 3", reply_to: nil}}, 500
    :ok = Gnat.stop(pid)
  end

  test "subscribing to the same topic multiple times" do
    {:ok, pid} = Gnat.Managed.start_link()
    {:ok, _sub1} = Gnat.sub(pid, self(), "dup")
    {:ok, _sub2} = Gnat.sub(pid, self(), "dup")
    :ok = Gnat.pub(pid, "dup", "yo")
    :ok = Gnat.pub(pid, "dup", "ma")
    assert_receive {:msg, %{topic: "dup", body: "yo"}}, 500
    assert_receive {:msg, %{topic: "dup", body: "yo"}}, 500
    assert_receive {:msg, %{topic: "dup", body: "ma"}}, 500
    assert_receive {:msg, %{topic: "dup", body: "ma"}}, 500
  end

  test "subscribing to the same topic multiple times with a queue group" do
    {:ok, pid} = Gnat.Managed.start_link()
    {:ok, _sub1} = Gnat.sub(pid, self(), "dup", queue_group: "us")
    {:ok, _sub2} = Gnat.sub(pid, self(), "dup", queue_group: "us")
    :ok = Gnat.pub(pid, "dup", "yo")
    :ok = Gnat.pub(pid, "dup", "ma")
    assert_receive {:msg, %{topic: "dup", body: "yo"}}, 500
    assert_receive {:msg, %{topic: "dup", body: "ma"}}, 500
    refute_receive {:msg, _}, 500
  end

  test "unsubscribing from a topic" do
    topic = "testunsub"
    {:ok, pid} = Gnat.Managed.start_link()
    {:ok, sub_ref} = Gnat.sub(pid, self(), topic)
    :ok = Gnat.pub(pid, topic, "msg1")
    assert_receive {:msg, %{topic: ^topic, body: "msg1"}}, 500
    :ok = Gnat.unsub(pid, sub_ref)
    :ok = Gnat.pub(pid, topic, "msg2")
    refute_receive {:msg, %{topic: _topic, body: _body}}, 500
  end

  test "unsubscribing from a topic after a maximum number of messages" do
    topic = "testunsub_maxmsg"
    {:ok, pid} = Gnat.Managed.start_link()
    {:ok, sub_ref} = Gnat.sub(pid, self(), topic)
    :ok = Gnat.unsub(pid, sub_ref, max_messages: 2)
    :ok = Gnat.pub(pid, topic, "msg1")
    :ok = Gnat.pub(pid, topic, "msg2")
    :ok = Gnat.pub(pid, topic, "msg3")
    assert_receive {:msg, %{topic: ^topic, body: "msg1"}}, 500
    assert_receive {:msg, %{topic: ^topic, body: "msg2"}}, 500
    refute_receive {:msg, _}, 500
    assert {:ok, 0} = Gnat.active_subscriptions(pid)
  end

  test "active_subscriptions" do
    topic = "testunsub"
    {:ok, pid} = Gnat.Managed.start_link()
    {:ok, sub_ref} = Gnat.sub(pid, self(), topic)
    {:ok, sub_ref2} = Gnat.sub(pid, self(), topic)
    assert {:ok, 2} = Gnat.active_subscriptions(pid)
    :ok = Gnat.unsub(pid, sub_ref2)
    assert {:ok, 1} = Gnat.active_subscriptions(pid)
    :ok = Gnat.unsub(pid, sub_ref)
    assert {:ok, 0} = Gnat.active_subscriptions(pid)
  end

  test "request-reply convenience function" do
    topic = "req-resp"
    {:ok, pid} = Gnat.Managed.start_link()
    spin_up_echo_server_on_topic(self(), pid, topic)
    # Wait for server to spawn and subscribe.
    assert_receive(true, 100)
    {:ok, msg} = Gnat.request(pid, topic, "ohai", receive_timeout: 500)
    assert msg.body == "ohai"
  end

  defp spin_up_echo_server_on_topic(ready, gnat, topic) do
    spawn(fn ->
      {:ok, subscription} = Gnat.sub(gnat, self(), topic)
      :ok = Gnat.unsub(gnat, subscription, max_messages: 1)
      send ready, true
      receive do
        {:msg, %{topic: ^topic, body: body, reply_to: reply_to}} ->
          Gnat.pub(gnat, reply_to, body)
      end
    end)
  end

  test "doesnt crash when the connection dies" do
    IO.inspect(self(), label: "self")
    proxy = IO.inspect(start_supervised!({SimpleTcpProxy, [42220, 4222]}), label: "proxy")
    {:ok, pid} = Gnat.Managed.start_link(connection_settings: %{port: 42220}, debug: [:statistics, :trace])

    Process.sleep(1000)
    IO.inspect(pid, label: "gnat.managed")
    SimpleTcpProxy.disconnect(proxy)
    Process.sleep(1000)

    {:ok, _ref} = Gnat.sub(pid, self(), "test")
    :ok = Gnat.pub(pid, "test", "yo dawg")

    assert_receive {:msg, %{topic: "test", body: "yo dawg", reply_to: nil}}, 1000
    :ok = Gnat.stop(pid)
  end

  test "subscriptions are recreated on reconnection" do
    IO.inspect(self(), label: "self")
    proxy = IO.inspect(start_supervised!({SimpleTcpProxy, [42220, 4222]}), label: "proxy")
    {:ok, pid} = Gnat.Managed.start_link(connection_settings: %{port: 42220}, debug: [:statistics, :trace])
    {:ok, _ref} = Gnat.sub(pid, self(), "test")

    #Process.sleep(1000)
    IO.inspect(pid, label: "gnat.managed")
    SimpleTcpProxy.set_allow_connection(proxy, false)
    SimpleTcpProxy.set_allow_connection(proxy, true)
    #Process.sleep(1000)

    :ok = Gnat.pub(pid, "test", "yo dawg")

    assert_receive {:msg, %{topic: "test", body: "yo dawg", reply_to: nil}}, 1000
    :ok = Gnat.stop(pid)
  end

  test "pubs while disconnected are sent on reconnection" do
    proxy = start_supervised!({SimpleTcpProxy, [42221, 4222]})
    {:ok, pid} = Gnat.Managed.start_link(connection_settings: %{port: 42221}, debug: [:statistics, :trace])
    {:ok, _ref} = Gnat.sub(pid, self(), "test")
    SimpleTcpProxy.set_allow_connection(proxy, false)

    :ok = Gnat.pub(pid, "test", "yo dawg")
    SimpleTcpProxy.set_allow_connection(proxy, true)

    assert_receive {:msg, %{topic: "test", body: "yo dawg", reply_to: nil}}, 1000
    :ok = Gnat.stop(pid)
  end

  test "subscriptions while disconnected are created on reconnection" do
    proxy = start_supervised!({SimpleTcpProxy, [42222, 4222]})
    {:ok, pid} = Gnat.Managed.start_link(connection_settings: %{port: 42222}, debug: [:statistics, :trace])
    SimpleTcpProxy.set_allow_connection(proxy, false)

    {:ok, _ref} = Gnat.sub(pid, self(), "test")
    :ok = Gnat.pub(pid, "test", "yo dawg")

    SimpleTcpProxy.set_allow_connection(proxy, true)

    assert_receive {:msg, %{topic: "test", body: "yo dawg", reply_to: nil}}, 10_000
    :ok = Gnat.stop(pid)
  end

  test "unsubs while disconnected are handled immediately" do
    proxy = start_supervised!({SimpleTcpProxy, [42223, 4222]})
    {:ok, pid} = Gnat.Managed.start_link(connection_settings: %{port: 42223}, debug: [:statistics, :trace])
    SimpleTcpProxy.set_allow_connection(proxy, false)

    {:ok, sid} = Gnat.sub(pid, self(), "test")
    :ok = Gnat.pub(pid, "test", "yo dawg")
    :ok = Gnat.unsub(pid, sid)

    SimpleTcpProxy.set_allow_connection(proxy, true)

    refute_receive {:msg, %{topic: "test", body: "yo dawg", reply_to: nil}}, 10_000
    :ok = Gnat.stop(pid)
  end

  test "unsubs with max_messages while disconnected are handled correctly" do
    proxy = start_supervised!({SimpleTcpProxy, [42224, 4222]})
    {:ok, pid} = Gnat.Managed.start_link(connection_settings: %{port: 42224}, debug: [:statistics, :trace])
    SimpleTcpProxy.set_allow_connection(proxy, false)

    {:ok, sid} = Gnat.sub(pid, self(), "test")
    :ok = Gnat.pub(pid, "test", "yo dawg")
    :ok = Gnat.pub(pid, "test", "yo dawg 2")
    :ok = Gnat.unsub(pid, sid, max_messages: 1)

    SimpleTcpProxy.set_allow_connection(proxy, true)

    assert_receive {:msg, %{topic: "test", body: "yo dawg", reply_to: nil}}, 10_000
    refute_receive {:msg, %{topic: "test", body: "yo dawg", reply_to: nil}}, 10_000
    :ok = Gnat.stop(pid)
  end

#
#  test "recording errors from the broker" do
#    import ExUnit.CaptureLog
#    {:ok, gnat} = Gnat.start_link()
#    assert capture_log(fn ->
#      Process.flag(:trap_exit, true)
#      Gnat.sub(gnat, self(), "invalid. subject")
#      Process.sleep(20) # errors are reported asynchronously so we need to wait a moment
#    end) =~ "Invalid Subject"
#  end
#
#  test "connection timeout" do
#    start = System.monotonic_time(:millisecond)
#    connection_settings = %{ host: '169.33.33.33', connection_timeout: 200 }
#    {:stop, :timeout} = Gnat.init(connection_settings)
#    assert_in_delta System.monotonic_time(:millisecond) - start, 200, 10
#  end
##  @tag :multi_server
#  test "connect to a server with user/pass authentication" do
#    connection_settings = %{
#      host: "localhost",
#      port: 4223,
#      tcp_opts: [:binary],
#      username: "bob",
#      password: "alice"
#    }
#    {:ok, pid} = Gnat.start_link(connection_settings)
#    assert Process.alive?(pid)
#    :ok = Gnat.ping(pid)
#    :ok = Gnat.stop(pid)
#  end
#
#  @tag :multi_server
#  test "connect to a server with token authentication" do
#    connection_settings = %{
#      host: "localhost",
#      port: 4226,
#      tcp_opts: [:binary],
#      token: "SpecialToken",
#      auth_required: true
#    }
#    {:ok, pid} = Gnat.start_link(connection_settings)
#    assert Process.alive?(pid)
#    :ok = Gnat.ping(pid)
#    :ok = Gnat.stop(pid)
#  end
#
#  @tag :multi_server
#  test "connet to a server which requires TLS" do
#    connection_settings = %{port: 4224, tls: true}
#    {:ok, gnat} = Gnat.start_link(connection_settings)
#    assert Gnat.ping(gnat) == :ok
#    assert Gnat.stop(gnat) == :ok
#  end
#
#  @tag :multi_server
#  test "connect to a server which requires TLS with a client certificate" do
#    connection_settings = %{
#      port: 4225,
#      tls: true,
#      ssl_opts: [
#        certfile: "test/fixtures/client-cert.pem",
#        keyfile: "test/fixtures/client-key.pem",
#      ],
#    }
#    {:ok, gnat} = Gnat.start_link(connection_settings)
#    assert Gnat.ping(gnat) == :ok
#    assert Gnat.stop(gnat) == :ok
#  end
#
end
