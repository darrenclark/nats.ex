ExUnit.configure(exclude: [:pending, :property, :multi_server])

ExUnit.start()

case :gen_tcp.connect('localhost', 4222, [:binary]) do
  {:ok, socket} ->
    :gen_tcp.close(socket)
  {:error, reason} ->
    Mix.raise "Cannot connect to gnatsd" <>
              " (http://localhost:4222):" <>
              " #{:inet.format_error(reason)}\n" <>
              "You probably need to start gnatsd."
end

# this is used by some property tests, see test/gnat_property_test.exs
Gnat.start_link(%{}, [name: :test_connection])

defmodule RpcEndpoint do
  def init do
    {:ok, pid} = Gnat.start_link()
    {:ok, _ref} = Gnat.sub(pid, self(), "rpc.>")
    loop(pid)
  end

  def loop(pid) do
    receive do
      {:msg, %{body: body, reply_to: topic}} ->
        Gnat.pub(pid, topic, body)
        loop(pid)
    end
  end
end
spawn(&RpcEndpoint.init/0)

defmodule CheckForExpectedNatsServers do
  def check(tags) do
    check_for_default()
    Enum.each(tags, &check_for_tag/1)
  end

  def check_for_default do
    case :gen_tcp.connect('localhost', 4222, [:binary]) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
      {:error, reason} ->
        Mix.raise "Cannot connect to gnatsd" <>
                  " (tcp://localhost:4222):" <>
                  " #{:inet.format_error(reason)}\n" <>
                  "You probably need to start gnatsd."
    end
  end

  def check_for_tag(:multi_server) do
    case :gen_tcp.connect('localhost', 4223, [:binary]) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
      {:error, reason} ->
        Mix.raise "Cannot connect to gnatsd" <>
                  " (tcp://localhost:4223):" <>
                  " #{:inet.format_error(reason)}\n" <>
                  "You probably need to start a gnatsd " <>
                  "server that requires authentication with " <>
                  "the following command `gnatsd -p 4223 " <>
                  "--user bob --pass alice`."
    end

    case :gen_tcp.connect('localhost', 4224, [:binary]) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
      {:error, reason} ->
        Mix.raise "Cannot connect to gnatsd" <>
                  " (tcp://localhost:4224):" <>
                  " #{:inet.format_error(reason)}\n" <>
                  "You probably need to start a gnatsd " <>
                  "server that requires tls with " <>
                  "a command like `gnatsd -p 4224 " <>
                  "--tls --tlscert test/fixtures/server.pem " <>
                  "--tlskey test/fixtures/key.pem`."
    end

    case :gen_tcp.connect('localhost', 4225, [:binary]) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
      {:error, reason} ->
        Mix.raise "Cannot connect to gnatsd" <>
                  " (tcp://localhost:4225):" <>
                  " #{:inet.format_error(reason)}\n" <>
                  "You probably need to start a gnatsd " <>
                  "server that requires tls with " <>
                  "a command like `gnatsd -p 4225 --tls " <>
                  "--tlscert test/fixtures/server.pem " <>
                  "--tlskey test/fixtures/key.pem " <>
                  "--tlscacert test/fixtures/ca.pem --tlsverify"
    end

    case :gen_tcp.connect('localhost', 4226, [:binary]) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
      {:error, reason} ->
        Mix.raise "Cannot connect to gnatsd" <>
                  " (tcp://localhost:4226):" <>
                  " #{:inet.format_error(reason)}\n" <>
                  "You probably need to start a gnatsd " <>
                  "server that requires authentication with " <>
                  "the following command `gnatsd -p 4226 " <>
                  "--auth SpecialToken`."
    end
  end
  def check_for_tag(_), do: :ok
end

defmodule SimpleTcpProxy do
  use GenServer

  def start_link([_listen_port, _remote_port] = init_arg) do
    GenServer.start_link(__MODULE__, init_arg, [])
  end

  def disconnect(proxy) do
    GenServer.call(proxy, :disconnect)
  end

  @impl true
  def init([listen_port, remote_port]) do
    {:ok, socket} = :gen_tcp.listen(listen_port, [:binary, packet: :raw, reuseaddr: true])

    state = %{
      listen_port: listen_port,
      remote_port: remote_port,
      socket: socket,
      inc: nil,
      out: nil
    }

    send(self(), :accept)
    {:ok, state}
  end

  @impl true
  def handle_call(:disconnect, _from, %{inc: nil} = state) do
    {:reply, :ok, state}
  end
  def handle_call(:disconnect, _from, state) do
    :gen_tcp.shutdown(state.out, :read_write)
    :gen_tcp.shutdown(state.inc, :read_write)
    send(self(), :accept)
    {:reply, :ok, state}
  end

  @impl true
  def handle_info(:accept, %{inc: nil} = state) do
    IO.puts "ACCEPTING"
    {:ok, inc} = :gen_tcp.accept(state.socket)
    IO.puts "CONNECTING"
    {:ok, out} = :gen_tcp.connect('localhost', state.remote_port, [:binary, packet: :raw])

    {:noreply, %{state | inc: inc, out: out}}
  end
  def handle_info(:accept, state) do
    {:noreply, state}
  end
  def handle_info({:tcp, inc, data}, %{inc: inc} = state) do
    :gen_tcp.send(state.out, data)
    {:noreply, state}
  end
  def handle_info({:tcp, out, data}, %{out: out} = state) do
    :gen_tcp.send(state.inc, data)
    {:noreply, state}
  end
  def handle_info({:tcp, _, _}, state) do
    {:noreply, state}
  end
  def handle_info({:tcp_closed, inc}, %{inc: inc} = state) do
    :gen_tcp.shutdown(state.out, :read_write)
    send(self(), :accept)
    {:noreply, %{state | inc: nil, out: nil}}
  end
  def handle_info({:tcp_closed, out}, %{out: out} = state) do
    :gen_tcp.shutdown(state.inc, :read_write)
    send(self(), :accept)
    {:noreply, %{state | inc: nil, out: nil}}
  end
  def handle_info({:tcp_closed, _}, state) do
    {:noreply, state}
  end
end
