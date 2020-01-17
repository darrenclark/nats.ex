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

  def set_allow_connection(proxy, allow?) do
    GenServer.call(proxy, {:set_allow_connection, allow?})
  end

  @impl true
  def init([listen_port, remote_port]) do
    {:ok, socket} = :gen_tcp.listen(listen_port, [:binary, packet: :raw, reuseaddr: true])

    state = %{
      allow?: true,
      conns: %{}
    }

    this = self()
    Task.start_link(fn -> accept(this, socket, remote_port) end)

    {:ok, state}
  end

  @impl true
  def handle_call({:set_allow_connection, allow?}, _from, %{allow?: allow?} = state) do
    {:reply, :ok, state}
  end
  def handle_call({:set_allow_connection, false}, _from, state) do
    state.conns
    |> Enum.each(fn {inc, out} ->
      :gen_tcp.shutdown(out, :read_write)
      :gen_tcp.shutdown(inc, :read_write)
    end)

    {:reply, :ok, %{state | allow?: false, conns: %{}}}
  end
  def handle_call({:set_allow_connection, true}, _from, state) do
    {:reply, :ok, %{state | allow?: true}}
  end

  @impl true
  def handle_cast({:new, inc, out}, state) do
    if state.allow? do
      conns = Map.put(state.conns, inc, out)
      {:noreply, %{state | conns: conns}}
    else
      :gen_tcp.shutdown(out, :read_write)
      :gen_tcp.shutdown(inc, :read_write)
      {:noreply, state}
    end
  end

  @impl true
  def handle_info({:tcp, sock, data}, state) do
    if opposite = opposite_socket(state, sock) do
      :gen_tcp.send(opposite, data)
    end
    {:noreply, state}
  end
  def handle_info({:tcp_closed, sock}, state) do
    if opposite = opposite_socket(state, sock) do
      :gen_tcp.shutdown(opposite, :read_write)
      conns = Map.drop(state.conns, [sock, opposite])
      {:noreply, %{state | conns: conns}}
    else
      {:noreply, state}
    end
  end

  defp accept(proxy, socket, remote_port) do
    IO.puts "ACCEPTING"
    {:ok, inc} = :gen_tcp.accept(socket)
    IO.puts "CONNECTING"
    {:ok, out} = :gen_tcp.connect('localhost', remote_port, [:binary, packet: :raw])
    IO.puts "CONNECTED!"

    :ok = :gen_tcp.controlling_process(inc, proxy)
    :ok = :gen_tcp.controlling_process(out, proxy)
    GenServer.cast(proxy, {:new, inc, out})

    accept(proxy, socket, remote_port)
  end

  defp opposite_socket(state, socket) do
    if opposite = Map.get(state.conns, socket) do
      opposite
    else
      state.conns
      |> Enum.find_value(fn
        {opposite, ^socket} -> opposite
        _ -> nil
      end)
    end
  end
end
