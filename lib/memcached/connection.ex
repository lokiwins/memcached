defmodule Memcached.Connection do
  use Connection
  alias Memcached.Packet

  @default_config [
    host: "localhost",
    port: 11211
  ]

  def get(conn, key) do
    case Connection.call(conn, {:get, key}) do
      {:error, :not_found} = result -> result
      result -> result
    end
  end

  def set(conn, item, opts \\ []) do
    Connection.call(conn, {:set, item, opts})
  end

  def start_link(config \\ []) do
    config = Keyword.merge(@default_config, config)
    Connection.start_link(__MODULE__, config)
  end

  def init(config) do
    state = %{
      config: config,
      socket: nil
    }
    {:connect, :init, state}
  end

  def connect(_, state) do
    %{config: config} = state

    host = config[:host]
    |> String.to_charlist()

    port = config[:port]

    case :gen_tcp.connect(host, port, [:binary, active: true]) do
      {:ok, socket} ->
        {:ok, %{state | socket: socket}}

      {:error, reason} ->
        IO.inspect(reason)
        {:backoff, 1000, state}
    end
  end

  def disconnect(reason, state) do
    IO.inspect(reason)

    %{socket: socket} = state

    :ok = :gen_tcp.close(socket)

    {:connect, reason, %{state | socket: nil}}
  end

  def handle_call({:get, key}, _, state) do
    %{socket: socket} = state

    packet = Packet.get(key)


    with :ok <- :inet.setopts(socket, active: false),
      :ok <- :gen_tcp.send(socket, packet),
      {:ok, packet} <- Packet.recv_packet(socket),
      :ok <- :inet.setopts(socket, active: true)
    do
      case packet.status do
        :ok -> {:reply, {:ok, packet.value}, state}
        reason -> {:reply, {:error, reason}, state}
      end
    else
      {:error, reason} -> {:disconnect, reason, state}
    end
  end

  def handle_call({:set, item, opts}, _, state) do
    %{socket: socket} = state
    {key, value} = item
    ttl = Keyword.get(opts, :ttl, 0)
    cas = Keyword.get(opts, :cas, 0)
    flags = Keyword.get(opts, :flags, 0)



    with :ok <- :inet.setopts(socket, active: false),
      packet = Packet.set(key, value, ttl, cas, flags),
      :ok <- :gen_tcp.send(socket, packet),
      {:ok, packet} <- Packet.recv_packet(socket),
      :ok <- :inet.setopts(socket, active: true)
    do
      case packet.status do
        :ok -> {:reply, :ok, state}
        reason -> {:reply, {:error, reason}, state}
      end
    else
      {:error, reason} -> {:disconnect, reason, state}
    end
  end

end
