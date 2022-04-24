defmodule Memcached.Packet do
  @request 0x80
  @response 0x81

  @get 0x00
  @set 0x01

  def get(key) do
    key_size = byte_size(key)

    [
      # Magic
      @request,
      # Opcode
      @get,
      # Key Length
      <<key_size::size(16)>>,
      # Extras Length
      0x00,
      # Data Type
      0x00,
      # vbucket id
      <<0x00::size(16)>>,
      # Total body length
      <<key_size::size(32)>>,
      # Opaque
      <<0x00::size(32)>>,
      # CAS
      <<0x00::size(64)>>,
      # Key
      key
    ]
  end

  def set(key, value, ttl, cas, flags) do
    key_size = byte_size(key)
    value_size = byte_size(value)
    total_body_length = key_size + value_size + 8 # The +8 is from extras which always has a length of 8 bytes.
    # 4 bytes for flags and 4 bytes for expiration

    [
      # Magic
      @request,
      # Opcode,
      @set,
      # Key Length
      <<key_size::size(16)>>,
      # Extras Length
      0x08,
      # Data Type
      0x00,
      # vbucket id
      <<0x00::size(16)>>,
      # Total body length
      <<total_body_length::size(32)>>,
      # Opaque
      <<0x00::size(32)>>,
      # CAS
      <<cas::size(64)>>,
      <<flags::size(32)>>,
      <<ttl::size(32)>>,
      # Key
      key,
      value
    ]
  end

  def recv_packet(socket) do
    # Response header is 24 bytes
    with {:ok, header} <- :gen_tcp.recv(socket, 24) do
      # Get the body length out of the header which is 8 bytes into the header and has a size of 4 bytes
      <<_begining::size(64), body_length::size(32), _rest::binary>> = header

      # Knowing the body length we can recv the response body

      if body_length == 0 do
        {:ok, parse_packet(header, "")}
      else
        with {:ok, body} <- :gen_tcp.recv(socket, body_length) do
          {:ok, parse_packet(header, body)}
        end
      end
    end
  end

  defp parse_packet(header, body) do
    <<
      @response,
      opcode::size(8),
      key_length::size(16),
      extras_length::size(8),
      data_type::size(8),
      status::size(16),
      total_body_length::size(32),
      opaque::size(32),
      cas::size(64)
    >> = header

    value_length = total_body_length - extras_length - key_length

    <<
      extras::binary-size(extras_length),
      key::binary-size(key_length),
      value::binary-size(value_length)
    >> = body

    %{
      opcode: opcode,
      key_size: key_length,
      data_type: data_type,
      status: parse_status(status),
      value_size: value_length,
      opaque: opaque,
      cas: cas,
      key: key,
      value: value,
      extras: extras
    }
  end

  defp parse_status(0x0000), do: :ok
  defp parse_status(0x0001), do: :not_found
  defp parse_status(0x0002), do: :exists
  defp parse_status(0x0003), do: :too_large
  defp parse_status(0x0004), do: :invalid_args
  defp parse_status(0x0005), do: :no_stored
  defp parse_status(0x0006), do: :non_numeric
  defp parse_status(0x0007), do: :vbucket_error
  defp parse_status(0x0008), do: :auth_error
  defp parse_status(0x0009), do: :auth_cont
  defp parse_status(0x0081), do: :unknown_cmd
  defp parse_status(0x0082), do: :oom
  defp parse_status(0x0083), do: :not_supported
  defp parse_status(0x0084), do: :internal_error
  defp parse_status(0x0085), do: :busy
  defp parse_status(0x0086), do: :temp_failure

end
