require 'socket'
module Paxos; end
class Paxos::Client
  def initialize addr
    @addr = addr
  end
  def tcp_socket addr, timeout=0.2
    m = addr.match(/([\w\d\.]+):(\d+)/)
    socket = Socket.new(Socket::AF_INET, Socket::SOCK_STREAM, 0)
    begin; socket.connect_nonblock(Socket.pack_sockaddr_in(m[2].to_i, m[1]))
    rescue Errno::EINPROGRESS; end
    rs, ws, = IO.select([], [socket], [], timeout)
    if ws
      ws[0]
    else
      nil
    end
  end
  def puts_and_gets in_msg
    s = tcp_socket @addr
    raise Errno::ECONNREFUSED unless s
    s.puts in_msg
    msg = s.gets
    s.close
    msg[-1] == "\n" ? msg[0...-1] : msg
  end
  def get k
    puts_and_gets "GET #{k}"
  end
  def set k, v
    puts_and_gets "SET #{k} #{v}"
  end
  def del k
    puts_and_gets "DEL #{k}"
  end
end
