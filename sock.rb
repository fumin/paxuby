require 'socket'
require 'timeout'

module Paxos; end
module Paxos::Sock
  module_function
  def connect_with_timeout addr_, timeout
    addr = if Paxos::LocalData['local_addr'] == addr_
             "127.0.0.1:#{Paxos::LocalData['local_port']}"
           else
             addr_
           end
    m = addr.match(/([\w\.]+):(\d+)/)
    socket = Socket.new(Socket::AF_INET, Socket::SOCK_STREAM, 0)
    begin; socket.connect_nonblock(Socket.pack_sockaddr_in(m[2].to_i, m[1]))
    rescue Errno::EINPROGRESS, Errno::EHOSTUNREACH; end
    rs, ws, = IO.select([], [socket], [], timeout)
    if ws
      ws[0]
    else
      nil
    end
  end

  def gets_with_timeout socket, timeout
    str_ = ''
    deadline = Time.now.to_f + timeout
    time_left = timeout
    while time_left > 0
      rs, ws, = IO.select([socket], [], [], time_left)
      raise Timeout::Error unless rs
      str_ << socket.recv(1024)
      break if str_[-1] == "\n"
      time_left = deadline - Time.now.to_f
    end

    raise Timeout::Error unless str_[-1] == "\n"
    str_[0...-1]
  end

  def puts_with_timeout socket, msg_, timeout
    msg = (msg_[-1] == "\n" ? msg_ : "#{msg_}\n")
    deadline = Time.now.to_f + timeout
    time_left = timeout
    offset = 0
    while time_left > 0 and offset < msg.size do
      rs, ws, = IO.select([], [socket], [], time_left)
      if ws.size > 0
        numbytes_sent = socket.send msg[offset..-1], 0
        offset += numbytes_sent
      end
      time_left = deadline - Time.now.to_f
    end
    offset
  end

  def puts_and_close socket, msg, timeout=0.2
    begin
      puts_with_timeout socket, msg, timeout
    rescue Errno::EPIPE
    ensure socket.close
    end
  end
end
