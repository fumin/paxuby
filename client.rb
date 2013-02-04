require 'socket'
require './paxos_app'

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
    return "" unless msg
    msg[-1] == "\n" ? msg[0...-1] : msg
  end

  def puts_gets_follow_redirect in_msg, sleep_secs=0
    loop do
      msg = puts_and_gets in_msg
      if m = msg.match(/^Please contact the leader: ([\w\.]+:\d+)$/)
        @addr = m[1]
      end
      return msg if msg =~ /^[\w\.]+:\d+$/
      sleep(sleep_secs) if sleep_secs > 0
    end
  end

  def find_leader
    puts_gets_follow_redirect "#{Paxos::App::Command::GET} leader", rand
  end

end
