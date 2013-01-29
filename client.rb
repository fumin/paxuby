require 'socket'
module Paxos; end
class Paxos::Client
  def initialize host, port
    @host = host; @port = port
  end
  def get k
    s = TCPSocket.new @host, @port
    s.puts "GET #{k}"
    msg = s.gets
    s.close
    msg
  end
  def set k, v
    s = TCPSocket.new @host, @port
    s.puts "SET #{k} #{v}"
    msg = s.gets
    s.close
    msg
  end
  def del k
    s = TCPSocket.new @host, @port
    s.puts "DEL #{k}"
    msg = s.gets
    s.close
    msg
  end
end
