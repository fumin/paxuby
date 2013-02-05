require 'socket'
require './paxos'
require './paxos_app'

module Paxos; end
class Paxos::Client
  MAX_REDIRECT_DEPTH = 5
  def initialize addr, cluster_addrs=[]
    @addr = addr; @cluster_addrs = cluster_addrs
  end

  def puts_and_gets in_msg
    s = Paxos::Sock.connect_with_timeout @addr, 0.2
    raise Errno::ECONNREFUSED unless s
    msg = ''
    begin
      s.puts in_msg
      msg = s.gets
    rescue Errno::EPIPE
    ensure s.close
    end
    msg[-1] == "\n" ? msg[0...-1] : msg
  end

  def puts_gets_follow_redirect in_msg, recursion_depth=0
    msg = puts_and_gets in_msg
    if m = msg.match(/^Please contact the leader: ([\w\.]+:\d+)$/)
      @addr = m[1]
      #sleep(rand)
      if recursion_depth < MAX_REDIRECT_DEPTH
        msg = puts_gets_follow_redirect in_msg, recursion_depth+1
      end
    end
    msg
  end

  def find_leader
    loop do
      addr = ''
      begin
        addr = puts_gets_follow_redirect "#{Paxos::App::Command::GET} leader"
      rescue Errno::ECONNREFUSED => e; end
      if addr =~ /^[\w\.]+:\d+$/
        @addr = addr; return @addr
      end
      if @cluster_addrs.size > 0 and rand(3) == 0
        @addr = @cluster_addrs.sample
      end
    end
  end
end
