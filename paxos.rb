require 'thread'

require './entities'
require './disk'
require './sock'

module Paxos
  H = {}
  AcceptorQueue = Queue.new
  ClientHandlerQueue = Queue.new
  CatchupQueue = Queue.new
  LocalData = {}
  SmallestProposeN = 1

  HeartbeatPing = 'paxos_heartbeat_ping'
  HeartbeatPong = 'paxos_heartbeat_pong'
  HeartbeatTimeout = 0.75 # seconds

  ClientHandlerRefreshId = 'client_handler_refresh_id'
  module_function
  def propose id, command, opts={}
    res = Paxos::FailedProposal.new(id, 1)
    timeout = (opts[:timeout].to_f > 0 ? opts[:timeout].to_f : Float::INFINITY)
    opts[:disk_conn] ||= disk_conn

    start_time = Time.now.to_f
    while Time.now.to_f < start_time + timeout do
      res = Paxos._propose id, command, opts
      return res if res.is_a?(Paxos::SuccessfulProposal)
      opts[:n] = increment_n(res.enemy_n)
    end
    res
  end
  def _propose id, command, opts={}
    n = opts[:n] || choose_n_from_disk(id, opts[:disk_conn]) || SmallestProposeN
    raise "pid: #{Process.pid}, local_addr: #{LocalData['local_addr']}, #{n} < SmallestProposeN, opts = #{opts}, id = #{id}" unless n >= SmallestProposeN
    promise_msgs, ignore_msgs = send_prepare_msgs id, n

    # send accept message if a majority of replicas promised us
    if promise_msgs.size > H['addrs'].size.to_f / 2
      promise_msgs_with_n = promise_msgs.select{|m| m.n}
      v = if promise_msgs_with_n.size > 0
            promise_msgs_with_n.max{|a, b| a.n <=> b.n}.v || command
          else
            command
          end
      accepted_msgs, ignore_msgs = send_accept_msgs id, n, v

      # consenses reached if a majority of replicas accepted
      if accepted_msgs.size > H['addrs'].size.to_f/2
        return SuccessfulProposal.new(id, n, v, accepted_msgs)
      end
    end

    largest_ignore_msgs = ignore_msgs.max{|a, b| a.n <=> b.n}
    FailedProposal.new(id, (largest_ignore_msgs ? largest_ignore_msgs.n : n))
  end
  def send_prepare_msgs id, n
    msg_to_be_sent = "#{id} #{Msg::PREPARE} #{n}"
    responses = send_msg_to_acceptors_and_collect_reponses msg_to_be_sent
    promise_msgs = responses.select{|r| r.type == Msg::PROMISE}
    ignore_msgs = responses.select{|r| r.type == Msg::IGNORED}
    [promise_msgs, ignore_msgs]
  end
  def send_accept_msgs id, n, v
    msg_to_be_sent = "#{id} #{Msg::ACCEPT} #{n} #{v}"
    responses = send_msg_to_acceptors_and_collect_reponses msg_to_be_sent
    accepted_msgs = responses.select{|r| r.type == Msg::ACCEPTED}
    ignore_msgs = responses.select{|r| r.type == Msg::IGNORED}
    [accepted_msgs, ignore_msgs]
  end
  def send_msg_to_acceptors_and_collect_reponses msg_to_be_sent
    responses = []; threads = [];
    H['addrs'].each do |addr|
      next unless (s = Sock.connect_with_timeout(addr, 0.2))
      threads << Thread.new(s) do |acceptor|
        str_msg = nil
        begin
          Sock.puts_with_timeout acceptor, msg_to_be_sent, 0.1
          str_msg = Sock.gets_with_timeout acceptor, 0.1
        rescue Timeout::Error, Errno::ECONNRESET, Errno::EPIPE
        ensure acceptor.close; end
        msg = Paxos::Msg.new str_msg
        responses << msg if msg.type
      end
    end
    threads.each{|t| t.join}
    responses
  end

  def should_become_leader?
    result = true; msg = nil
    s = Sock.connect_with_timeout H['leader'], 0.2
    if s
      begin 
        Sock.puts_with_timeout s, HeartbeatPing, 0.2
        msg = Sock.gets_with_timeout s, 0.2
      rescue Errno::EPIPE, Errno::ECONNRESET; ensure s.close; end
      result = false if msg == HeartbeatPong
    end
    result
  end

  def choose_n_from_disk id, conn=nil
    disk_n = select_n_where_id_from_disk id, conn
    return unless disk_n
    increment_n disk_n
  end

  def increment_n n
    ind = H['addrs'].index(LocalData['local_addr'])
    raise "#{LocalData['local_addr']} not in #{H['addrs']}" unless ind
    (n.to_i / H['addrs'].size + 1) * H['addrs'].size + ind + 1
  end
end
