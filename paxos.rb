require 'fcntl'
require 'thread'
require 'socket'
require 'timeout'

require 'sqlite3'

require './entities'
require './paxos_app'

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
    n = opts[:n] ||
          select_n_where_id_from_disk(id, opts[:disk_conn]) || SmallestProposeN
    raise "#{n} < SmallestProposeN" unless n >= SmallestProposeN

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
      next unless (s = tcp_socket(addr))
      threads << Thread.new(s) do |acceptor|
        str_msg = nil
        begin
          acceptor.send(msg_to_be_sent+"\n", 0)
          rs, ws, = IO.select([acceptor], [], [], 0.1)
          raise Timeout::Error unless rs
          str_ = acceptor.recv(1024)
          str_msg = (str_[-1] == "\n" ? str_[0...-1] : "")
        rescue Timeout::Error, Errno::ECONNRESET => e
        ensure acceptor.close; end
        msg = Paxos::Msg.new str_msg
        responses << msg if msg.type
      end
    end
    threads.each{|t| t.join}
    responses
  end
  def disk_conn; SQLite3::Database.new "paxos.db"; end
  def setup_disk
    conn = disk_conn
    conn.execute <<-SQL
      CREATE TABLE paxos(
        id INTEGER PRIMARY KEY,
        promised_n INTEGER NOT NULL CHECK(promised_n > -1), v TEXT);
    SQL
    conn.execute <<-SQL
      CREATE TRIGGER update_paxos BEFORE UPDATE OF promised_n ON paxos BEGIN
        SELECT CASE WHEN NEW.promised_n <= OLD.promised_n THEN
          RAISE(ABORT, 'promised_n should increase upon UPDATEs') END;
        END;
    SQL
  end
  def select_n_where_id_from_disk id, conn=nil
    sql = "SELECT promised_n FROM paxos WHERE id = #{id}"
    row = (conn || disk_conn).execute(sql)[0]
    return nil unless row
    row[0]
  end
  def smallest_executable_id
    r = disk_conn.execute(
          'SELECT id FROM paxos WHERE v IS NOT NULL ORDER BY id DESC LIMIT 1')[0]
    (r ? r[0] : 0) + 1
  end
  def tcp_socket addr_, timeout=0.2
    addr = if Paxos::LocalData['local_addr'] == addr_
             "127.0.0.1:#{Paxos::LocalData['local_port']}"
           else
             addr_
           end
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
  def increment_n n
    ind = H['addrs'].index(LocalData['local_addr'])
    raise "#{LocalData['local_addr']} not in #{H['addrs']}" unless ind
    (n.to_i / H['addrs'].size + 1) * H['addrs'].size + ind + 1
  end
end
