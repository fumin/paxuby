require 'socket'
require 'timeout'
require 'sqlite3'
module Paxos
  H = {}
  AcceptorQueue = Queue.new
  ClientHandlerQueue = Queue.new
  ProposerQueue = Queue.new
  CatchupQueue = Queue.new
  LocalData = {}
  NoOp = 'NoOp'
  module_function
  def smallest_executable_id
    r = disk_conn.execute(
          'SELECT id FROM paxos WHERE v IS NOT NULL ORDER BY id DESC LIMIT 1')[0]
    (r || [0])[0] + 1
  end
  def propose id, command
    n = (disk_conn.execute("SELECT n FROM paxos WHERE id = #{id}")[0] || [1])[0]
    loop do
      promise_msgs, ignore_msgs = send_prepare_msgs id, n

      # send accept message if a majority of replicas promised us
      if promise_msgs.size > H[:addrs].size.to_f / 2
        promise_msgs_with_n = promise_msgs.select{|m| m.n}
        v = if promise_msgs_with_n.size > 0
              promise_msgs_with_n.max{|a, b| a.n <=> b.n}.v || command
            else
              command
            end
        accepted_msgs, ignore_msgs = send_accept_msgs id, n, v

        # consenses reached if a majority of replicas accepted
        return v if accepted_msgs.size > H[:addrs].size.to_f / 2
      end

      # sleep for a while and try with a larger n again
      sleep(rand(H[:addrs].size))
      n = ignore_msgs.max{|a, b| a.n <=> b.n} + rand(H[:addrs].size) + 1
    end
  end
  def send_prepare_msgs id, n
    msg_to_be_sent = "#{id} #{Msg::PREPARE} #{n}"
    responses = send_msg_to_acceptors_and_collect_reponses msg_to_be_sent
    promise_msgs = responses.select{|r| r.type == Msg::PROMISE}
    ignore_msgs = response.select{|r| r.type == Msg::IGNORED}
    [promise_msgs, ignore_msgs]
  end
  def send_accept_msgs id, n, v
    msg_to_be_sent = "#{id} #{Msg::ACCEPT} #{n} #{v}"
    responses = send_msg_to_acceptors_and_collect_reponses msg_to_be_sent
    accepted_msgs = responses.select{|r| r.type == Msg::ACCEPTED}
    ignore_msgs = response.select{|r| r.type == Msg::IGNORED}
    [accepted_msgs, ignore_msgs]
  end
  def send_msg_to_acceptors_and_collect_reponses msg_to_be_sent, timeout=3
    responses = []; threads;
    H[:addrs].each do |addr|
      next unless (s = tcp_socket(addr))
      threads << Thread.new(s) do |acceptor|
        str_msg = nil
        begin
          Timeout::timeout(timeout) {
            acceptor.puts msg_to_be_sent
            str_ = acceptor.gets
            str_msg = (str_[-1] == "\n" ? str_[0...-1] : "") }
        rescue Timeout::Error, Errno::ECONNRESET;
        ensure acceptor.close; end
        msg = Paxos::Msg.new str_msg
        responses << msg if msg.type
      end
    end
    threads.each{|t| t.join}
    responses
  end
  def cannot_be_executed id

  end
  def disk_conn; SQLite3::Database.new "paxos.db"; end
  def setup_disk
    disk_conn.execute <<-SQL
      CREATE TABLE paxos(
        id INTEGER PRIMARY KEY, promised_n INTEGER NOT NULL, v TEXT);
      CREATE TRIGGER update_paxos BEFORE UPDATE OF promised_n ON paxos BEGIN
        SELECT CASE WHEN NEW.promised_n <= OLD.promised_n THEN
          RAISE(ABORT, 'promised_n should increase upon UPDATEs') END;
        END;
    SQL
  end
  def tcp_socket addr
    m = addr.match(/([\w\d\.]+):(\d+)/)
    return nil unless m.size == 3
    Timeout::timeout(3) do
      TCPSocket.new m[1], m[2].to_i
    end
  rescue Timeout::Error, Errno::ECONNREFUSED
  end
end
