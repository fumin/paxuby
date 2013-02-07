require 'yaml'

require './paxos'
require './app/app'

CONFIG = YAML.load(File.read('./config.yaml'))
Paxos::LocalData['local_addr'] = ARGV[0]
Paxos::LocalData['local_port'] =
  Paxos::LocalData['local_addr'].match(/:(\d+)$/)[1].to_i

Paxos::H['addrs'] = CONFIG['addrs']
Paxos::H['leader'] = Paxos::H['addrs'][0]

puts "pid: #{Process.pid}, ADDR: #{Paxos::LocalData['local_addr']}, PORT: #{Paxos::LocalData['local_port']}, H: #{Paxos::H}"

# heartbeat listener thread
Thread.new do
  Thread.current.abort_on_exception = true
  local_addr = Paxos::LocalData['local_addr']
  loop do
    sleep(Paxos::HeartbeatTimeout + 0.5*rand)
    next if local_addr == Paxos::H['leader']

    if Paxos.should_become_leader?
      puts "TRYTOBELEADER!!!!!!!!! #{Process.pid} #{Time.now}"
      command = "#{Paxos::App::Command::SET} leader #{local_addr}"
      p = Paxos.propose Paxos.smallest_executable_id, command, timeout: 0.3
      if p.is_a?(Paxos::SuccessfulProposal) and local_addr == Paxos::H['leader']
        Paxos::ClientHandlerQueue << Paxos::ClientHandlerRefreshId
        puts "WE BECAME THE LEADER!!!!! #{Process.pid} #{Time.now}"
      end
    end
  end
end
# Client handler thread
Thread.new do
  Thread.current.abort_on_exception = true
  local_addr = Paxos::LocalData['local_addr']
  db = Paxos.disk_conn
  id = Paxos.smallest_executable_id
  n = Paxos::SmallestProposeN - 1
  loop do
    command, client = Paxos::ClientHandlerQueue.pop
    if command == Paxos::ClientHandlerRefreshId
      id = Paxos.smallest_executable_id(db); next
    end
    unless local_addr == Paxos::H['leader']
      client.puts "Please contact the leader: #{Paxos::H['leader']}"
      client.close; next
    end

    # Run multi-paxos (omit prepare messages).
    # We always chose n as `Paxos::SmallestProposeN - 1` so that it is
    # impossible for us to overwrite an established consensus for this id,
    # and that there can only be three possible outcomes:
    # 1. A majority of replicas successfully executes our command
    # 2. We got ignored by some replicas because they held promises
    #    with larger n's
    # 3. Network glitch
    acc_msgs, ign_msgs = Paxos.send_accept_msgs id, n, command
    if acc_msgs.size > (Paxos::H['addrs'].size.to_f / 2)
      client.puts acc_msgs[0].exec_result
      id += 1
    elsif ign_msgs.size > 0
      client.puts "Please contact the leader: #{Paxos::H['leader']}"
      id += 1
    else
      client.puts "Please contact the leader: #{Paxos::H['leader']}"
    end
    client.close
  end
end
# catchup thread
Thread.new do
  Thread.current.abort_on_exception = true
  local_addr = Paxos::LocalData['local_addr']
  db = Paxos.disk_conn
  loop do
    end_id = Paxos::CatchupQueue.pop
    puts "!#{end_id}CATCHING UP#{Paxos.smallest_executable_id}!!!!!!"
    (Paxos.smallest_executable_id..end_id).each do |id|
      # propose until our acceptor has accepted for this id
      loop do
        p = Paxos.propose id, Paxos::App::Command::NoOp,
                          disk_conn: db, wait_for_all_acceptors: true
        break if p.accepted_msgs.find{|m| m.addr == local_addr }
      end
    end
    Paxos::LocalData['catching_up'] = false
  end
end
# acceptor thread
Thread.new do
  Thread.current.abort_on_exception = true
  local_addr = Paxos::LocalData['local_addr']
  db = Paxos.disk_conn
  Paxos::LocalData['catching_up'] = false
  loop do
    request, client = Paxos::AcceptorQueue.pop
    # Become a non-voting member and start catching up if we're lagging behind
    if Paxos.smallest_executable_id(db) < request.id
      unless Paxos::LocalData['catching_up']
        Paxos::LocalData['catching_up'] = true
        Paxos::CatchupQueue << request.id
      end
      client.close; next
    end

    last_promise = Paxos::Disk.find_by_id(request.id, db)
    case request.type
    when Paxos::Msg::PREPARE
      if last_promise.nil? or request.n > last_promise.n
        Paxos::Disk.new(request.id, request.n, request.v).save_n_to_disk(db)
        response = "#{request.id} #{Paxos::Msg::PROMISE}"
        if last_promise and last_promise.v
          response << " #{last_promise.n} #{last_promise.v}"
        end
      elsif request.n <= last_promise.n
        response = "#{request.id} #{Paxos::Msg::IGNORED} #{last_promise.n}"
      end 
    when Paxos::Msg::ACCEPT
      unless (last_promise and last_promise.n > request.n)
        if last_promise and request.v == last_promise.v
          response = "#{request.id} #{Paxos::Msg::ACCEPTED} #{local_addr} "
        else
          exec_res = if (paxos_command = Paxos::App::Command.create(request.v))
                       Paxos::App.execute paxos_command
                     else
                       App.execute request.v
                     end
          if exec_res.is_successful
            n = [request.n, Paxos::SmallestProposeN].max
            Paxos::Disk.new(request.id, n, request.v).save_n_v_to_disk(db)
            response = "#{request.id} #{Paxos::Msg::ACCEPTED} #{local_addr} #{exec_res.value}"
          else
            response = "#{request.id} execution_failed"
          end
        end
      else
        response = "#{request.id} #{Paxos::Msg::IGNORED} #{last_promise.n}"
      end
    end
    begin; Paxos::Sock.puts_with_timeout client, response, 0.1
    rescue Errno::EPIPE; ensure client.close; end
  end
end
# main thread handles socket
server = TCPServer.new Paxos::LocalData['local_port']
loop do
  Thread.new(server.accept) do |client|
    Thread.current.abort_on_exception = true
    str_msg = client.gets
    if str_msg.nil?
      client.close; Thread.exit
    elsif str_msg[-1] != "\n"
      client.puts "newline at the end needed"; client.close; Thread.exit
    else
      str_msg = str_msg[0...-1]
    end

    if str_msg == Paxos::HeartbeatPing
      Paxos::Sock.puts_and_close client, Paxos::HeartbeatPong
      Thread.exit
    end

    if (paxos_msg = Paxos::Msg.create(str_msg))
      Paxos::AcceptorQueue << [paxos_msg, client]
    elsif (paxos_command = Paxos::App::Command.create(str_msg))
      Paxos::ClientHandlerQueue << [str_msg, client]
    else
      command = ::App::Command.new(str_msg)
      if command.err_msg
        Paxos::Sock.puts_and_close client, command.err_msg
        Thread.exit
      end
      Paxos::ClientHandlerQueue << [str_msg, client]
    end

  end # Thread.new do
end
