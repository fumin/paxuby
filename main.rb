require './paxos'
require './app'

`rm paxos.db`
Paxos.setup_disk

Paxos::LocalData[:local_addr] = ARGV[0]
Paxos::LocalData['local_port'] =
  Paxos::LocalData['local_addr'].match(/:(\d+)$/)[1].to_i

#Paxos::H[:addrs] = ['localhost:6660', 'localhost:6661', 'localhost:6662']
Paxos::H['addrs'] = ['localhost:6660']
Paxos::H['leader'] = Paxos::H['addrs'][0]

puts "ADDR: #{Paxos::LocalData['local_addr']}, PORT: #{Paxos::LocalData['local_port']}, H: #{Paxos::H}"

# Client handler thread
Thread.new do
  Thread.current.abort_on_exception = true
  local_addr = Paxos::LocalData['local_addr']
  id = Paxos.smallest_executable_id
  n = Paxos::SmallestProposeN - 1
  loop do
    command, client = Paxos::ClientHandlerQueue.pop

    if we are woke up to set leader to us
      Paxos.propose, Paxos.smallest_executable_id, 'set leader to_us', timeout:
    else
      multi_paxos
    end

    id = if command == "#{Paxos::App::Command::SET} leader #{local_addr}"
           # Here, our ping listener told us to wake up, thus we need refresh id
           id = Paxos.smallest_executable_id
         else
           id + 1
         end

    # Run multi-paxos (omit prepare messages). In this case 'n' must be
    # chosen to be smaller
    acc_msgs, ign_msgs = send_accept_msgs id, n, command
    if acc_msgs.size > (Paxos::H['addrs'].size.to_f / 2)
      client.puts acc_msgs[0].exec_result
    else
      # Someone succeeded in becoming leader in advance of us, respect her
      client.puts "Please contact the leader: #{Paxos::H['leader']}"
      sleep 0.2
    end
    client.close
  end
end
# catchup thread
Thread.new do
  Thread.current.abort_on_exception = true
  local_addr = Paxos::LocalData['local_addr']
  loop do
    end_id = Paxos::CatchupQueue.pop
    puts "!#{end_id}CATCHING UP#{Paxos.smallest_executable_id}!!!!!!"
    (Paxos.smallest_executable_id...end_id).each do |id|
      # propose until our acceptor has accepted for this id
      loop do
        p = Paxos.propose id, Paxos::App::Command::NoOp
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
  Paxos::LocalData['catching_up'] = false
  loop do
    request, client = Paxos::AcceptorQueue.pop
    # Become a non-voting member and start catching up if we're lagging behind
    if Paxos.smallest_executable_id < request.id
      unless Paxos::LocalData['catching_up']
        Paxos::LocalData['catching_up'] = true
        Paxos::CatchupQueue << request.id
      end
      client.close; next
    end

    last_promise = Paxos::Disk.find_by_id(request.id)
    case request.type
    when Paxos::Msg::PREPARE
      if last_promise.nil? or request.n > last_promise.n
        Paxos::Disk.new(request.id, request.n, request.v).save_n_to_disk
        response = "#{request.id} #{Paxos::Msg::PROMISE}"
        if last_promise
          response << " #{last_promise.n}"
          response << " #{last_promise.v}" if last_promise.v
        end
        client.puts response
      elsif request.n <= last_promise.n
        client.puts "#{request.id} #{Paxos::Msg::IGNORED} #{last_promise.n}"
      end 
    when Paxos::Msg::ACCEPT
      unless (last_promise and last_promise.n > request.n)
        if last_promise and request.v == last_promise.v
          client.puts "#{request.id} #{Paxos::Msg::ACCEPTED} #{local_addr} "
        else
          exec_res = if (paxos_command = Paxos::App::Command.create(request.v))
                       Paxos::App.execute paxos_command
                     else
                       App.execute request.v
                     end
          if exec_res.is_successful
            Paxos::Disk.new(request.id, request.n, request.v).save_n_v_to_disk
            client.puts "#{request.id} #{Paxos::Msg::ACCEPTED} #{local_addr} #{exec_res.value}"
          else
            client.puts "#{request.id} execution_failed"
          end
        end
      else
        client.puts "#{request.id} #{Paxos::Msg::IGNORED} #{last_promise.n}"
      end
    end
    client.close
  end
end
# main thread handles socket
server = TCPServer.new Paxos::LocalData['local_port']
loop do
  Thread.new(server.accept) do |client|
    Thread.current.abort_on_exception = true
    str_msg = client.gets
    if str_msg[-1] != "\n"
      client.puts "newline at the end needed"; client.close; Thread.exit
    else
      str_msg = str_msg[0...-1]
    end

    if (paxos_msg = Paxos::Msg.create(str_msg))
      Paxos::AcceptorQueue << [paxos_msg, client]
    else
      command = ::App::Command.new(str_msg)
      if command.err_msg
        client.puts command.err_msg; client.close
      end
      if Paxos::LocalData['local_addr'] == Paxos::H['leader']
        Paxos::ClientHandlerQueue << [str_msg, client]
      else
        client.puts "Please contact the leader: #{Paxos::H['leader']}"
        client.close
      end
    end

  end # Thread.new do
end
