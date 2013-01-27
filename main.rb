require 'socket'
require './msg_command'
ADDR = ARGV[0]
PORT = ADDR.match(/:(\d+)$/)[1].to_i

Paxos::H[:addrs] = ['localhost:6660', 'localhost:6661', 'localhost:6662']
Paxos::H[:leader] = H[:addrs][0]

# table schema
# id | promised_n | v

# Client handler and proposer thread
Thread.new do
  Thread.current.abort_on_exception = true
  loop do
    command, client = Paxos::ClientHandlerQueue.pop
    n = 1
    #loop do
    #  # we propose paxos repeated when 
    #  result = Paxos.propose(Paxos.last_index+1, command, n)
    #  break if result.ignored_because_of_gaps? or result.ok?
    #  n = result.n + rand(3*Paxos::H[:addrs].size)
    #  sleep(2*rand)
    #end
    client.puts result.response
    client.close
  end
end
# catchup thread
catchup_thread = Thread.new do
  Thread.current.abort_on_exception = true
  loop do
    end_id = Paxos::CatchupQueue.pop
    (Paxos.smallest_executable_id...end_id).each do |id|
      res = Paxos.propose_phase_1 id
      raise unless res
      App.execute res
      res.save_all_i_n_and_v_to_disk
    end
    Paxos::LocalData[:catching_up] = false
  end
end
# acceptor thread
Thread.new do
  Thread.current.abort_on_exception = true
  catching_up = false
  loop do
    request, client = Paxos::AcceptorQueue.pop
    # Become a non-voting member and start catching up if we're lagging behind
    if Paxos.smallest_executable_id < request.id
      unless Paxos::LocalData[:catching_up]
        Paxos::LocalData[:catching_up] = true
        Paxos::CatchupQueue << request.id
      end
      client.close; next
    end

    last_promise = Paxos.last_promise(request.id)
    case request.type
    when Paxos::Msg::PREPARE
      if last_promise.nil?
        request.promise_to_disk
        client.puts "#{request.id} #{Paxos::Msg::PROMISE}"
      elsif request.n > last_promise.n
        last_promise.save_n_to_disk
        client.puts "#{request.id} #{Paxos::Msg::PROMISE} #{last_promise.n} #{last_promise.v}"
      elsif request.n <= last_promise.n
        client.puts "#{request.id} #{Paxos::Msg::IGNORED} #{last_promise.n}"
      end 
    when Paxos::Msg::ACCEPT
      unless last_promise.n > request.n
        request.save_n_and_v_to_disk
        client.puts "#{request.id} #{Paxos::Msg::ACCEPTED}"
      else
        client.puts "#{request.id} #{Paxos::Msg::IGNORED} #{last_promise.n}"
      end
    end
    client.close
  end
end
# main thread handles socket
server = TCPServer PORT
loop do
  Thread.new(server.accept) do |client|
    msg = client.gets
    if msg[-1] != "\n"
      client.puts "newline at the end needed"; client.close; Thread.exit
    else
      msg = msg[0...-1]
    end

    # try to parse the message into a Paxos message
    paxos_msg = Paxos::Msg.new msg
    if paxos_msg.ok?
      if [Paxos::Msg::PREPARE, Paxos::Msg::ACCEPT].include?(paxos_msg.type)
        Paxos::AcceptorQueue << [paxos_msg, client]
      else # PROMISE, ACCPETED, IGNORED
        Paxos::ProposerQueue << [paxos_msg, client]
      end
    else # if not, try to parse the message into an app command
      command = App::Command.new msg
      if command.err_msg
        client.puts command.err_msg; client.close; Thread.exit
      end
      if ADDR == Paxos::H[:leader]
        Paxos::ClientHandlerQueue << [command, client]
      else
        client.puts "Please contact the leader: #{h[:leader]}"; client.close
      end
    end
  end
end

# = = = = =  =GARGBAGE

Thread.new do
  Thread.current.abort_on_exception = true
  server = TCPServer.new PORT
  loop do
    Thread.new(server.accept) do |client|
      msg = client.gets
      Thread.exit unless msg[-1] == "\n"
      paxos_msg = Paxos::PaxosMsg.new msg
      human_command = Paxos::HumanCommand.new msg
      if paxos_msg.type
        queue << [paxos_msg, client]
      elsif human_command.err_msg.nil?
        queue << [human_command, client]
      end
    end
  end
end



proposer = h[:addrs][0]
become_proposer_margin = rand(6) + 1
loop do
  msg, socket = queue.pop
  if msg.is_a?(HumanCommand)
    comd = msg
    if comd.err_msg
      puts comd.err_msg; next
    end
    if addr == proposer
      propose_paxos(h, comd)
    else
      if become_proposer_margin > 0
        become_proposer_margin -= 1
      else
        proposer = addr; become_proposer_margin = rand(6) + 1
      end
      puts "Please contact proposer: #{proposer}"
    end
  elsif msg.is_a?(PaxosMsg)
    
  end
end



server = TCPServer.new ARGV[0]
loop do
  if proposer
    threads = []; vs = []
    INIT_PORTS.each do |port|
      next if port == ARGV[0]
      threads << Thread.new{
        s = TCPSocket.new 'localhost', port
        s.puts "<#{PREPARE_MSG}>#{n}"
        begin
          type, msg = Timeout::timeout(1){ s.gets.match(/<(\w+)>(.*)/)[1..-1] }
        rescue Timeout::Error; end
        s.close
        if type == PROMISE_MSG
          m = msg.match(/(\d+):(.*)/)
          if m
            vs << m[1..-1]
          end
        end
      }
    end
    threads.each{|t| t.join}
    if vs.size > INIT_N.to_f / 2
      value_of_n = vs.empty? ? our_v : vs.max{|a, b| a[0] <=> b[0]}[1]
      send_accept_requests INIT_PORTS, n, value_of_n
    end

  else
    client = server.accept
    msg = client.read
    type, n = /(\w+): (\d+)/.match(msg)[1..-1]
    case type
    when PREPARE_MSG
      if n > last_n
        client.write "#{PROMISE_MSG}: #{n}"
        last_n = n
      end
    end
    client.close
  end
end
