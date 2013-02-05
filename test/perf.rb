require './test/util'
require './app/client'

def profile
  start_time = Time.now.to_f
  yield
  ((Time.now.to_f - start_time)*1000).to_i
end

def data_to_freq data
  data.reduce({}){ |h, e|
    h[e] = 0 unless h[e]
    h[e] = h[e] + 1
    h
  }
end

pids = []
MachineAddrs = ['127.0.0.1:6660', '127.0.0.1:6661', '127.0.0.1:6662']
MachineAddrs.each do |addr|
  start_machine addr, pids
end

# increase the cluster size to 9
addrs = ['127.0.0.1:6660', '127.0.0.1:6661', '127.0.0.1:6662',
         '127.0.0.1:6663', '127.0.0.1:6664', '127.0.0.1:6665',
         '127.0.0.1:6666', '127.0.0.1:6667', '127.0.0.1:6668']
addrs_str = JSON.dump addrs
client = App::Client.new '127.0.0.1:6660', addrs
client.puts_and_gets "#{Paxos::App::Command::SET} addrs #{addrs_str}"
addrs[3..-1].each do |addr|
  start_machine addr, pids
end
client.puts_and_gets "#{Paxos::App::Command::SET} addrs #{addrs_str}"

# =======================================
# Profile for normal mode
# =======================================
puts "Profile normal situations (though replicas might die):"
time_spents = []
long_str = 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz'
333.times do |i|
  time_spents << profile{ msg = client.get(i)
                          puts "#{msg} #{i} get" if msg != 'nil' }
  time_spents << profile{ msg = client.set(i, long_str)
                          puts "#{msg} #{i} set" if msg != long_str }
  time_spents << profile{ msg = client.get(i)
                          puts "#{msg} #{i} del" if msg != long_str}
  # kill some replicas
  if i % 100 == 0
    pid_index = 1 + i/100
    stop_machine pids[pid_index]
  end
end
puts data_to_freq(time_spents)


# =====================================
# profile for how long it takes for a replica to be promoted to leader
# =====================================
puts "Profile dead leader situations (under default client algorithm):"
# configure cluster for the remaining 5 machines
remaining_addrs = addrs[5..-1] + [addrs[0]]
addrs_str = JSON.dump remaining_addrs
client = App::Client.new '127.0.0.1:6660', remaining_addrs
client.puts_and_gets "#{Paxos::App::Command::SET} addrs #{addrs_str}"
# start profiling
ts = []
pids = pids[5..-1] + [pids[0]]
10.times do |i|
  stop_machine pids[-1]
  ts << profile{client.find_leader}
  print '.'
  start_machine MachineAddrs[0], pids
  client.puts_and_gets "#{Paxos::App::Command::SET} leader #{MachineAddrs[0]}"
end
print "\n"
puts ts

# clean up
pids.each do |pid|
  stop_machine pid
end
Process.waitall
