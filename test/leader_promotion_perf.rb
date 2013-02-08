require 'yaml'

require './test/util'
require './app/client'

puts "Profile dead leader situations (under default client algorithm):"
FileUtils.rm Dir.glob('tmp/*'), force: true
MachineAddrs = ['127.0.0.1:6660', '127.0.0.1:6661', '127.0.0.1:6662',
                '127.0.0.1:6663', '127.0.0.1:6664']
original_config = File.read('./config.yaml')
File.write('./config.yaml',
           YAML.dump({'addrs' => MachineAddrs, 'heartbeat_timeout' => 0.75,
                      'perftools' => false}))
pids = []
MachineAddrs.each do |addr|
  start_machine addr, pids
end
# move pid of 127.0.0.1:6660 to the tail to ease stopping and starting it
pids = pids[1..-1] + [pids[0]]

# start profiling
client = App::Client.new '127.0.0.1:6660', MachineAddrs
ts = []
10.times do |i|
  stop_machine pids[-1]
  ts << profile{client.find_leader}
  print '.'
  start_machine MachineAddrs[0], pids
  client.puts_and_gets "#{Paxos::App::Command::SET} leader #{MachineAddrs[0]}"
end
print "\n"
puts ts

pids.each{|pid| stop_machine pid}
File.write('./config.yaml', original_config)
