require 'yaml'
require './test/util'
require './app/client'

def nil_on_err
  res = nil
  begin
    res = yield
  rescue Exception
  end
  res
end

def data_to_freq data
  data.reduce({}){ |h, e|
    h[e] = 0 unless h[e]
    h[e] = h[e] + 1
    h
  }
end

FileUtils.rm Dir.glob('tmp/*'), force: true
addrs = ['127.0.0.1:6660', '127.0.0.1:6661', '127.0.0.1:6662',
         '127.0.0.1:6663', '127.0.0.1:6664']
original_config = File.read('./config.yaml')
File.write('./config.yaml',
           YAML.dump({'addrs' => addrs, 'heartbeat_timeout' => 1000,
                      'perftools' => true}))
pids = []
addrs.each do |addr|
  start_machine addr, pids
end

# =======================================
# Profile for normal mode
# =======================================
puts "Profile normal situations (though replicas might die):"
time_spents = []
long_str = 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz'
client = App::Client.new '127.0.0.1:6660', addrs
N = 1200
N.times do |i|
  time_spents << profile{ until nil_on_err{client.get(i)} == 'nil'
                   sleep(0.05); client.find_leader if rand(3) == 0; end}
  time_spents << profile{ until nil_on_err{client.set(i, long_str)} == long_str
                   sleep(0.05); client.find_leader if rand(3) == 0; end}
  time_spents << profile{ until nil_on_err{client.get(i)} == long_str
                   sleep(0.05); client.find_leader if rand(3) == 0; end}
  if (i-N/4) % (N/2) == 0
    pid_index = 1 + (i-N/4)/(N/2)
    stop_machine pids[pid_index]
  end
  print '.' if (N % (N/100)) == 0
end
print "\n"
stats = data_to_freq(time_spents)
puts "time spent (milliseconds), frequency"
stats.to_a.sort_by{|e| e[0]}.each{|e| puts "#{e[0]}, #{e[1]}"}

# cleanup
['127.0.0.1:6660', '127.0.0.1:6663', '127.0.0.1:6664'].each do |addr|
  s = TCPSocket.new '127.0.0.1', addr[-4..-1]
  s.puts 'exit'
  s.close
end
File.write('./config.yaml', original_config)
