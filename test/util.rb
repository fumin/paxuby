require 'fileutils'
require './client'

def start_machine addr, pids, opts={}
  pid = Process.fork
  if pid.nil?
    $stdout.reopen("#{addr}.log", 'w')
    close_all_fds
    unless opts[:dont_setup_disk]
      FileUtils.rm ["#{addr}paxos.db"], force: true
      Paxos.setup_disk "#{addr}paxos.db"
    end
    exec("ruby main.rb #{addr}")
  else
    loop do # ensure machine is able to serve requests
      c = Paxos::Client.new addr
      s = nil; begin; s = c.puts_and_gets('gibberish')
      rescue Exception; end
      break if s =~ /^USAGE:/
      sleep(0.01)
    end
    pids << pid
  end
end

def stop_machine pid
  begin
    Process.kill 'HUP', pid
    Process.wait(pid)
  rescue Errno::ESRCH; end
end

def close_all_fds
  3.upto(1023) do |fd|
    begin
      io.close if io = IO::new(fd)
    rescue
    end
  end
end
