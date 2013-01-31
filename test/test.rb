require 'bacon'

require './paxos'
require './client'

MachineAddrs = ['127.0.0.1:6660', '127.0.0.1:6661', '127.0.0.1:6662']

describe 'test paxos' do
  before do
    `rm *paxos.db *.log`
    @machine_pids = []
    MachineAddrs.each do |addr|
      Paxos.setup_disk "#{addr}paxos.db"
      pid = Process.fork
      if pid.nil?
        $stdout.reopen("#{addr}.log", 'w')
        exec("ruby main.rb #{addr}")
      else
        @machine_pids << pid
      end
    end

    # ensure that all machines are able to serve requests
    MachineAddrs.each do |addr|
      loop do
        c = Paxos::Client.new addr
        s = nil
        begin; s = c.puts_and_gets('')
        rescue Errno::EPIPE, Errno::ECONNREFUSED; end
        break if s
      end
    end
  end

  after do
    @machine_pids.each do |pid|
      begin; Process.kill "HUP", pid
      rescue Errno::ESRCH; end
    end
    Process.waitall
  end

  def check_success addrs
    client = Paxos::Client.new '127.0.0.1:6661'
    client.get(:key).should.equal "Please contact the leader: 127.0.0.1:6660"
    client = Paxos::Client.new '127.0.0.1:6660'
    client.get(:key).should.equal 'nil'
    client.set(:key, :value).should.equal 'value'
    client.get(:key).should.equal 'value'
    client.del(:key).should.equal 'value'
    client.get(:key).should.equal 'nil'
    db_res = [[1, 0, 'GET key'], [2, 0, 'SET key value'], [3, 0, 'GET key'],
              [4, 0, 'DEL key'], [5, 0, 'GET key']]
    addrs.each do |addr|
      sql = 'SELECT * FROM paxos'
      Paxos.disk_conn("#{addr}paxos.db").execute(sql).should.equal db_res
    end
  end

  it 'should succeed' do
    check_success MachineAddrs
  end

  it 'should success when only two' do
    Process.kill 'HUP', @machine_pids[-1]; Process.wait(@machine_pids[-1])
    check_success MachineAddrs[0...-1]
# test catchup, data consistency, no change in leader even after 2 secs
  end

  # kill leader, other replica becomes leader, data consistency
  # shouldn't work when kill 2 machines
end
