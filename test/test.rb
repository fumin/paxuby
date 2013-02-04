require 'fileutils'
require 'bacon'

require './paxos'
require './app/client'

MachineAddrs = ['127.0.0.1:6660', '127.0.0.1:6661', '127.0.0.1:6662']

describe 'test paxos' do
  def start_machine addr, pids, opts={}
    unless opts[:dont_setup_disk]
      FileUtils.rm ["#{addr}paxos.db"], force: true
      Paxos.setup_disk "#{addr}paxos.db"
    end
    ObjectSpace.each_object(File){ |f|
      f.close unless f === filehandle rescue nil}
    pid = Process.fork
    if pid.nil?
      $stdout.reopen("#{addr}.log", 'w')
      exec("ruby main.rb #{addr}")
    else
      loop do # ensure machine is able to serve requests
        c = Paxos::Client.new addr
        s = nil; begin; s = c.puts_and_gets('')
        rescue Errno::EPIPE, Errno::ECONNREFUSED; end
        break if s
      end
      pids << pid
    end
  end

  before do
    FileUtils.rm Dir.glob('*.log'), force: true
    @machine_pids = []
    MachineAddrs.each do |addr|
      start_machine addr, @machine_pids
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
    client = App::Client.new '127.0.0.1:6661'
    client.get(:key).should.equal "Please contact the leader: 127.0.0.1:6660"
    client = App::Client.new '127.0.0.1:6660'
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

  it 'should work' do
    check_success MachineAddrs
  end

  it 'should work when a replica got a network glitch' do
    Process.kill 'HUP', @machine_pids[-1]; Process.wait(@machine_pids[-1])
    check_success MachineAddrs[0...-1]

    # Network glitch resolved
    start_machine MachineAddrs[-1], @machine_pids, dont_setup_disk: true
    client = App::Client.new '127.0.0.1:6660'
    instructions = []
    8.times do |i|
      instructions << [6+i, "GET key#{i}"]
      client.get("key#{i}").should.equal 'nil'
    end
    db_res = [[1, 'GET key'], [2, 'SET key value'], [3, 'GET key'],
              [4, 'DEL key'], [5, 'GET key']] + instructions
    nss = []
    MachineAddrs.each do |addr|
      sql = 'SELECT id, v FROM paxos'
      Paxos.disk_conn("#{addr}paxos.db").execute(sql).should.equal db_res
      nsql = 'SELECT promised_n FROM paxos'
      nss << Paxos.disk_conn("#{addr}paxos.db").execute(nsql).map{|r| r[0]}
    end
    nss.each do |ns|
      ns.should.equal nss[0]
      ns[0, 6].should.all?{|n| n == 1}
      ns[6..-1].should.all?{|n| n == 0}
    end
  end

  it 'should work when leader is dead' do
    client = App::Client.new '127.0.0.1:6660'
    client.get(:key).should.equal 'nil'
    client.set(:key, :value).should.equal 'value'
    client.get(:key).should.equal 'value'
    Process.kill 'HUP', @machine_pids[0]; Process.wait(@machine_pids[0])

    client = App::Client.new '127.0.0.1:6661'
    client.get(:key).should.equal "Please contact the leader: 127.0.0.1:6660"
    sleep(Paxos::HeartbeatTimeout * 2)

    # After we waited for around Paxos::HeartbeatTimeout seconds since the
    # original leader has died, some replica should promote itself to be leader
    leader_addr = client.find_leader
    ['127.0.0.1:6661', '127.0.0.1:6662'].should.include?(leader_addr)
    client = App::Client.new leader_addr
    client.get(:key).should.equal 'value'
    client.set(:key, :another_value).should.equal 'another_value'
    client.get(:key).should.equal 'another_value'

    # The original leader has been repaired, and should catchup with others.
    # Besides, it should be simply a follower.
    start_machine MachineAddrs[0], @machine_pids
    client.del(:key).should.equal 'another_value'
    8.times do |i|
      client.get(:key).should.equal 'nil'; sleep(rand*0.2)
    end
    client.find_leader.should.equal leader_addr

    logs = []
    MachineAddrs.each do |addr|
      logs << Paxos.disk_conn("#{addr}paxos.db").execute("SELECT * FROM paxos")
    end
    logs.each do |log|
      # the promised_n's of the last 9 paxos should equal to 0
      log[-9..-1].should.all?{|row| row[1] == 0}
      log[0...-9].should.all?{|row| row[1] > 0}

      # consistency of logs of every machine
      log.should.equal logs[0]
    end
  end
end
