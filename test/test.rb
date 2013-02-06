require 'bacon'

require './paxos'
require './test/util'
require './app/client'

MachineAddrs = ['127.0.0.1:6660', '127.0.0.1:6661', '127.0.0.1:6662']

describe 'test paxos' do
  before do
    FileUtils.rm Dir.glob('*.log'), force: true
    @machine_pids = []
    MachineAddrs.each do |addr|
      start_machine addr, @machine_pids
    end
  end

  after do
    close_all_fds
    @machine_pids.each do |pid|
      stop_machine pid
    end
    Process.waitall
  end

  def check_success addrs
    client = App::Client.new '127.0.0.1:6661'
    redirect_str = "Please contact the leader: 127.0.0.1:6660"
    client.get(:key, false).should.equal redirect_str
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
    5.times do |i|
      instructions << [6+i, "GET key#{i}"]
      client.get("key#{i}").should.equal 'nil'
      sleep(0.1) if i == 0
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

    client = App::Client.new '127.0.0.1:6661', MachineAddrs
    redirect_str = "Please contact the leader: 127.0.0.1:6660"
    client.get(:key, false).should.equal redirect_str

    # Upon leader's death, some replica should promote itself to be leader
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
      # the promised_n's till the last 9 paxos should be larger than 0 because
      # the newly started MachineAddrs[0] needs to catch up with others
      log[0...-9].should.all?{|row| row[1] > 0}

      # logs should be as expected
      first_3_commands = log[0, 3].map{|row| row[2]}
      first_3_commands.should.equal ['GET key', 'SET key value', 'GET key']
      last_14_commands = log[-14..-1].map{|row| row[2]}
      c14 = ['PAXOS_GET leader', 'GET key', 'SET key another_value', 'GET key',
             'DEL key']
      8.times{c14 << 'GET key'}
      c14 << 'PAXOS_GET leader'
      last_14_commands.should.equal c14

      # consistency of logs of every machine
      log.should.equal logs[0]
    end
  end

  it 'should pause when more than half of the machines are down' do
    client = App::Client.new '127.0.0.1:6660'
    client.get(:key).should.equal 'nil'
    client.set(:key, :value).should.equal 'value'
    client.get(:key).should.equal 'value'
    Process.kill 'HUP', @machine_pids[1]; Process.wait(@machine_pids[1])
    Process.kill 'HUP', @machine_pids[2]; Process.wait(@machine_pids[2])

    redirect_str = "Please contact the leader: 127.0.0.1:6660"
    client.get(:key, false).should.equal redirect_str

    start_machine MachineAddrs[1], @machine_pids
    loop do
      break if 'value' == client.set(:keyy, :value)
      sleep(0.2*rand)
    end
    client.get(:keyy).should.equal 'value'

    logs = []
    MachineAddrs[0..1].each do |addr|
      logs << Paxos.disk_conn("#{addr}paxos.db").execute("SELECT * FROM paxos")
    end
    logs.each do |log|
      log.should.equal logs[0]
    end
  end

  it 'should be able to change cluster size' do
    client = App::Client.new '127.0.0.1:6660'
    new_addrs = ['127.0.0.1:6660', '127.0.0.1:6661', '127.0.0.1:6662',
                 '127.0.0.1:6663', '127.0.0.1:6664', '127.0.0.1:6665',
                 '127.0.0.1:6666', '127.0.0.1:6667', '127.0.0.1:6668']
    addrs_str = JSON.dump new_addrs
    client.puts_and_gets "#{Paxos::App::Command::SET} addrs #{addrs_str}"

    # We should be redirected since we have only 4 over 9 machines
    redirect_str = "Please contact the leader: 127.0.0.1:6660"
    start_machine '127.0.0.1:6663', @machine_pids
    5.times{ client.get(:key, false) }
    client.get(:key, false).should.equal redirect_str

    # 5 over 9 machines is sufficient
    start_machine '127.0.0.1:6664', @machine_pids
    5.times{ client.get(:key, false) }
    client.get(:key).should.equal 'nil'
    client.set(:key, :value).should.equal 'value'
    client.get(:key).should.equal 'value'

    # back to normal configuration
    addrs_str = JSON.dump MachineAddrs
    client.puts_and_gets "#{Paxos::App::Command::SET} addrs #{addrs_str}"
    stop_machine @machine_pids[-2]; stop_machine @machine_pids[-1]
    client.del(:key, :value).should.equal 'value'
    client.get(:key).should.equal 'nil'
  end
end
