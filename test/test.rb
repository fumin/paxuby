require 'bacon'

require './paxos'
require './client'

MachineAddrs = ['127.0.0.1:6660', '127.0.0.1:6661', '127.0.0.1:6662']

describe 'test paxos' do
  def start_machine addr, pids, opts={}
    Paxos.setup_disk "#{addr}paxos.db" unless opts[:dont_setup_disk]
    pid = Process.fork
    if pid.nil?
      $stdout.reopen("#{addr}.log", 'w')
$stderr.reopen("#{addr}.err.log", 'w')
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
    `rm *paxos.db *.log`
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

  it 'should work' do
    check_success MachineAddrs
  end

  it 'should work when a replica got a network glitch' do
    Process.kill 'HUP', @machine_pids[-1]; Process.wait(@machine_pids[-1])
    check_success MachineAddrs[0...-1]

    # Network glitch resolved
    start_machine MachineAddrs[-1], @machine_pids, dont_setup_disk: true
    client = Paxos::Client.new '127.0.0.1:6660'
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
      ns[0, 6].should.satisfy{ |ns_| ns_.all?{|n| n == 1} }
      ns[6..-1].should.satisfy{|ns_| ns_.all?{|n| n == 0} }
    end
  end

  # kill leader, other replica becomes leader, data consistency
  # shouldn't work when kill 2 machines
end
