module Paxos; end
class Paxos::Msg
  PREPARE = 'prepare'; PROMISE = 'promise'; ACCEPT = 'accept'
  ACCEPTED = 'accepted'; IGNORED = 'ignored'
  attr_reader :id, :type, :n, :v, :exec_result
  def self.create s
    msg = new s
    msg.type ? msg : nil
  end
  def initialize s_
    s = s_.to_s
    if m = s.match(/^(\d+) #{PREPARE} (\d+)$/)
      @id = m[1].to_i; @type = PREPARE; @n = m[2].to_i

    elsif m = s.match(/^(\d+) #{PROMISE} (\d+) ([\w\s]+)$/)
      @id = m[1].to_i; @type = PROMISE; @n = m[2].to_i; @v = m[3]
    elsif m = s.match(/^(\d+) #{PROMISE} (\d+)$/)
      @id = m[1].to_i; @type = PROMISE; @n = m[2].to_i
    elsif m = s.match(/^(\d+) #{PROMISE}$/)
      @id = m[1].to_i; @type = PROMISE

    elsif m = s.match(/^(\d+) #{ACCEPT} (\d+) ([\w\s]+)$/)
      @id = m[1].to_i; @type = ACCEPT; @n = m[2].to_i; @v = m[3]
    elsif m = s.match(/^(\d+) #{ACCEPTED} ([\w\s]*)$/)
      @id = m[1].to_i; @type = ACCEPTED; @exec_result = m[2]
    elsif m = s.match(/^(\d+) #{IGNORED} (\d+)$/)
      @id = m[1].to_i; @type = IGNORED; @n = m[2].to_i
    end
  end
end

class Paxos::Disk
  attr_reader :id, :n, :v
  def self.find_by_id id_
    db = Paxos.disk_conn
    r = db.execute("SELECT id, promised_n, v FROM paxos WHERE id=#{id_}")[0]
    r ? new(*r) : nil
  end
  def initialize id_, n_, v_
    @id = id_; @n = n_; @v = v_
  end
  def save_n_v_to_disk
    raise "id and n should not be nil" unless @id and @n
    Paxos.disk_conn.execute("INSERT OR REPLACE INTO paxos(id, promised_n, v) VALUES(#{@id}, #{@n}, '#{@v || 'NULL'}')")
  end
  def save_n_to_disk
    raise "id and n should not be nil" unless @id and @n
    Paxos.disk_conn.execute("INSERT OR REPLACE INTO paxos(id, promised_n, v) VALUES(#{@id}, #{@n}, (SELECT v FROM paxos WHERE id=#{@id}))")
  end
end

class Paxos::FailedProposal
  attr_reader :id, :enemy_n
  def initialize id_, n_
    @id = id_; @enemy_n = n_
  end
end

class Paxos::SuccessfulProposal
  attr_reader :id, :n, :v, :exec_result
  def initialize id_, n_, v_, exec_res
    @id = id_; @n = n_; @v = v_; @exec_result = exec_res
  end
end
