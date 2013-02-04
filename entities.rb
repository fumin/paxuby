module Paxos; end
class Paxos::Msg
  PREPARE = 'prepare'; PROMISE = 'promise'; ACCEPT = 'accept'
  ACCEPTED = 'accepted'; IGNORED = 'ignored'
  attr_reader :id, :type, :n, :v, :exec_result, :addr
  def self.create s
    msg = new s
    msg.type ? msg : nil
  end
  def initialize s_
    s = s_.to_s
    if m = s.match(/^(\d+) #{PREPARE} (\d+)$/)
      @id = m[1].to_i; @type = PREPARE; @n = m[2].to_i

    elsif m = s.match(/^(\d+) #{PROMISE} (\d+) (.+)$/)
      @id = m[1].to_i; @type = PROMISE; @n = m[2].to_i; @v = m[3]
    elsif m = s.match(/^(\d+) #{PROMISE}$/)
      @id = m[1].to_i; @type = PROMISE

    elsif m = s.match(/^(\d+) #{ACCEPT} (\d+) (.+)$/)
      @id = m[1].to_i; @type = ACCEPT; @n = m[2].to_i; @v = m[3]
    elsif m = s.match(/^(\d+) #{ACCEPTED} ([\w\.:]+) (.*)$/)
      @id = m[1].to_i; @type = ACCEPTED; @addr = m[2]; @exec_result = m[3]
    elsif m = s.match(/^(\d+) #{IGNORED} (\d+)$/)
      @id = m[1].to_i; @type = IGNORED; @n = m[2].to_i
    end
  end
end

class Paxos::FailedProposal
  attr_reader :id, :enemy_n
  def initialize id_, n_
    @id = id_; @enemy_n = n_
  end
end

class Paxos::SuccessfulProposal
  attr_reader :id, :n, :v, :accepted_msgs
  def initialize id_, n_, v_, acc_msgs
    @id = id_; @n = n_; @v = v_; @accepted_msgs = acc_msgs
  end
end
