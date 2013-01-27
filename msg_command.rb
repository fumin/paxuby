module Paxos; end
class Paxos::Msg
  PREPARE = 'prepare'; PROMISE = 'promise'; ACCEPT = 'accept'
  ACCEPTED = 'accepted'; IGNORED = 'ignored'
  attr_reader :id, :type, :n, :v
  def initialize s
    if m = s.match(/(\d+) #{PREPARE} (\d+)/)
      @id = m[1]; @type = PREPARE; @n = m[2]
    elsif m = s.match(/(\d+) #{PROMISE} (\d+) (\w+)/)
      @id = m[1]; @type = PROMISE; @n = m[2]; @v = m[3]
    elsif m = s.match(/(\d+) #{PROMISE}/)
      @id = m[1]; @type = PROMISE
    elsif m = s.match(/(\d+) #{ACCEPT} (\d+) (\w+)/)
      @id = m[1]; @type = ACCEPT; @n = m[2]; @v = m[3]
    end
  end
  def to_s
    res = "#{@id} #{@type}"
    res << " #{@n}" if @n
    res << " #{@v}" if @v
    res
  end
  def ok?; @id; end
end
