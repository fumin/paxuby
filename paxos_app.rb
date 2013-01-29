module Paxos; end
module Paxos::App
  class Command
    SET = 'PAXOS_SET'; GET = 'PAXOS_GET'; DEL = 'PAXOS_DEL'; NoOp = 'PAXOS_NoOp'
    attr_reader :instruction, :k, :v
    def self.create s
      command = new s
      command.instruction ? command : nil
    end
    def initialize s
      if m = s.match(/^#{SET} (\w+) (\w+)$/)
        @instruction = SET; @k = m[1]; @v = m[2]
      elsif m = s.match(/^#{GET} (\w+)$/)
        @instruction = GET; @k = m[1]
      elsif m = s.match(/^#{DEL} (\w+)$/)
        @instruction = DEL; @k = m[1]
      elsif s == NoOp
        @instruction = NoOp
      end
    end
  end
  class Result
    attr_reader :is_successful, :value
    def initialize is_s, v
      @is_successful = is_s; @value = v
    end
  end
  module_function
  def execute command
    case command.instruction
    when Command::NoOp; Result.new(true, true)
    when Command::SET; Result.new(true, Paxos::H[command.k] = command.v)
    when Command::GET; Result.new(true, Paxos::H[command.k])
    when Command::DEL; Result.new(true, Paxos::H.delete(command.k))
    else; Result.new(false, "Unrecognized command: #{command}")
    end
  end
end
