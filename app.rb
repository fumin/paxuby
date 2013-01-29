require './paxos_app'

module App
  DICT = {}
  class Command
    SET = 'SET'; GET = 'GET'; DEL = 'DEL'
    attr_reader :type, :key, :value, :err_msg
    def initialize s
      if m = s.match(/^#{SET} (\w+) (\w+)$/)
        @type = SET; @key = m[1]; @value = m[2]
      elsif m = s.match(/^#{GET} (\w+)$/)
        @type = GET; @key = m[1]
      elsif m = s.match(/^#{DEL} (\w+)$/)
        @type = DEL; @key = m[1]
      else
        @err_msg = "USAGE: 'SET key val' or 'GET key' or 'DEL key'"
      end
    end
  end

  module_function
  def execute s
    command = Command.new s
    case command.type
    when Command::SET
      Paxos::App::Result.new(true, DICT[command.key] = command.value)
    when Command::GET
      Paxos::App::Result.new(true, DICT[command.key] || 'nil')
    when Command::DEL
      Paxos::App::Result.new(true, DICT.delete(command.key))
    else
      Paxos::App::Result.new(false, "Unrecognized command: #{s}")
    end
  end
end
