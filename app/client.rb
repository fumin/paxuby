require './client'

module App; end
class App::Client < Paxos::Client

  def get k
    puts_and_gets "GET #{k}"
  end

  def set k, v
    puts_and_gets "SET #{k} #{v}"
  end

  def del k
    puts_and_gets "DEL #{k}"
  end

end
