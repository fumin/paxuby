require './client'

module App; end
class App::Client < Paxos::Client

  def get k, follow_redirect=true
    return puts_and_gets "GET #{k}" unless follow_redirect
    puts_gets_follow_redirect "GET #{k}"
  end

  def set k, v, follow_redirect=true
    return puts_and_gets "SET #{k} #{v}" unless follow_redirect
    puts_gets_follow_redirect "SET #{k} #{v}"
  end

  def del k, follow_redirect=true
    return puts_and_gets "DEL #{k}" unless follow_redirect
    puts_gets_follow_redirect "DEL #{k}"
  end

end
