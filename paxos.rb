require 'sqlite3'
module Paxos
  H = {}
  AcceptorQueue = Queue.new
  ClientHandlerQueue = Queue.new
  ProposerQueue = Queue.new
  CatchupQueue = Queue.new
  LocalData = {}
  NoOp = 'NoOp'
  module_function
  def smallest_executable_id
    r = disk_conn.execute(
          'SELECT id FROM paxos WHERE v IS NOT NULL ORDER BY id DESC LIMIT 1')[0]
    (r || [0])[0] + 1
  end
  def propose_phase_1 id
    n = n_of_id
    return_the_v # which is everyone's consensus or nil

  end
  def propose id, command, suggested_n=1

  end
  def cannot_be_executed id

  end
  def disk_conn; SQLite3::Database.new "paxos.db"; end
  def setup_disk
    disk_conn.execute <<-SQL
      CREATE TABLE paxos(
        id INTEGER PRIMARY KEY, promised_n INTEGER NOT NULL, v TEXT);
      CREATE TRIGGER update_paxos BEFORE UPDATE OF promised_n ON paxos BEGIN
        SELECT CASE WHEN NEW.promised_n <= OLD.promised_n THEN
          RAISE(ABORT, 'promised_n should increase upon UPDATEs') END;
        END;
    SQL
  end
end
