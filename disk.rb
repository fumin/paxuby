require 'sqlite3'
module Paxos
  module_function
  def disk_conn fn=nil
    SQLite3::Database.new(fn || "#{Paxos::LocalData['local_addr']}paxos.db")
  end

  def setup_disk fn=nil
    conn = disk_conn fn
    conn.execute <<-SQL
      CREATE TABLE paxos(
        id INTEGER PRIMARY KEY,
        promised_n INTEGER NOT NULL CHECK(promised_n > -1), v TEXT);
    SQL
    conn.execute <<-SQL
      CREATE TRIGGER update_paxos BEFORE UPDATE OF promised_n ON paxos BEGIN
        SELECT CASE WHEN NEW.promised_n <= OLD.promised_n THEN
          RAISE(ABORT, 'promised_n should increase upon UPDATEs') END;
        END;
    SQL
  end

  def select_n_where_id_from_disk id, conn=nil
    sql = "SELECT promised_n FROM paxos WHERE id = #{id}"
    row = (conn || disk_conn).execute(sql)[0]
    return nil unless row
    row[0]
  end

  def smallest_executable_id conn=nil
    r = (conn || disk_conn).execute(
          'SELECT id FROM paxos WHERE v IS NOT NULL ORDER BY id DESC LIMIT 1')[0]
    (r ? r[0] : 0) + 1
  end
end

class Paxos::Disk
  attr_reader :id, :n, :v
  def self.find_by_id id_, conn=nil
    db = conn || Paxos.disk_conn
    r = db.execute("SELECT id, promised_n, v FROM paxos WHERE id=#{id_}")[0]
    r ? new(*r) : nil
  end

  def initialize id_, n_, v_
    @id = id_; @n = n_; @v = v_
  end

  def save_n_v_to_disk conn=nil
    raise "id and n should not be nil" unless @id and @n
    begin
      (conn || Paxos.disk_conn).execute("INSERT OR REPLACE INTO paxos(id, promised_n, v) VALUES(#{@id}, #{@n}, '#{@v || 'NULL'}')")
    rescue SQLite3::CantOpenException
      sleep(0.05)
      retry
    end
  end

  def save_n_to_disk conn=nil
    raise "id and n should not be nil" unless @id and @n
    (conn || Paxos.disk_conn).execute("INSERT OR REPLACE INTO paxos(id, promised_n, v) VALUES(#{@id}, #{@n}, (SELECT v FROM paxos WHERE id=#{@id}))")
  end
end
