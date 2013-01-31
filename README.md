# Paxuby
Paxos algorithm in Ruby

## Test
* `bacon test.rb`
* The logs of the test can be displayed by
  ```
  ps aux | grep ruby; cat *log; echo 'select * from paxos;' | sqlite3 '127.0.0.1:6660paxos.db'
  ```

## License
Copyright 2013 Awaw Fumin awawfumin@gmail.com  
Licensed under GNU Affero General Public License v3

## Todos
* test catchup, data consistency, no change in leader even after 2 secs
* kill leader, other replica becomes leader, data consistency, leader wakes up and catchup, becomes leader after 2 secs, data consistency
* shouldn't work when 2 machines killed

* implement snapshots
