# Paxuby
Paxos algorithm in Ruby

## Test
* `bacon test/test.rb`
* The logs of the test can be displayed by
  ```
  ps aux | grep ruby; cat *log; echo 'select * from paxos;' | sqlite3 '127.0.0.1:6660paxos.db'
  ```

## License
Copyright 2013 Awaw Fumin awawfumin@gmail.com  
Licensed under GNU Affero General Public License v3

## Todos
* implement snapshots
