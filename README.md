# Paxuby
Paxos algorithm in Ruby

## Testing and Profiling
* Run `bacon test/test.rb` to run all tests
* The logs of the test can be displayed by
  ```
  ps aux | grep ruby; cat *log; echo 'select * from paxos;' | sqlite3 '127.0.0.1:6660paxos.db'
  ```
* Run `ruby test/perf.rb` to profile

## License
Copyright 2013 Awaw Fumin awawfumin@gmail.com  
Licensed under GNU Affero General Public License v3

## Todos
* README
* implement snapshots
