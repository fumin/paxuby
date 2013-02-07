# Paxuby
Paxos algorithm in Ruby

## Introduction
The Paxos algorithm is a cornerstone in building *distributed*, *fault-tolerant*
storage systems. Regardless of its backend, under the assumption that only *less than half* of a system's instances may encouter failures, Paxos guarantees that:
* service continues to be available without performance compromises
* data remains consistent at every single moment (compare [Eventual consistency](http://en.wikipedia.org/wiki/Eventual_consistency))

Besides the above guarantess, this particular implementation aims at:
* providing a convenient interface to add Paxos support for arbitrary backends, possibly Postgresql, Mysql, Redis, etc. (a feature that is theoretically given but less seen in actually implementations)
* easier maintenance by being written in a high level language, while sustaining sufficient performance (median < 10ms, 90% < 15ms, 99% < 20ms)

## Usage
Assuming you are using the default key-value backend,
* Set up 'config.yaml' to list the IPs of your cluster, for example
  ```
  addrs:
    - 127.0.0.1:6660
    - 127.0.0.1:6661
    - 127.0.0.1:6662
  ```
* Run `ruby main.rb` in each machine
* Interract with Paxos using the built-in client:
  ```
  irb> require './app/client'
  irb> client = App::Client.new '127.0.0.1:6660'
  irb> client.get 'some_key'
    => 'nil'
  irb> client.set 'some_key', 'the_value'
  irb> client.get 'some_key'
    => 'the_value'
  ```
* Enjoy the fault-tolerance provided by Paxos

## Interface between paxuby and backends
The main interface lies in the `App` module. Custom backends can be supported by implementing:
* the class `App::Command` which assigns the instance variable `@err_msg`
  when there's an error parsing a command string
* the module_function `App.execute`, which is expected to return a `Paxos::App::Result` object.
A straight forward example be found in './app/app.rb'.

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
* implement snapshots
