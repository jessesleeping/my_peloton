[![Peloton Logo](http://db.cs.cmu.edu/wordpress/wp-content/uploads/2015/11/peloton.jpg)](http://pelotondb.org/)

[![Build Status](http://jenkins.db.cs.cmu.edu:8080/job/Peloton/badge/icon?style=flat)](http://jenkins.db.cs.cmu.edu:8080/job/Peloton/)

## TODO List
* CKP 1 (due in 02/21/2016)
	* [ ] **pos/neg infinity bug**
	* [x] **implement iterator:** jiexi --> change to `Scanner`
	* [x] **scan:** rxian + runshenz -> Change to `Search` and `Buffer`
	* [x] **bwtree insert, delete interface:** ?? 
	* [x] **bwtree index wrapper:** ?? 
	* **test:** 
* **node_table, fixed size --> dynamic resize. (list + vector)** ??
* **buffer** --> **buffer key** 

## Unsolved Problems:
* bwtree iterator, erase
* deleteDelta needs value comparator

## What Is Peloton?

Peloton is an in-memory DBMS designed for real-time analytics. It can handle both fast ACID transactions and complex analytical queries on the same database. 

## What Problem Does Peloton Solve?

The current trend is to use specialized systems that are optimized for only one of these workloads, and thus require an organization to maintain separate copies of the database. This adds additional cost to deploying a database application in terms of both storage and administration overhead. We present a hybrid DBMS architecture that efficiently supports varied workloads on the same database.

## How Does Peloton Accomplish Its Goals?

Our approach differs from previous methods in that we use a single execution engine that is oblivious to the storage layout of data without sacrificing the performance benefits of the specialized systems. This obviates the need to maintain separate copies of the database in multiple independent systems.

For more details, please visit the [Peloton Wiki](https://github.com/cmu-db/peloton/wiki "Peloton Wiki") page.

If you have any questions on Peloton usage, please send a mail to peloton-dev@cs.cmu.edu.

## Installation

Check out the [installation instructions](https://github.com/cmu-db/peloton/wiki/Installation).

## Development / Contributing

Please look up the [contributing guide](https://github.com/cmu-db/peloton/blob/master/CONTRIBUTING.md#development) for details.

## Issues

Before reporting a problem, check out this how to [file an issue](https://github.com/cmu-db/peloton/blob/master/CONTRIBUTING.md#file-an-issue) guide.

## Contributors

[https://github.com/cmu-db/peloton/graphs/contributors](https://github.com/cmu-db/peloton/graphs/contributors)

## License

Copyright (c) 2014-15 [Carnegie Mellon Database Group](http://db.cs.cmu.edu/)  
Licensed under the [Apache License](LICENSE).
