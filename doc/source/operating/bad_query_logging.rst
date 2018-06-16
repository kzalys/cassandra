.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at
..
..     http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing, software
.. distributed under the License is distributed on an "AS IS" BASIS,
.. WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
.. See the License for the specific language governing permissions and
.. limitations under the License.

.. highlight:: none



BadQuery Logging
----------------

Cassandra has the ability to detect different types of bad queries happening in the cluster. These bad queries doesn’t always means there is a definite problem in the cluster. It is trying to warn about some uncommon behavior for given query, on the other side if there is some problem in the cluster then these are not the only bad patterns in your cluster, it could be any other scenario as well. Here Cassandra is trying to inform user about some uncommon behavior which could have potential effect on your workload. At the end you will have to analyze everything for your workload.


What does it capture
^^^^^^^^^^^^^^^^^^^^^^^

BadQuery logging captures important information like keyspace name, table name, partition key, problem type, etc. such that end user can take action on them.


Limitations
^^^^^^^^^^^

BadQuery yet doesn't cover all different aspects which could have potentatial performance problems with cluster.


What does it log
^^^^^^^^^^^^^^^^^^^

 - ``policyid``: BadQuery type (SlowQuery, Incorrect compaction type, etc.)
 - ``keyspace``: Keyspace name
 - ``table``: Table name
 - ``partition key``: Partition key (if applicable)
 - ``actual value``: Actual values for given query (latency, read partition size, etc.)
 - ``problem``: Problem text with more details


How to configure
^^^^^^^^^^^^^^^^^^

BadQuery logging can be configured using cassandra.yaml. You can also change different threshold for BadQuery using ``nodetool``.

cassandra.yaml configurations for BadQuery
"""""""""""""""""""""""""""""""""""""""""""
	- ``enabled``: This option enables/disables badquery logging
	- ``reporter``: Class name of the BadQuery logger/custom logger if not set, default to `system.log`
	- ``tracing_fraction``: BadQuery tracing fraction, if not set, default to `25%` 
	- ``logging_interval_in_secs``: BadQuery logging interval, if not set, default to `15 minutes`
	- ``max_samples_per_interval_in_syslog``: Maximum badquery samples to log in ``system.log`` per interval, if not set, default to `5` for each type of badquery 
	- ``max_samples_per_interval_in_table``: Maximum badquery samples to log in table per interval, if not set, default to `5` for each type of badquery 
	- ``write_max_partitionsize_in_bytes``: max partition size threshold during write operation, if not set, default to `50M` 
	- ``read_max_partitionsize_in_bytes``: max partition size threshold during read operation, if not set, default to `50M` 
	- ``read_slow_local_latency_in_ms``: read latency threshold for slow local reads, if not set, default to `100ms` 
	- ``write_slow_local_latency_in_ms``: write latency threshold for slow local writes, if not set, default to `100ms` 
	- ``read_slow_coord_latency_in_ms``: read latency threshold for slow coordinator reads, if not set, default to `200ms` 
	- ``write_slow_coord_latency_in_ms``: write latency threshold for slow coordinator writes, if not set, default to `200ms` 
	- ``tombstone_limit``: tombstone threshold, if not set, default to `1000` 
	- ``ignore_keyspaces``: Comma separated list of keyspaces to be excluded in badquery loggin, default - excludes none of the keyspaces 


NodeTool command to get thresholds for BadQuery logging
""""""""""""""""""""""""""""""""""""""""""""""""""""""""
``getbadquerythreshold``: Gets currently set thresholds with yaml defaults. 

::

    nodetool getbadquerythreshold

NodeTool command to set thresholds for BadQuery logging
""""""""""""""""""""""""""""""""""""""""""""""""""""""""
``setbadquerythreshold``: Sets currently set thresholds with yaml defaults. yaml configurations can be overridden using options via nodetool command.

::

    nodetool setbadquerythreshold <badquerythresholdtype> <thresholdvalue>

badquerythresholdtype
*********************


``badquerytracingfraction``
    BadQuery tracing fraction which determines what percentage of queries
    to trace. If not set the value from cassandra.yaml will be used

``badquerymaxsamplesinsyslog``
    Maximum samples to report for each badquery type in sytem.log. If
    not set the value from cassandra.yaml will be used

``badquerymaxsamplesintable``
    Maximum samples to report for each badquery type in table. If
    not set the value from cassandra.yaml will be used

``badqueryreadmaxpartitionsizeinbytes``
    Threshold for maximum partition size in bytes for read operation. If not
    set the value from cassandra.yaml will be used

``badquerywritemaxpartitionsizeinbytes``
    Threshold for maximum partition size in bytes for write operation. If not
    set the value from cassandra.yaml will be used

``badqueryreadslowlocallatencyinms``
    Threshold for local read latency in milliseconds. If not
    set the value from cassandra.yaml will be used

``badquerywriteslowlocallatencyinms``
    Threshold for local write latency in milliseconds. If not
    set the value from cassandra.yaml will be used

``badqueryreadslowcoordlatencyinms``
    Threshold for coordinator read latency in milliseconds. If not
    set the value from cassandra.yaml will be used

``badquerywriteslowcoordlatencyinms``
    Threshold for coordinator write latency in milliseconds. If not
    set the value from cassandra.yaml will be used

``badquerytombstonelimit``
    Threshold limit for tombstones for given query. If
    not set the value from cassandra.yaml will be used

``badqueryignorekeyspaces``
    Comma separated list of keyspaces to be excluded in badquery loggin. If
    not set the value from cassandra.yaml will be used


NodeTool command to disable BadQuery logging
"""""""""""""""""""""""""""""""""""""""""""""
``nodetool setbadquerythreshold badquerytracingfraction 0.0``

NodeTool command to change threshold for max partition size during write
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
``nodetool setbadquerythreshold badquerywritemaxpartitionsizeinbytes 104857600``

NodeTool command to get current thresholds
"""""""""""""""""""""""""""""""""""""""""""
``nodetool getbadquerythreshold``


Sample output in system.log
^^^^^^^^^^^^^^^^^^^^^^^^^^^

::

    LogMessage: 

``policyid:0, slow local read detected: ks:k1, table:t1, key:abc, latency:128ms``

``policyid:1, slow coordinator read detected: ks:k2, table:t2, key:920fd087-46ba-4b86-838a-f0a216528b8f, latency:249ms``

``policyid:3, slow coordinator write detected: ks:k1, table:t4, key:hello:1, latency:496ms``

``policyid:7, incorrect consistency level detected: ks:k5, table:t2, problemText:found QUORUM, recommendation is to use LOCAL_QUORUM``

``policyid:6, incorrect compaction strategy detected: ks:k9, table:t3, problemText:found STCS for ttl data, recommendation is to use TWCS for ttl data``

``policyid:8, too many tombstones detected: ks:k7, table:t4, key:10, tombstones:10437``

``policyid:5, large partition write detected: ks:k6, table:t11, key:63761e7f-f836-4d5b-b8ab-2f2382bc0ef9:11, size:64686276B``


How to get BadQuery output in table insetad of ``system.log``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To use ``BadQueriesInTable`` as a logger in badquery logging, set the logger to ``BadQueriesInTable`` in cassandra.yaml under section ``bad_query_options``, sub-section ``reporter``. And then you can get all the badqueries by running following query on ``cqlsh``:
 -  ``SELECT * FROM system_monitor.badquery;``

Different types of BadQueries explained here
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**policyid:0, slow local read detected: ks:k1, table:t1, key:abc, latency:128ms**
  + keyspace name: k1
  + table name: t1
  + key: abc
  + latency: 128ms    

This means slower local read detected against query. This metrics doesn't involve time talking to peer nodes. This may happen for a variety of reasons, few of the most common scenarios are:
    * Data hotspots in which few queries are reading more data than others.
    * Overall Cassandra cluster has become slow.
    * Particular node has some problem.

Tools which may help: `nodetool tablehistograms`, `nodetool toppartitions`


**policyid:1, slow coordinator read detected: ks:k2, table:t2, key:920fd087-46ba-4b86-838a-f0a216528b8f, latency:249ms**
  + keyspace name: k2
  + table name: t3
  + key: 920fd087-46ba-4b86-838a-f0a216528b8f
  + latency: 249ms    

This means slower coordinator read detected against query. This metrics involves time talking to peer nodes as well. This may happen for a variety of reasons, few of the most common scenarios are:
    * Data hotspots in which few queries are reading more data than others.
    * Overall Cassandra cluster has become slow.
    * Particular node has some problem.
    * Some problem with network.

Tools which may help: `nodetool tablehistograms`, `nodetool toppartitions`

**policyid:2, slow local write detected: ks:k1, table:t4, key:hello:1, latency:323ms**
  + keyspace name: k1
  + table name: t4
  + key: hello:1
  + latency: 323ms    

This means slower local write detected against query. This metrics doesn't involve time talking to peer nodes. This may happen for variety of reasons, few of the most common scenarios leading to this are highlighted here:
    * Data hotspots in which few queries are writing more data than others.
    * Overall Cassandra cluster has become slow.
    * Particular node has some problem.

**policyid:3, slow coordinator write detected: ks:k1, table:t4, key:hello:1, latency:496ms**
  + keyspace name: k1
  + table name: t4
  + key: hello:1
  + latency: 496ms    

This means slower coordinator write detected against query. This metrics involves time talking to peer nodes as well. This may happen for variety of reasons, few of the most common scenarios leading to this are highlighted here:
    * Data hotspots in which few queries are writing more data than others.
    * Overall Cassandra cluster has become slow.
    * Particular node has some problem.
    * Some problem with network.

**policyid:4, large partition read detected: ks:k123, table:test11, key:63761e7f-f836-4d5b-b8ab-2f2382bc0ef9:11, size:54006276B**
  + keyspace name: k123
  + table name: test11
  + key: 63761e7f-f836-4d5b-b8ab-2f2382bc0ef9:11
  + latency: 54006276B  

This means large partition read has happened. Casandra works well if partitions are smaller so try to keep your partitions as small as possible. This may happen for variety of reasons, few of the most common scenarios leading to this are highlighted here:
    * You have some data hotspots due to which certain partitions are bigger than others.
    * You are using Cassandra as a blob store.

**policyid:5, large partition write detected: ks:k6, table:t11, key:63761e7f-f836-4d5b-b8ab-2f2382bc0ef9:11, size:64686276B**
  + keyspace name: k6
  + table name: t11
  + key: 63761e7f-f836-4d5b-b8ab-2f2382bc0ef9:11
  + latency: 64686276B  

This means large partition write has happened. Casandra works well if partitions are smaller so try to keep your partitions as small as possible. This may happen for variety of reasons, few of the most common scenarios leading to this are highlighted here:
    * You have some data hotspots due to which certain partitions are bigger than others.
    * You are using Cassandra as a blob store.

**policyid:6, incorrect compaction strategy detected: ks:k9, table:t3, problemText:found STCS for ttl data, recommendation is to use TWCS for ttl data**
  + keyspace name: k9
  + table name: t3

This means incorrect compaction strategy detected. This happens in following scenarios:
    * If compaction strategy on your table is other than TWCS and you are writing TTL data. If you want to use TTL data then TWCS is the recommended compaction strategy.

**policyid:7, incorrect consistency level detected: ks:k5, table:t2, problemText:found QUORUM, recommendation is to use LOCAL_QUORUM**
  + keyspace name: k5
  + table name: t2

This means incorrect consistency level. This happens in following scenarios:
    * If you have configured your keyspace as NetworkTopologyStrategy then recommendation is to use LOCAL_<ConsistencyLevel> instead of global <ConsistencyLevel> to limit read/writes within local data center only. To solve this warning, please change your consistency level to LOCAL_<ConsistencyLevel>.

**policyid:8, too many tombstones detected: ks:k7, table:t4, key:10:20:30, tombstones:10437**
  + keyspace name: k7
  + table name: t4
  + key: 10:20:30
  + tombstones: 10437  

Too many tombstones detected. This may happen for variety of reasons, few of the most common scenarios leading to this are highlighted here:
    * Incorrect data model in which you are adding and deleting cells to the same partitions.
    * Maybe using Cassandra as a queue https://www.datastax.com/dev/blog/cassandra-anti-patterns-queues-and-queue-like-datasets.
