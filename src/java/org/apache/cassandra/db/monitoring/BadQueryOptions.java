/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.monitoring;

public class BadQueryOptions
{
    /**
     * whether to trace query for badquery or not
     */
    public Boolean enabled = false;

    /**
     * BadQuery logging can be configured using this option, default is to system.log
     */
    public String reporter;

    /**
     * percentage of queries to trace against badquery. Ideally we can make this to
     * 100% as logging bad query is very light weight and doesn't impact ongoing traffic
     * but one can control using this parameter
     */
    public Double tracing_fraction = 0.25;

    /**
     * Default logging will be in system.log file, interval at which badqueries will be
     * logged to system.log file
     */
    public Integer logging_interval_in_secs = 15 * 60;

    /**
     * maximum bad query samples to log in system.log file. This is to not flood log
     * files. Once can override reporter and consume all the bad queries in
     * cluster
     */
    public Integer max_samples_per_interval_in_syslog = 5;

    /**
     * maximum bad query samples to log in table. This is to not flood Cassandra
     * table. Once can override reporter and consume all the bad queries in
     * cluster
     */
    public Integer max_samples_per_interval_in_table = 5;

    /**
     * Maximum partition size during read at which given query will be considered as
     * badquery
     */
    public Long read_max_partitionsize_in_bytes = 50 * 1024 * 1024L;

    /**
     * Maximum partition size during write at which given query will be considered as
     * badquery
     */
    public Long write_max_partitionsize_in_bytes = 50 * 1024 * 1024L;

    /**
     * local read latency threshold
     */
    public Integer read_slow_local_latency_in_ms = 100;

    /**
     * local write latency threshold
     */
    public Integer write_slow_local_latency_in_ms = 100;

    /**
     * coordinator read latency threshold
     */
    public Integer read_slow_coord_latency_in_ms = 200;

    /**
     * coordinator write latency threshold
     */
    public Integer write_slow_coord_latency_in_ms = 200;

    /**
     * tombstone threshold beyond this given query will be qualified for badquery
     */
    public Integer tombstone_limit = 1000;

    /**
     * ignore keyspaces if we don't want to trace for bad query
     */
    public String ignore_keyspaces = "";
}
