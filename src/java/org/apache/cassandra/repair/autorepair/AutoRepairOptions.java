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

package org.apache.cassandra.repair.autorepair;

public class AutoRepairOptions
{
    public Boolean auto_repair_enabled = true;

    // This should be false for production, this is only set to true for Cassandra dtest so auto repair won't be triggerd
    // by the auto repair executor scheduleWithFixedDelay function
    public Boolean auto_repair_auto_schedule = true;

    // auto repair is default repair table by table, if this is enabled, we will repair keyspace by keyspace
    public Boolean auto_repair_by_keyspace = false;

    public Integer auto_repair_no_of_subranges = 1;

    public Integer auto_repair_number_of_repair_threads = 1;

    public Integer auto_repair_parallel_repair_count_in_group = 1;

    public Integer auto_repair_parallel_repair_percentage_in_group = 0;  // this number should be between 0 - 100 inclusive

    public Integer auto_repair_sstable_upper_threshold = 10000;

    public String auto_repair_ignore_keyspaces = "system.*";

    public String auto_repair_only_keyspaces = "";

    public Integer auto_repair_check_interval_in_sec = 300;

    public Integer auto_repair_min_repair_frequency_in_hours = 24;

    public String auto_repair_ignore_dc = "";

    // Set this 'true' if AutoRepair should repair only the primary ranges owned by this node; else, 'false'
    public Boolean auto_repair_primary_token_range_only = true;

    // by default, the value is empty, it means the all nodes are in one ring and the repair in that ring should be done
    // node by node. Adding this config for special case(cstar-peloton) that certain keyspace data is only stored in
    // certain cluster. We can run multiple node repair at the same time without worrying about the conflict. For example,
    // if the value is "dca1,phx2|dca11|phx3|phx4,dca5,dca6", there will be 4 groups {dca1, phx2}, {dca11}, {phx3} and
    // {phx4, dca5, dca6}. This means we can run repair parallely on 4 nodes, each in one group.
    public String auto_repair_dc_groups = "";

    public boolean auto_repair_force_repair_new_node = false;

    public Long auto_repair_table_max_repair_time_in_sec = 6 * 60 * 60L;

    public Integer auto_repair_history_clear_delete_hosts_buffer_in_sec = 60 * 60 * 2; // two hours

    public Boolean isAutoRepairEnabled()
    {
        return auto_repair_enabled;
    }

    public Boolean getAutoRepairAutoSchedule()
    {
        return auto_repair_auto_schedule;
    }

    public Integer getAutoRepairNumberOfSubRanges()
    {
        return auto_repair_no_of_subranges;
    }

    public Integer getAutoRepairNumberOfRepairThreads()
    {
        return auto_repair_number_of_repair_threads;
    }

    public Integer getAutoRepairParallelRepairCountInGroup() {
        return auto_repair_parallel_repair_count_in_group;
    }

    public Integer getAutoRepairParallelRepairPercentageInGroup() {
        return auto_repair_parallel_repair_percentage_in_group;
    }

    public Integer getAutoRepairSSTableUpperThreshold()
    {
        return auto_repair_sstable_upper_threshold;
    }

    public String getAutoRepairIgnoreKeyspaces()
    {
        return auto_repair_ignore_keyspaces;
    }

    public String getAutoRepairOnlyKeyspaces()
    {
        return auto_repair_only_keyspaces;
    }

    public Integer getAutoRepairCheckInterval()
    {
        return auto_repair_check_interval_in_sec;
    }

    public Integer getAutoRepairMinRepairFrequencyInHours()
    {
        return auto_repair_min_repair_frequency_in_hours;
    }

    public Integer getAutoRepairHistoryClearDeleteHostsBufferInSec() {
        return auto_repair_history_clear_delete_hosts_buffer_in_sec;
    }

    public String getAutoRepairIgnoreDC()
    {
        return auto_repair_ignore_dc;
    }

    public String getAutoRepairDCGroups()
    {
        return auto_repair_dc_groups;
    }

    public boolean isAutoRepairForceRepairNewNode() {return auto_repair_force_repair_new_node;}

    public boolean getAutoRepairByKeyspace() {
        return auto_repair_by_keyspace;
    }

    public long getAutoRepairTableMaxRepairTimeInSec()
    {
        return auto_repair_table_max_repair_time_in_sec;
    }

    public boolean getPrimaryTokenRangeOnly()
    {
        return auto_repair_primary_token_range_only;
    }

    public void setAutoRepairParallelRepairCountInGroup(int count) {
        this.auto_repair_parallel_repair_count_in_group = count;
    }

    public void setAutoRepairParallelRepairPercentageInGroup(int percentage) {
        this.auto_repair_parallel_repair_percentage_in_group = percentage;
    }
}
