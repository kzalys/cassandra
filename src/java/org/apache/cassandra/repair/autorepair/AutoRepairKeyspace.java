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

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;

public class AutoRepairKeyspace
{
    private AutoRepairKeyspace()
    {
    }

    /**
     * Generation is used as a timestamp for automatic table creation on startup.
     * If you make any changes to the tables below, make sure to increment the
     * generation and document your change here.
     *
     * gen 0: original definition in 3.0
     */
    public static final long GENERATION = 2;

    static final String AUTO_REPAIR_HISTORY = "auto_repair_history";

    public static final String AUTO_REPAIR_PRIORITY = "auto_repair_priority";

    private static final TableMetadata AutoRepairHistory =
    parse(AUTO_REPAIR_HISTORY,
            "Auto repair history for each node",
            "CREATE TABLE %s ("
            + "pid int,"
            + "host_id uuid,"
            + "repair_turn text,"
            + "repair_start_ts timestamp,"
            + "repair_finish_ts timestamp,"
            + "delete_hosts set<uuid>,"
            + "delete_hosts_update_time timestamp,"
            + "force_repair boolean,"
            + "PRIMARY KEY (pid, host_id))");

    private static final TableMetadata AutoRepairPriority =
    parse(AUTO_REPAIR_PRIORITY,
            "Auto repair priority for each group",
            "CREATE TABLE %s ("
            + "pid int,"
            + "repair_priority set<uuid>,"
            + "PRIMARY KEY (pid))");


    private static TableMetadata parse(String name, String description, String schema)
    {
        return CreateTableStatement.parse(String.format(schema, name), SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME)
                                   .id(TableId.forSystemTable(SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, name))
                                   .comment(description)
                                   .gcGraceSeconds((int) TimeUnit.DAYS.toSeconds(90))
                                   .build();
    }

    public static KeyspaceMetadata metadata()
    {
        return KeyspaceMetadata.create(SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, KeyspaceParams.simple(1), Tables.of(AutoRepairPriority, AutoRepairHistory));
    }
}
