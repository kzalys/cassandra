package org.apache.cassandra.stress.operations.userdefined;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import org.antlr.runtime.RecognitionException;
import org.apache.cassandra.cql3.CQLFragmentParser;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.cql3.conditions.ColumnCondition;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.stress.generate.*;
import org.apache.cassandra.stress.generate.values.Generator;
import org.apache.cassandra.stress.report.Timer;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.stress.util.JavaDriverClient;
import org.apache.cassandra.utils.Pair;

public class SchemaQuery extends SchemaStatement
{
    public static enum ArgSelect
    {
        MULTIROW, SAMEROW;
        //TODO: FIRSTROW, LASTROW
    }

    final ArgSelect argSelect;
    final Object[][] randomBuffer;
    final Random random = new Random();
    PreparedStatement casReadConditionStatement;
    String casReadConditionQuery;
    List<Integer> keysIndex;
    Map<Integer, Integer> casConditionMapping;

    public SchemaQuery(Timer timer, StressSettings settings, PartitionGenerator generator, SeedManager seedManager, PreparedStatement statement, ConsistencyLevel cl, ArgSelect argSelect, final String tableName)
    {
        super(timer, settings, new DataSpec(generator, seedManager, new DistributionFixed(1), settings.insert.rowPopulationRatio.get(), argSelect == ArgSelect.MULTIROW ? statement.getVariables().size() : 1), statement,
              statement.getVariables().asList().stream().map(d -> d.getName()).collect(Collectors.toList()), cl);
        this.argSelect = argSelect;
        randomBuffer = new Object[argumentIndex.length][argumentIndex.length];

        /**check if given query is cas or not
         * if it is a cas query then we first read db value of all CAS condition
         * columns and then use given db value during the update operation
         * so CAS will be executed like:
         * 1. select {CAS condition columns} from {table}
         * 2. update {table} set {columns} where {condition} IF {CAS condition columns}
         */
        checkCASQuery(tableName);
    }

    private void checkCASQuery(final String tableName) throws IllegalArgumentException {
        try {
            if (statement.getQueryString().toUpperCase().startsWith("UPDATE"))
            {
                final ModificationStatement.Parsed statement = CQLFragmentParser.parseAnyUnhandled(CqlParser::updateStatement,
                                                                                                    this.statement.getQueryString());
                final List<Pair<ColumnMetadata.Raw, ColumnCondition.Raw>> casConditionList = statement.getConditions();
                if (casConditionList.size() > 0)
                {
                    /** here we differenciate between static condition vs dynamic condition
                     *  static condition example: if col1 = NULL
                     *  dynamic condition example: if col1 = ?
                     *  for static condition we don't have to replace value, no extra work
                     *  involved
                     *  for dynamic condition we have to read existing db value and then
                     *  use current db value during update
                     */
                    boolean conditionExpectsDynamicValue = false;
                    for (Pair<ColumnMetadata.Raw, ColumnCondition.Raw> condition : casConditionList)
                    {
                        /**if condition is like a = 10 then condition.right has string value "NULL"
                         * if condition is like a = ? then condition.right has string value "?"
                         * so compare value "NULL" and see if this is a static condition or dynamic
                         */
                        if (!condition.right.getValue().getText().equals("NULL"))
                        {
                            conditionExpectsDynamicValue = true;
                            break;
                        }
                    }
                    if (!conditionExpectsDynamicValue)
                    {
                        return;
                    }

                    List<Integer> casConditionIndex = new ArrayList<Integer>();

                    boolean first = true;
                    casReadConditionQuery = "SELECT ";
                    for (final Pair<ColumnMetadata.Raw, ColumnCondition.Raw> condition : casConditionList)
                    {
                        if (condition.right.getValue().getText().equals("NULL"))
                        {
                            //condition uses static value, ignore it
                            continue;
                        }
                        if (!first)
                        {
                            casReadConditionQuery += ", ";
                        }
                        casReadConditionQuery += condition.left.rawText();
                        casConditionIndex.add(spec.partitionGenerator.indexOf(condition.left.rawText()));
                        first = false;
                    }
                    casReadConditionQuery += " FROM ";
                    casReadConditionQuery += tableName;
                    casReadConditionQuery += " WHERE ";

                    first = true;
                    keysIndex = new ArrayList<Integer>();
                    for (final Generator key : spec.partitionGenerator.partitionKey)
                    {
                        if (!first)
                        {
                            casReadConditionQuery += " AND ";
                        }
                        casReadConditionQuery += key.name;
                        casReadConditionQuery += " = ? ";
                        keysIndex.add(spec.partitionGenerator.indexOf(key.name));
                        first = false;
                    }

                    for (final Generator clusteringKey : spec.partitionGenerator.clusteringComponents)
                    {
                        casReadConditionQuery += " AND ";
                        casReadConditionQuery += clusteringKey.name;
                        casReadConditionQuery += " = ? ";
                        keysIndex.add(spec.partitionGenerator.indexOf(clusteringKey.name));
                    }

                    casConditionMapping = new HashMap<Integer, Integer>();

                    for (final Integer oneConditionIndex : casConditionIndex)
                    {
                        int count = 0;
                        for (Integer oneArgumentIndex : argumentIndex)
                        {
                            if (oneArgumentIndex == oneConditionIndex)
                            {
                                count++;
                            }
                        }
                        casConditionMapping.put(oneConditionIndex, count);
                    }
                }
            }
        } catch (RecognitionException e)
        {
            e.printStackTrace();
            throw new IllegalArgumentException("could not parse update query:" + statement.getQueryString());
        }
    }

    private class JavaDriverRun extends Runner
    {
        final JavaDriverClient client;

        private JavaDriverRun(JavaDriverClient client)
        {
            this.client = client;
        }

        public boolean run() throws Exception
        {
            ResultSet rs;
            //check if query is CAS or not
            if (casReadConditionQuery != null) {
                if (argSelect != ArgSelect.SAMEROW)
                {
                    throw new IllegalArgumentException("CAS is supported only for type 'samerow'");
                }
                if (casReadConditionStatement == null)
                {
                    synchronized (SchemaQuery.this)
                    {
                        if (casReadConditionStatement == null)
                        {
                            casReadConditionStatement = client.prepare(casReadConditionQuery);
                        }
                    }
                }
                final Object keys[] = new Object[keysIndex.size()];
                final Row row = partitions.get(0).next();

                for (int i = 0; i < keysIndex.size(); i++)
                {
                    keys[i] = row.get(keysIndex.get(i));
                }

                //get cas columns' current value from db
                rs = client.getSession().execute(casReadConditionStatement.bind(keys));
                final Object casDbValues[] = new Object[casConditionMapping.size()];

                final com.datastax.driver.core.Row casDbValue = rs.one();
                if (casDbValue != null)
                {
                    for (int i = 0; i < casConditionMapping.size(); i++)
                    {
                        casDbValues[i] = casDbValue.getObject(i);
                    }
                }

                //now execute actual update operation
                rs = client.getSession().execute(bindRowCAS(row,
                                                            casDbValues,
                                                            casConditionMapping));

                /**TODO: we can check 'applied' status here and retry configurable number of times
                 * if CAS didn't applied
                 */
            } else {
                rs = client.getSession().execute(bindArgs());
            }
            rowCount = rs.all().size();
            partitionCount = Math.min(1, rowCount);
            return true;
        }
    }

    private int fillRandom()
    {
        int c = 0;
        PartitionIterator iterator = partitions.get(0);
        while (iterator.hasNext())
        {
            Row row = iterator.next();
            Object[] randomBufferRow = randomBuffer[c++];
            for (int i = 0 ; i < argumentIndex.length ; i++)
                randomBufferRow[i] = row.get(argumentIndex[i]);
            if (c >= randomBuffer.length)
                break;
        }
        assert c > 0;
        return c;
    }

    BoundStatement bindArgs()
    {
        switch (argSelect)
        {
            case MULTIROW:
                int c = fillRandom();
                for (int i = 0 ; i < argumentIndex.length ; i++)
                {
                    int argIndex = argumentIndex[i];
                    bindBuffer[i] = randomBuffer[argIndex < 0 ? 0 : random.nextInt(c)][i];
                }
                return statement.bind(bindBuffer);
            case SAMEROW:
                return bindRow(partitions.get(0).next());
            default:
                throw new IllegalStateException();
        }
    }

    @Override
    public void run(JavaDriverClient client) throws IOException
    {
        timeWithRetry(new JavaDriverRun(client));
    }
}
