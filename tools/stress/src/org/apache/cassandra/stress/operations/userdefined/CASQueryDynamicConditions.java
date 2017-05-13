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


import com.datastax.driver.core.*;
import org.antlr.runtime.RecognitionException;
import org.apache.cassandra.cql3.CQLFragmentParser;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.cql3.conditions.ColumnCondition;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.stress.generate.Row;
import org.apache.cassandra.stress.generate.values.Generator;
import org.apache.cassandra.stress.util.JavaDriverClient;
import org.apache.cassandra.utils.Pair;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CASQueryDynamicConditions
{
    private final SchemaQuery schemaQuery;
    private final String tableName;
    private List<Integer> keysIndex;
    private Map<Integer, Integer> casConditionArgFreqMap;
    private PreparedStatement casReadConditionStatement;
    private StringBuilder casReadConditionQuery;

    CASQueryDynamicConditions(SchemaQuery schemaQuery, String tableName)
    {
        this.schemaQuery = schemaQuery;
        this.tableName = tableName;
        prepareCASDynamicConditionsReadStatement();
    }

    public static boolean dynamicConditionExists(PreparedStatement statement) throws IllegalArgumentException
    {
        if (statement.getQueryString().toUpperCase().startsWith("UPDATE"))
        {
            ModificationStatement.Parsed modificationStatement = null;
            try
            {
                modificationStatement = CQLFragmentParser.parseAnyUnhandled(CqlParser::updateStatement,
                        statement.getQueryString());
            }
            catch (RecognitionException e)
            {
                e.printStackTrace();
                throw new IllegalArgumentException("could not parse update query:" + statement.getQueryString());
            }

            List<Pair<ColumnMetadata.Raw, ColumnCondition.Raw>> casConditionList = modificationStatement.getConditions();
            if (casConditionList.size() > 0)
            {
                /** here we differenciate between static condition vs dynamic condition
                 *  static condition example: if col1 = NULL
                 *  dynamic condition example: if col1 = ?
                 *  for static condition we don't have to replace value, no extra work
                 *  involved
                 *  for dynamic condition we have to read existing db value and then
                 *  use current db values during the update
                 */
                boolean conditionExpectsDynamicValue = false;
                for (Pair<ColumnMetadata.Raw, ColumnCondition.Raw> condition : casConditionList)
                {
                    /**if condition is like a = 10 then condition.right has string value "NULL"
                     * if condition is like a = ? then condition.right has string value "?"
                     * so compare value "NULL" and see if this is a static condition or dynamic
                     */
                    if (condition.right.getValue().getText().equals("?"))
                    {
                        conditionExpectsDynamicValue = true;
                        break;
                    }
                }
                if (!conditionExpectsDynamicValue)
                {
                    return false;
                }
                else
                {
                    return true;
                }
            }
        }
        return false;
    }

    private void prepareCASDynamicConditionsReadStatement()
    {
        ModificationStatement.Parsed modificationStatement = null;
        try
        {
            modificationStatement = CQLFragmentParser.parseAnyUnhandled(CqlParser::updateStatement,
                    schemaQuery.statement.getQueryString());
        }
        catch (RecognitionException e)
        {
            e.printStackTrace();
            throw new IllegalArgumentException("could not parse update query:" + schemaQuery.statement.getQueryString());
        }
        final List<Pair<ColumnMetadata.Raw, ColumnCondition.Raw>> casConditionList = modificationStatement.getConditions();

        List<Integer> casConditionIndex = new ArrayList<Integer>();

        boolean first = true;
        casReadConditionQuery = new StringBuilder();
        casReadConditionQuery.append("SELECT ");
        for (final Pair<ColumnMetadata.Raw, ColumnCondition.Raw> condition : casConditionList)
        {
            if (!condition.right.getValue().getText().equals("?"))
            {
                //condition uses static value, ignore it
                continue;
            }
            if (!first)
            {
                casReadConditionQuery.append(", ");
            }
            casReadConditionQuery.append(condition.left.rawText());
            casConditionIndex.add(schemaQuery.getDataSpecification().partitionGenerator.indexOf(condition.left.rawText()));
            first = false;
        }
        casReadConditionQuery.append(" FROM ");
        casReadConditionQuery.append(tableName);
        casReadConditionQuery.append(" WHERE ");

        first = true;
        keysIndex = new ArrayList<Integer>();
        for (final Generator key : schemaQuery.getDataSpecification().partitionGenerator.partitionKey)
        {
            if (!first)
            {
                casReadConditionQuery.append(" AND ");
            }
            casReadConditionQuery.append(key.name);
            casReadConditionQuery.append(" = ? ");
            keysIndex.add(schemaQuery.getDataSpecification().partitionGenerator.indexOf(key.name));
            first = false;
        }
        for (final Generator clusteringKey : schemaQuery.getDataSpecification().partitionGenerator.clusteringComponents)
        {
            casReadConditionQuery.append(" AND ");
            casReadConditionQuery.append(clusteringKey.name);
            casReadConditionQuery.append(" = ? ");
            keysIndex.add(schemaQuery.getDataSpecification().partitionGenerator.indexOf(clusteringKey.name));
        }

        casConditionArgFreqMap = new HashMap();
        for (final Integer oneConditionIndex : casConditionIndex)
        {
            casConditionArgFreqMap.put(oneConditionIndex, (int) Arrays.stream(schemaQuery.argumentIndex).filter((x) -> x == oneConditionIndex).count());
        }
    }

    private BoundStatement prepare(final Row row, final Object[] casDbValues, final Map<Integer, Integer> casConditionIndexOccurences)
    {
        final Map<Integer, Integer> localMapping = new HashMap<>(casConditionIndexOccurences);
        int conditionIndexTracker = 0;
        for (int i = 0; i < schemaQuery.argumentIndex.length; i++)
        {
            boolean replace = false;
            Integer count = localMapping.get(schemaQuery.argumentIndex[i]);
            if (count != null)
            {
                count--;
                localMapping.put(schemaQuery.argumentIndex[i], count);
                if (count == 0)
                {
                    replace = true;
                }
            }

            if (replace)
            {
                schemaQuery.bindBuffer[i] = casDbValues[conditionIndexTracker++];
            }
            else
            {
                Object value = row.get(schemaQuery.argumentIndex[i]);
                if (schemaQuery.definitions.getType(i).getName().equals(DataType.date().getName()))
                {
                    // the java driver only accepts com.datastax.driver.core.LocalDate for CQL type "DATE"
                    value = LocalDate.fromDaysSinceEpoch((Integer) value);
                }

                schemaQuery.bindBuffer[i] = value;
            }

            if (schemaQuery.bindBuffer[i] == null && !schemaQuery.getDataSpecification().partitionGenerator.permitNulls(schemaQuery.argumentIndex[i]))
            {
                throw new IllegalStateException();
            }
        }
        return schemaQuery.statement.bind(schemaQuery.bindBuffer);
    }

    BoundStatement bind(JavaDriverClient client)
    {
        //get current db values for all the coluns which are part of dynamic conditions
        ResultSet rs;
        if (schemaQuery.argSelect != SchemaQuery.ArgSelect.SAMEROW)
        {
            throw new IllegalArgumentException("CAS is supported only for type 'samerow'");
        }
        if (casReadConditionStatement == null)
        {
            synchronized (CASQueryDynamicConditions.this)
            {
                if (casReadConditionStatement == null)
                {
                    casReadConditionStatement = client.prepare(casReadConditionQuery.toString());
                }
            }
        }
        final Object keys[] = new Object[keysIndex.size()];
        final Row row = schemaQuery.getPartitions().get(0).next();

        for (int i = 0; i < keysIndex.size(); i++)
        {
            keys[i] = row.get(keysIndex.get(i));
        }

        //get cas columns' current value from db
        rs = client.getSession().execute(casReadConditionStatement.bind(keys));
        final Object casDbValues[] = new Object[casConditionArgFreqMap.size()];

        final com.datastax.driver.core.Row casDbValue = rs.one();
        if (casDbValue != null)
        {
            for (int i = 0; i < casConditionArgFreqMap.size(); i++)
            {
                casDbValues[i] = casDbValue.getObject(i);
            }
        }
        //now bind db values for dynamic conditions in actual CAS update operation
        return prepare(row,
                casDbValues,
                casConditionArgFreqMap);
    }
}