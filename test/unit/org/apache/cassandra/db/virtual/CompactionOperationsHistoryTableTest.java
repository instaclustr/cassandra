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

package org.apache.cassandra.db.virtual;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.schema.SchemaConstants;

import static org.apache.cassandra.tools.ToolRunner.invokeNodetool;

/**
 * Test for the compaction_operations_history virtual table, which is a view over the compaction operations history.
 * @see org.apache.cassandra.db.CleanupTest
 */
public class CompactionOperationsHistoryTableTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        startJMXServer();
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(SchemaConstants.VIRTUAL_VIEWS,
                                                                      SystemViewsKeyspace.instance.tables()));
    }

    @Test
    public void testCleanup()
    {
        CompactionManager.instance.disableAutoCompaction();

        createTable("CREATE TABLE %s (pk text, ck1 int, ck2 int, v int, PRIMARY KEY (pk, ck1, ck2))");
        for (int i = 0; i < 10; i++)
            execute("insert into %s (pk, ck1, ck2, v) values (?, ?, ?, ?)", "key", i, i, i);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        invokeNodetool("cleanup", "--jobs", "2", cfs.keyspace.getName(), cfs.getTableName())
            .assertOnCleanExit();

        assertRows(execute(String.format("SELECT operation_type, keyspaces, tables, operation_result, " +
                                         "operation_result_by_table, processed_by_keyspace " +
                                         "FROM %s.%s", SchemaConstants.VIRTUAL_VIEWS,
                                         SystemViewsKeyspace.COMPACTION_OPERATIONS_HISTORY)),
                   row(OperationType.CLEANUP.toString(),
                       KEYSPACE,
                       String.format("[%s.%s]", cfs.getKeyspaceName(), cfs.getTableName()),
                       CompactionManager.AllSSTableOpStatus.SUCCESSFUL.toString(),
                       "[cql_test_keyspace.table_testcleanup_00: SUCCESSFUL]",
                       "[cql_test_keyspace: 0]"));
    }
}
