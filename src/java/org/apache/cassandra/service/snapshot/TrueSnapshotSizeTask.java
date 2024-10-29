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

package org.apache.cassandra.service.snapshot;

import java.util.concurrent.Callable;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;

public class TrueSnapshotSizeTask implements Callable<Long>
{
    private final Predicate<TableSnapshot> predicate;
    private Supplier<Iterable<TableSnapshot>> snapshots;

    public TrueSnapshotSizeTask(Predicate<TableSnapshot> predicate,
                                Supplier<Iterable<TableSnapshot>> snapshots)
    {
        this.predicate = predicate;
        this.snapshots = snapshots;
    }

    @Override
    public Long call() throws Exception
    {
        long size = 0;
        for (TableSnapshot snapshot : snapshots.get())
        {
            if (predicate.test(snapshot))
            try
            {
                Keyspace keyspace = Keyspace.getValidKeyspace(snapshot.getKeyspaceName());
                ColumnFamilyStore table = keyspace.getColumnFamilyStore(snapshot.getTableName());
                size += snapshot.computeTrueSizeBytes(table.getFilesOfCfs());
            }
            catch (IllegalArgumentException ex)
            {
                // when not found, we do not include the size
            }
        }

        return size;
    }
}
