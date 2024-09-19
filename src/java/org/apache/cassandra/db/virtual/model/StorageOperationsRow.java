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

package org.apache.cassandra.db.virtual.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.utils.TimeUUID;

import static java.lang.String.format;

/**
 * Representation of operation for a {@link org.apache.cassandra.db.virtual.CollectionVirtualTableAdapter}.
 */
public class StorageOperationsRow
{
    private static final String UNDEFINED = "undefined";
    private final Map.Entry<TimeUUID, CompactionManager.OperationInfo> entry;

    public StorageOperationsRow(Map.Entry<TimeUUID, CompactionManager.OperationInfo> entry)
    {
        this.entry = entry;
    }

    @Column(type = Column.Type.PARTITION_KEY)
    public TimeUUID operationId()
    {
        return entry.getKey();
    }

    @Column
    public String operationType()
    {
        return entry.getValue().operationType.toString();
    }

    @Column
    public String keyspaces()
    {
        return String.join(", ", entry.getValue().keyspaceNames);
    }

    @Column
    public String tables()
    {
        return entry.getValue().sstableOperationStatusPerKeyspace().keySet().toString();
    }

    @Column
    public String operationResult()
    {
        CompactionManager.AllSSTableOpStatus status = entry.getValue().operationResult();
        return status == null ? UNDEFINED : status.toString();
    }

    @Column
    public String operationResultByTable()
    {
        return formatMap(entry.getValue().sstableOperationStatusPerKeyspace());
    }

    @Column
    public String processedByKeyspace()
    {
        return formatMap(entry.getValue().processedSSTablesPerKeyspace());
    }

    private static String formatMap(Map<?, ?> map)
    {
        List<String> result = new ArrayList<>(map == null || map.isEmpty() ? 1 : map.size());
        if (map == null || map.isEmpty())
            result.add(UNDEFINED);
        else
            for (Map.Entry<?, ?> e : map.entrySet())
                result.add(format("%s: %s", e.getKey(), formatNull(e.getValue())));

        return format("[%s]", String.join(", ", result));
    }

    private static String formatNull(Object value)
    {
        return value == null ? UNDEFINED : value.toString();
    }
}
