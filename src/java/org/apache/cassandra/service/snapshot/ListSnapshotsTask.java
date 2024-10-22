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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;

public class ListSnapshotsTask implements Callable<Map<String, TabularData>>
{
    private static final Logger logger = LoggerFactory.getLogger(ListSnapshotsTask.class);

    private final Map<String, String> options;

    public ListSnapshotsTask(Map<String, String> options)
    {
        this.options = options;
    }

    @Override
    public Map<String, TabularData> call()
    {
        boolean skipExpiring = options != null && Boolean.parseBoolean(options.getOrDefault("no_ttl", "false"));
        boolean includeEphemeral = options != null && Boolean.parseBoolean(options.getOrDefault("include_ephemeral", "false"));

        Map<String, TabularData> snapshotMap = new HashMap<>();

        Set<String> tags = new HashSet<>();

        List<TableSnapshot> snapshots = SnapshotManager.instance.getSnapshots(s -> {
            if (skipExpiring && s.isExpiring())
                return false;

            if (!includeEphemeral && s.isEphemeral())
                return false;

            return true;
        });

        for (TableSnapshot t : snapshots)
            tags.add(t.getTag());

        for (String tag : tags)
            snapshotMap.put(tag, new TabularDataSupport(SnapshotDetailsTabularData.TABULAR_TYPE));

        Map<String, Set<String>> keyspaceTables = new HashMap<>();
        for (TableSnapshot s : snapshots)
        {
            keyspaceTables.computeIfAbsent(s.getKeyspaceName(), ignore -> new HashSet<>());
            keyspaceTables.get(s.getKeyspaceName()).add(s.getTableName());
        }

        Map<String, Set<String>> cfsFiles = new HashMap<>();

        for (Map.Entry<String, Set<String>> entry : keyspaceTables.entrySet())
        {
            for (String table : entry.getValue())
            {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(entry.getKey(), table);
                if (cfs == null)
                    continue;

                try
                {
                    cfsFiles.put(cfs.getKeyspaceName() + '.' + cfs.name, cfs.getFilesOfCfs());
                }
                catch (Throwable t)
                {
                    logger.debug("Unable to get all files of live SSTables for {}.{}", cfs.getKeyspaceName(), cfs.name);
                }
            }
        }

        for (TableSnapshot snapshot : snapshots)
        {
            TabularDataSupport data = (TabularDataSupport) snapshotMap.get(snapshot.getTag());
            SnapshotDetailsTabularData.from(snapshot, data, cfsFiles.get(snapshot.getKeyspaceTable()));
        }

        return snapshotMap;
    }
}
