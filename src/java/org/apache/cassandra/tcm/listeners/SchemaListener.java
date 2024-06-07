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

package org.apache.cassandra.tcm.listeners;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaDiagnostics;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Location;

public class SchemaListener implements ChangeListener
{
    private static final Logger logger = LoggerFactory.getLogger(SchemaListener.class);

    private final boolean loadSSTables;

    public SchemaListener(boolean loadSSTables)
    {
        this.loadSSTables = loadSSTables;
    }

    @Override
    public void notifyPreCommit(ClusterMetadata prev, ClusterMetadata next, boolean fromSnapshot)
    {
        notifyInternal(prev, next, fromSnapshot, loadSSTables);
    }

    protected void notifyInternal(ClusterMetadata prev, ClusterMetadata next, boolean fromSnapshot, boolean loadSSTables)
    {
        if (!fromSnapshot && next.schema.lastModified().equals(prev.schema.lastModified()))
            return;
        next.schema.initializeKeyspaceInstances(prev.schema, loadSSTables);
    }

    @Override
    public void notifyPostCommit(ClusterMetadata prev, ClusterMetadata next, boolean fromSnapshot)
    {
        if (!fromSnapshot && next.schema.lastModified().equals(prev.schema.lastModified()))
            return;

        DistributedSchema.maybeRebuildViews(prev.schema, next.schema);
        SchemaDiagnostics.versionUpdated(Schema.instance);
        Gossiper.instance.addLocalApplicationState(ApplicationState.SCHEMA, StorageService.instance.valueFactory.schema(next.schema.getVersion()));
        SystemKeyspace.updateSchemaVersion(next.schema.getVersion());
        Location myLocation = next.directory.myLocation();
        if (myLocation != null)
            removeAvailableRanges(prev, next, myLocation);
    }

    private void removeAvailableRanges(ClusterMetadata prev, ClusterMetadata next, Location myLocation)
    {
        Keyspaces previousKeyspaces = prev.schema.getKeyspaces();
        Keyspaces nextKeyspaces = next.schema.getKeyspaces();

        Iterator<KeyspaceMetadata> previousKeyspacesIterator = previousKeyspaces.iterator();
        Map<String, Set<String>> keyspaceDcsForRemoval = new HashMap<>();

        String myDc = myLocation.datacenter;
        logger.info("my datacenter: {}", myDc);

        while (previousKeyspacesIterator.hasNext())
        {
            KeyspaceMetadata previousKeyspaceMetadata = previousKeyspacesIterator.next();
            Optional<KeyspaceMetadata> maybeNextKeyspaceMetadata = nextKeyspaces.get(previousKeyspaceMetadata.name);
            if (maybeNextKeyspaceMetadata.isPresent())
            {
                Set<String> dcsForRemoval = getDcsToRemove(previousKeyspaceMetadata, maybeNextKeyspaceMetadata.get());
                if (!dcsForRemoval.isEmpty())
                    keyspaceDcsForRemoval.put(previousKeyspaceMetadata.name, dcsForRemoval);
            }
            else // if next metadata does not contain such keyspace, we clearly dropped that
            {
                keyspaceDcsForRemoval.put(previousKeyspaceMetadata.name, ImmutableSet.of(myDc));
            }
        }

        for (Map.Entry<String, Set<String>> entry : keyspaceDcsForRemoval.entrySet())
        {
            String keyspace = entry.getKey();
            for (String removedDc : entry.getValue())
            {
                if (myDc.equals(removedDc))
                {
                    logger.info("Removing available ranges for keyspace {}", keyspace);
                    SystemKeyspace.resetAvailableStreamedRangesForKeyspace(keyspace);
                }
            }
        }
    }

    private static Set<String> getDcsToRemove(KeyspaceMetadata previousKeyspaceMetadata, KeyspaceMetadata maybeNextKeyspaceMetadata)
    {
        ImmutableMap<String, String> oldParams = previousKeyspaceMetadata.params.replication.options;
        ImmutableMap<String, String> newParams = maybeNextKeyspaceMetadata.params.replication.options;

        logger.info("keyspace: {}, old params: {}, new params: {}", previousKeyspaceMetadata.name, oldParams, newParams);

        Set<String> dcsForRemoval = new HashSet<>();
        if (newParams.size() < oldParams.size()) // we are removing some dc
            for (Map.Entry<String, String> dcReplication : oldParams.entrySet())
                if (!newParams.containsKey(dcReplication.getKey()))
                    dcsForRemoval.add(dcReplication.getKey());

        logger.info("dcs for removal for keyspace {}: {}", previousKeyspaceMetadata.name, dcsForRemoval);

        return dcsForRemoval;
    }
}
