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

package org.apache.cassandra.service;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.InetAddressAndPort;

public class MetadataService
{
    private final Map<InetAddressAndPort, Map<String, String>> metadata = new ConcurrentHashMap<>();

    public static final MetadataService instance = new MetadataService();

    private MetadataService()
    {
    }

    Map<String, String> loadLocalMetadata()
    {
        String metadataFile = CassandraRelevantProperties.METADATA_FILE.getString();
        if (metadataFile == null)
            return Collections.emptyMap();

        File propertiesFile = new File(metadataFile);

        if (!propertiesFile.exists() || !propertiesFile.isFile() || !propertiesFile.isReadable())
            return Collections.emptyMap();

        try
        {
            Map props = FileUtils.readProperties(propertiesFile);
            return new HashMap<>((Map<String, String>) props);
        }
        catch (Exception ex)
        {
            throw new RuntimeException(String.format("Unable to load properties from %s", propertiesFile), ex);
        }
    }

    public Map<InetAddressAndPort, Map<String, String>> getAll()
    {
        return Collections.unmodifiableMap(metadata);
    }

    void addMetadata(InetAddressAndPort ep, Map<String, String> metadata, boolean gossip)
    {
        this.metadata.merge(ep, metadata, (map1, map2) -> {
            map1.putAll(map2);
            return map1;
        });

        if (gossip)
            Gossiper.instance.addLocalApplicationState(ApplicationState.METADATA,
                                                       StorageService.instance.valueFactory.metadata(metadata));
    }

    @VisibleForTesting
    public void addMetadata(InetAddressAndPort ep, Map<String, String> metadata)
    {
        addMetadata(ep, metadata, false);
    }

    void addMetadata(InetAddressAndPort ep, String mapAsString)
    {
        if (mapAsString == null || mapAsString.isEmpty())
            return;

        addMetadata(ep, Arrays.stream(mapAsString.split(","))
                              .map(e -> e.split("="))
                              .collect(Collectors.toMap(e -> e[0], e -> e[1])));
    }

    public Map<String, String> get(InetAddressAndPort ep)
    {
        return this.metadata.getOrDefault(ep, Collections.emptyMap());
    }

    public String getValue(InetAddressAndPort ep, String key)
    {
        return get(ep).get(key);
    }

    public void remove(InetAddressAndPort endpoint)
    {
        metadata.remove(endpoint);
    }
}
