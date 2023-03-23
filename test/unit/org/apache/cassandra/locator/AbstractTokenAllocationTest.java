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

package org.apache.cassandra.locator;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.junit.BeforeClass;
import org.junit.Ignore;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.tokenallocator.OfflineTokenAllocator;
import org.apache.cassandra.service.metadata.MetadataService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;
import org.mockito.Mockito;

import static org.apache.cassandra.locator.InetAddressAndPort.getByAddress;
import static org.apache.cassandra.locator.SpotNetworkTopologyStrategy.DatacenterEndpoints.SPOT_NODE_TAG;
import static org.apache.cassandra.locator.SpotNetworkTopologyStrategy.DatacenterEndpoints.TAG_KEY_NAME;
import static org.junit.Assert.assertEquals;

@Ignore
public abstract class AbstractTokenAllocationTest
{
    public static int NUM_INSERTS = 1_000_000;
    public static int ALL_RACKS = 100;
    public static int ALL_NODES = 100;

    public static class TestingSnitch extends AbstractNetworkTopologySnitch
    {
        Map<InetAddressAndPort, NodeInfo> nodes = new HashMap<>();

        public TestingSnitch(Map<InetAddressAndPort, NodeInfo> nodes)
        {
            this.nodes.putAll(nodes);
        }

        @Override
        public String getRack(InetAddressAndPort endpoint)
        {
            return nodes.get(endpoint).rack;
        }

        @Override
        public String getDatacenter(InetAddressAndPort endpoint)
        {
            return nodes.get(endpoint).dc;
        }
    }

    public static class NodeInfo
    {
        String dc;
        String rack;
        Set<String> tag;

        public NodeInfo(String dc, String rack, String tag)
        {
            this.dc = dc;
            this.rack = rack;
            this.tag = Collections.singleton(tag);
        }

        public static NodeInfo create(String dc, String rack)
        {
            return create(dc, rack, null);
        }

        public static NodeInfo create(String dc, String rack, String tag)
        {
            return new NodeInfo(dc, rack, tag);
        }
    }

    private static final String DC1 = "dc1";
    private static String[] all_racks;
    private static InetAddressAndPort[] all_nodes;

    static
    {
        try
        {
            all_nodes = new InetAddressAndPort[ALL_NODES];
            for (int i = 0; i < all_nodes.length; i++)
                all_nodes[i] = getByAddress(new byte[]{ (byte) 172, 19, 0, (byte) i });

            all_racks = new String[ALL_RACKS];
            for (int i = 0; i < all_racks.length; i++)
                all_racks[i] = "rack" + (i + 1);
        }
        catch (Exception ex)
        {
            // ignore
        }
    }

    private static class TopologyBuilder
    {
        private Map<InetAddressAndPort, NodeInfo> nodes = new HashMap<>();

        public static TopologyBuilder create()
        {
            return new TopologyBuilder();
        }

        public TopologyBuilder add(String dc, String rack, InetAddressAndPort addr)
        {
            nodes.put(addr, NodeInfo.create(dc, rack));
            return this;
        }

        public TopologyBuilder add(String dc, String rack, InetAddressAndPort addr, boolean isSpot)
        {
            nodes.put(addr, NodeInfo.create(dc, rack));
            if (isSpot)
                MetadataService.instance.addMetadata(addr, new HashMap<String, String>()
                {{
                    put(TAG_KEY_NAME, SPOT_NODE_TAG);
                }});

            return this;
        }

        private TopologyBuilder()
        {
        }

        public Map<InetAddressAndPort, NodeInfo> build()
        {
            return Collections.unmodifiableMap(nodes);
        }
    }

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(new Murmur3Partitioner());
    }

    public static class ContextHolder
    {
        public IEndpointSnitch snitch;
        public TokenMetadata metadata;
        public Map<InetAddressAndPort, NodeInfo> topology;
    }

    public ContextHolder prepare(int racksCount, int nodesCount, int rf, int numTokens, Integer... spots)
    {
        MetadataService.instance.clear();

        TopologyBuilder topologyBuilder = TopologyBuilder.create();

        int nodesPerRack = nodesCount / racksCount;
        int nodeIndex = 0;

        List<Integer> spotsNodes = Arrays.asList(spots);

        for (int i = 0; i < racksCount; i++)
        {
            for (int j = nodeIndex; j < (nodeIndex + nodesPerRack); j++)
            {
                topologyBuilder.add(DC1, all_racks[i], all_nodes[j], spotsNodes.contains(j));
            }
            nodeIndex += nodesPerRack;
        }

        Map<InetAddressAndPort, NodeInfo> topology = topologyBuilder.build();

        IEndpointSnitch snitch = new TestingSnitch(topology);
        DatabaseDescriptor.setEndpointSnitch(snitch);

        /////////////////////////////

        ImmutableMultimap.Builder<String, InetAddressAndPort> builder = ImmutableMultimap.builder();

        for (int i = 0; i < nodesCount; i++)
        {
            builder.put(DC1, all_nodes[i]);
        }

        Multimap<String, InetAddressAndPort> datacenterEndpoints = builder.build();

        /////////////////////////////

        ImmutableMultimap.Builder<String, InetAddressAndPort> racksBuilder = ImmutableMultimap.<String, InetAddressAndPort>builder();

        nodeIndex = 0;
        for (int i = 0; i < racksCount; i++)
        {
            for (int j = nodeIndex; j < (nodeIndex + nodesPerRack); j++)
            {
                racksBuilder.put(all_racks[i], all_nodes[j]);
            }
            nodeIndex += nodesPerRack;
        }


        ImmutableMap<String, ImmutableMultimap<String, InetAddressAndPort>> racks = ImmutableMap.<String, ImmutableMultimap<String, InetAddressAndPort>>builder()
                                                                                                .put(DC1, racksBuilder.build()).build();

        /////////////////////////////

        TokenMetadata.Topology tokenMetadataTopology = Mockito.mock(TokenMetadata.Topology.class);
        Mockito.when(tokenMetadataTopology.getDatacenterEndpoints()).thenReturn(datacenterEndpoints);
        Mockito.when(tokenMetadataTopology.getDatacenterRacks()).thenReturn(racks);

        TokenMetadata metadata = new TokenMetadata(snitch);
        TokenMetadata spy = Mockito.spy(metadata);

        Mockito.when(spy.getTopology()).thenReturn(tokenMetadataTopology);

        /////////////////////////////

        metadata.cloneOnlyTokenMap();

        int[] allocatorRacks = new int[racksCount];
        Arrays.fill(allocatorRacks, nodesPerRack);

        List<OfflineTokenAllocator.FakeNode> allocation = OfflineTokenAllocator.allocate(rf, numTokens,
                                                                                         allocatorRacks,
                                                                                         new OutputHandler.SystemOutput(false, true, true),
                                                                                         FBUtilities.newPartitioner(Murmur3Partitioner.class.getSimpleName()));

        for (int i = 0; i < nodesCount; i++)
        {
            OfflineTokenAllocator.FakeNode fakeNode = allocation.get(i);
            metadata.updateNormalTokens(fakeNode.tokens(), all_nodes[i]);
        }

        /////////////////////////////

        ContextHolder holder = new ContextHolder();
        holder.topology = topology;
        holder.metadata = metadata;
        holder.snitch = snitch;

        return holder;
    }

    public long checkViolations(ContextHolder holder, AbstractReplicationStrategy strategy, int maxSpots)
    {
        Murmur3Partitioner partitioner = Murmur3Partitioner.instance;

        Map<String, Integer> placement = new HashMap<>();

        long numberOfViolations = 0;

        int max = (strategy.getReplicationFactor().fullReplicas / 2) + 1;

        for (long i = 0; i < NUM_INSERTS; i++)
        {
            int currentViolations = 0;
            placement.clear();

            Murmur3Partitioner.LongToken token = partitioner.getRandomToken();
            Set<InetAddressAndPort> replicas = strategy.getNaturalReplicasForToken(token).endpoints();

            assertEquals(strategy.getReplicationFactor().fullReplicas, replicas.size());

            for (InetAddressAndPort replica : replicas)
            {
                String rack = holder.topology.get(replica).rack;
                Integer replicasInRack = placement.getOrDefault(rack, 0);
                placement.put(rack, ++replicasInRack);
            }

            for (Map.Entry<String, Integer> placementEntry : placement.entrySet())
                if (placementEntry.getValue() >= max)
                {
                    ++currentViolations;
                }

            if (currentViolations != 0)
            {
                ++numberOfViolations;
                continue;
            }

            int spots = 0;

            for (InetAddressAndPort replica : replicas)
            {
                String tagValue = MetadataService.instance.getValue(replica, TAG_KEY_NAME);
                if (tagValue != null && tagValue.contains(SPOT_NODE_TAG))
                    ++spots;
            }

            if (spots > maxSpots)
            {
                ++numberOfViolations;
            }
        }

        return numberOfViolations;
    }

    public long checkMaxSpotsViolations(AbstractReplicationStrategy strategy, int maxSpots)
    {
        Murmur3Partitioner partitioner = Murmur3Partitioner.instance;

        long numberOfViolations = 0;

        for (long i = 0; i < NUM_INSERTS; i++)
        {
            Murmur3Partitioner.LongToken token = partitioner.getRandomToken();
            Set<InetAddressAndPort> replicas = strategy.getNaturalReplicasForToken(token).endpoints();

            assertEquals(strategy.getReplicationFactor().fullReplicas, replicas.size());

            int spots = 0;

            for (InetAddressAndPort replica : replicas)
            {
                String tagValue = MetadataService.instance.getValue(replica, TAG_KEY_NAME);
                if (tagValue != null && tagValue.contains(SPOT_NODE_TAG))
                    ++spots;
            }

            if (spots > maxSpots)
                ++numberOfViolations;
        }

        return numberOfViolations;
    }
}
