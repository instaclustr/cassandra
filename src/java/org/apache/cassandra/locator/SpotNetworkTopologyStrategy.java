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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.dht.Datacenters;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.metadata.MetadataService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.locator.SpotNetworkTopologyStrategy.DatacenterEndpoints.SPOT_NODE_TAG;
import static org.apache.cassandra.locator.SpotNetworkTopologyStrategy.DatacenterEndpoints.TAG_KEY_NAME;

public class SpotNetworkTopologyStrategy extends AbstractReplicationStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(SpotNetworkTopologyStrategy.class);

    public static final String REPLICATION_FACTOR = "replication_factor";
    public static final String MAX_SPOT_REPLICAS_KEY = "max_spots";
    public static final String ENSURE_QUORUM_KEY = "ensure_quorum_not_spots";
    public static final int DEFAULT_MAX_SPOT_REPLICAS = 1;

    private final Map<String, ReplicationFactor> datacenters;
    private final ReplicationFactor aggregateRf;

    private final int maxSpotReplicas;
    private final boolean enusureQuorumPerDcNotSpots;

    public SpotNetworkTopologyStrategy(String keyspaceName,
                                       TokenMetadata tokenMetadata,
                                       IEndpointSnitch snitch,
                                       String... configOptions)
    {
        this(keyspaceName, tokenMetadata, snitch, toMap(configOptions));
    }

    public SpotNetworkTopologyStrategy(String keyspaceName,
                                       TokenMetadata tokenMetadata,
                                       IEndpointSnitch snitch,
                                       Map<String, String> configOptions) throws ConfigurationException
    {
        super(keyspaceName, tokenMetadata, snitch, filterDatacenterOptions(configOptions));

        int replicas = 0;
        int trans = 0;
        Map<String, ReplicationFactor> newDatacenters = new HashMap<>();
        if (configOptions != null)
        {
            for (Map.Entry<String, String> entry : filterDatacenterOptions(configOptions).entrySet())
            {
                String dc = entry.getKey();
                // prepareOptions should have transformed any "replication_factor" options by now
                if (dc.equalsIgnoreCase(REPLICATION_FACTOR))
                    throw new ConfigurationException(REPLICATION_FACTOR + " should not appear as an option at construction time for NetworkTopologyStrategy");
                ReplicationFactor rf = ReplicationFactor.fromString(entry.getValue());
                replicas += rf.allReplicas;
                trans += rf.transientReplicas();
                newDatacenters.put(dc, rf);
            }
        }

        datacenters = Collections.unmodifiableMap(newDatacenters);
        aggregateRf = ReplicationFactor.withTransient(replicas, trans);

        maxSpotReplicas = parseMaxSpotReplicas(configOptions, datacenters);
        enusureQuorumPerDcNotSpots = parseEnusureQuorumPerDcNotSpots(configOptions);

        if (enusureQuorumPerDcNotSpots && maxSpotReplicas > DEFAULT_MAX_SPOT_REPLICAS)
            throw new IllegalStateException(String.format("You can not use '%s' set to 'true' together with '%s' bigger than 1",
                                                          ENSURE_QUORUM_KEY,
                                                          MAX_SPOT_REPLICAS_KEY));

        logger.info("Configured datacenter for replicas are {}, with {}={},{}={}" +
                    FBUtilities.toString(datacenters),
                    MAX_SPOT_REPLICAS_KEY,
                    maxSpotReplicas,
                    ENSURE_QUORUM_KEY,
                    enusureQuorumPerDcNotSpots);
    }

    @Override
    public Map<String, String> getConfigOptions()
    {
        Map<String, String> options = new HashMap<>(super.getConfigOptions());

        options.put(MAX_SPOT_REPLICAS_KEY, Integer.toString(maxSpotReplicas));
        options.put(ENSURE_QUORUM_KEY, Boolean.toString(enusureQuorumPerDcNotSpots));

        return ImmutableMap.copyOf(options);
    }

    @Override
    public Collection<String> recognizedOptions()
    {
        Set<String> recognized = new HashSet<>(Datacenters.getValidDatacenters());
        recognized.add(MAX_SPOT_REPLICAS_KEY);
        recognized.add(ENSURE_QUORUM_KEY);
        return recognized;
    }

    private static Map<String, String> toMap(Object[] opts)
    {
        if (opts.length % 2 != 0)
            throw new IllegalStateException("Number of all options has to have even.");

        if (opts.length == 0)
            return Collections.emptyMap();

        Map<String, String> result = new HashMap<>();

        for (int i = 0; i < opts.length - 1; i += 2)
            result.put(opts[i].toString(), opts[i + 1].toString());

        return result;
    }

    private static Map<String, String> filterDatacenterOptions(Map<String, String> configOptions)
    {
        HashMap<String, String> datacentersOnly = new HashMap<>(configOptions);
        datacentersOnly.remove(MAX_SPOT_REPLICAS_KEY);
        datacentersOnly.remove(ENSURE_QUORUM_KEY);
        return datacentersOnly;
    }

    private static int parseMaxSpotReplicas(Map<String, String> configOptions,
                                            Map<String, ReplicationFactor> datacenters)
    {
        int parsedValue = parseMaxSpotReplicasInternal(configOptions);
        for (Map.Entry<String, ReplicationFactor> datacenter : datacenters.entrySet())
        {
            ReplicationFactor rf = datacenter.getValue();

            if (rf.fullReplicas <= parsedValue)
            {
                // min rf can be 1 anyway
                return DEFAULT_MAX_SPOT_REPLICAS;
            }
        }

        return parsedValue;
    }

    private static int parseMaxSpotReplicasInternal(Map<String, String> configOptions)
    {
        if (configOptions == null)
            return DEFAULT_MAX_SPOT_REPLICAS;

        String mapValue = configOptions.getOrDefault(MAX_SPOT_REPLICAS_KEY, "1");
        try
        {
            int parsedValue = Integer.parseInt(mapValue);
            return parsedValue < 0 ? DEFAULT_MAX_SPOT_REPLICAS : parsedValue;
        }
        catch (NumberFormatException ex)
        {
            logger.warn("Unable to parse {}, using default value {}", MAX_SPOT_REPLICAS_KEY, DEFAULT_MAX_SPOT_REPLICAS);
            return DEFAULT_MAX_SPOT_REPLICAS;
        }
    }

    private boolean parseEnusureQuorumPerDcNotSpots(@Nullable Map<String, String> configOptions)
    {
        if (configOptions == null)
            return false;

        return Boolean.parseBoolean(configOptions.getOrDefault(ENSURE_QUORUM_KEY, "false"));
    }

    /**
     * calculate endpoints in one pass through the tokens by tracking our progress in each DC.
     */
    @Override
    public EndpointsForRange calculateNaturalReplicas(Token searchToken, TokenMetadata tokenMetadata)
    {
        // we want to preserve insertion order so that the first added endpoint becomes primary
        ArrayList<Token> sortedTokens = tokenMetadata.sortedTokens();
        Token replicaEnd = TokenMetadata.firstToken(sortedTokens, searchToken);
        Token replicaStart = tokenMetadata.getPredecessor(replicaEnd);
        Range<Token> replicatedRange = new Range<>(replicaStart, replicaEnd);

        EndpointsForRange.Builder builder = new EndpointsForRange.Builder(replicatedRange);
        Set<Pair<String, String>> seenRacks = new HashSet<>();

        TokenMetadata.Topology topology = tokenMetadata.getTopology();

        // all endpoints in each DC, so we can check when we have exhausted all the members of a DC
        Multimap<String, InetAddressAndPort> allEndpoints = topology.getDatacenterEndpoints();

        // all racks in a DC so we can check when we have exhausted all racks in a DC
        Map<String, ImmutableMultimap<String, InetAddressAndPort>> racks = topology.getDatacenterRacks();

        assert !allEndpoints.isEmpty() && !racks.isEmpty() : "not aware of any cluster members";

        Map<InetAddressAndPort, Map<String, String>> metadata = MetadataService.instance.getAll();

        int dcsToFill = 0;
        Map<String, DatacenterEndpoints> dcs = new HashMap<>(datacenters.size() * 2);

        // Create a DatacenterEndpoints object for each non-empty DC.
        // map of <dc, rf>, like <dc1, 3>, <dc2, 5>
        for (Map.Entry<String, ReplicationFactor> en : datacenters.entrySet())
        {
            String dc = en.getKey();
            ReplicationFactor rf = en.getValue();

            // number of nodes in a particular dc
            int nodeCount = sizeOrZero(allEndpoints.get(dc));

            if (rf.allReplicas <= 0 || nodeCount <= 0)
                continue;

            Set<InetAddressAndPort> spotsInDc = metadata.entrySet()
                                                        .stream()
                                                        .filter(e -> e.getValue().getOrDefault(TAG_KEY_NAME, "").contains(SPOT_NODE_TAG))
                                                        .filter(e -> allEndpoints.get(dc).contains(e.getKey()))
                                                        .map(Map.Entry::getKey)
                                                        .collect(Collectors.toSet());

            DatacenterEndpoints dcEndpoints = new DatacenterEndpoints(rf,
                                                                      sizeOrZero(racks.get(dc)),
                                                                      nodeCount,
                                                                      builder,
                                                                      seenRacks,
                                                                      spotsInDc,
                                                                      maxSpotReplicas,
                                                                      enusureQuorumPerDcNotSpots);
            dcs.put(dc, dcEndpoints);
            ++dcsToFill;
        }

        Iterator<Token> tokenIter = TokenMetadata.ringIterator(sortedTokens, searchToken, false);
        while (dcsToFill > 0 && tokenIter.hasNext())
        {
            Token next = tokenIter.next();
            InetAddressAndPort ep = tokenMetadata.getEndpoint(next);
            // datacenter, rack
            Pair<String, String> dcRackOfEp = topology.getLocation(ep);
            // get datacenter endpoints of the node responsible for next token in the ring
            DatacenterEndpoints dcEndpoints = dcs.get(dcRackOfEp.left);
            if (dcEndpoints != null && dcEndpoints.addEndpointAndCheckIfDone(ep, dcRackOfEp, replicatedRange))
                --dcsToFill;
        }
        return builder.build();
    }

    protected int sizeOrZero(Multimap<?, ?> collection)
    {
        return collection != null ? collection.asMap().size() : 0;
    }

    protected int sizeOrZero(Collection<?> collection)
    {
        return collection != null ? collection.size() : 0;
    }

    @Override
    public ReplicationFactor getReplicationFactor()
    {
        return aggregateRf;
    }

    public ReplicationFactor getReplicationFactor(String dc)
    {
        ReplicationFactor replicas = datacenters.get(dc);
        return replicas == null ? ReplicationFactor.ZERO : replicas;
    }

    @Override
    public void validateOptions() throws ConfigurationException
    {
        for (Map.Entry<String, String> e : this.configOptions.entrySet())
        {
            // prepareOptions should have transformed any "replication_factor" by now
            if (e.getKey().equalsIgnoreCase(REPLICATION_FACTOR))
                throw new ConfigurationException(REPLICATION_FACTOR + " should not appear as an option to NetworkTopologyStrategy");
            validateReplicationFactor(e.getValue());
        }
    }

    @Override
    public boolean hasSameSettings(AbstractReplicationStrategy other)
    {
        return super.hasSameSettings(other) && ((SpotNetworkTopologyStrategy) other).maxSpotReplicas == maxSpotReplicas;
    }

    /**
     * Endpoint adder applying the replication rules for a given DC.
     */
    @VisibleForTesting
    public static final class DatacenterEndpoints
    {
        public static final String SPOT_NODE_TAG = "spot";
        public static final String TAG_KEY_NAME = "tags";

        /**
         * List accepted endpoints get pushed into.
         */
        EndpointsForRange.Builder endpointsForRange;

        /**
         * Racks encountered so far. Replicas are put into separate racks while possible.
         * For efficiency the set is shared between the instances, using the location pair (dc, rack) to make sure
         * clashing names aren't a problem.
         */
        Set<Pair<String, String>> seenRacks;

        Set<InetAddressAndPort> spotNodes;

        /**
         * Number of replicas left to fill from this DC.
         */
        int rfLeft;
        int acceptableRackRepeats;
        int transients;

        /**
         * Number of replicas which can be spots, the figure
         * is lowered every time we place a replica to a spot node.
         * When 0 is reached, we can not place replica to any spot anymore.
         */
        int availableSpotReplicas;
        /**
         * Number of replicas which have to be normal, not spots
         */
        int minimumRequiredNormalReplicas;

        DatacenterEndpoints(ReplicationFactor rf,
                            int rackCount,
                            int nodeCount,
                            EndpointsForRange.Builder endpointsForRange,
                            Set<Pair<String, String>> seenRacks,
                            Set<InetAddressAndPort> spotsInDc,
                            int maxSpotReplicas,
                            boolean enusureQuorumPerDcNotSpots)
        {
            this.endpointsForRange = endpointsForRange;
            this.seenRacks = seenRacks;
            // If there aren't enough nodes in this DC to fill the RF, the number of nodes is the effective RF.
            this.rfLeft = Math.min(rf.allReplicas, nodeCount);

            // If there aren't enough racks in this DC to fill the RF, we'll still use at least one node from each rack,
            // and the difference is to be filled by the first encountered nodes.
            acceptableRackRepeats = rf.allReplicas - rackCount;

            // if we have fewer replicas than rf calls for, reduce transients accordingly
            int reduceTransients = rf.allReplicas - this.rfLeft;
            transients = Math.max(rf.transientReplicas() - reduceTransients, 0);
            ReplicationFactor.validate(rfLeft, transients);

            spotNodes = spotsInDc;
            availableSpotReplicas = maxSpotReplicas;

            if (enusureQuorumPerDcNotSpots)
                minimumRequiredNormalReplicas = rfLeft % 2 == 1 ? (rfLeft + 1) / 2 : (rfLeft / 2) + 1;
            else
                minimumRequiredNormalReplicas = 0;

            if (rfLeft - maxSpotReplicas > minimumRequiredNormalReplicas)
                minimumRequiredNormalReplicas = rfLeft - maxSpotReplicas;
        }

        private boolean maybeAddReplica(Replica replica, boolean isSpot)
        {
            if (isSpot && availableSpotReplicas == 0)
                return false;

            try
            {
                endpointsForRange.add(replica, ReplicaCollection.Builder.Conflict.NONE);

                --rfLeft;
                if (isSpot)
                    --availableSpotReplicas;
                else
                    --minimumRequiredNormalReplicas;
            }
            catch (Exception ex)
            {
                return false;
            }

            return true;
        }

        /**
         * Attempts to add an endpoint to the replicas for this datacenter, adding to the replicas set if successful.
         * Returns true if the endpoint was added, and this datacenter does not require further replicas.
         */
        boolean addEndpointAndCheckIfDone(InetAddressAndPort ep, Pair<String, String> location, Range<Token> replicatedRange)
        {
            if (done())
                return false;

            // Cannot repeat a node.
            if (endpointsForRange.endpoints().contains(ep))
                return false;

            boolean isSpot = isSpot(ep);

            // fill dc with enough "normal" nodes firstly
            if (isSpot)
            {
                if (minimumRequiredNormalReplicas != 0)
                    return false;
                else if (availableSpotReplicas == 0)
                    // do not use more that allowed number of spots
                    return false;
            }

            Replica replica = new Replica(ep, replicatedRange, rfLeft > transients);

            if (seenRacks.add(location))
            {
                maybeAddReplica(replica, isSpot);
                return done();
            }
            if (acceptableRackRepeats <= 0)
                // There must be rfLeft distinct racks left, do not add any more rack repeats.
                return false;

            if (maybeAddReplica(replica, isSpot))
            {
                // Added a node that is from an already met rack to match RF when there aren't enough racks.
                --acceptableRackRepeats;
            }
            return done();
        }

        boolean done()
        {
            assert rfLeft >= 0;
            return rfLeft == 0;
        }

        private boolean isSpot(InetAddressAndPort ep)
        {
            return spotNodes.contains(ep);
        }
    }

    @Override
    public void maybeWarnOnOptions(ClientState state)
    {
        if (!SchemaConstants.isSystemKeyspace(keyspaceName))
        {
            ImmutableMultimap<String, InetAddressAndPort> dcsNodes = Multimaps.index(StorageService.instance.getTokenMetadata().getAllMembers(), snitch::getDatacenter);
            for (Map.Entry<String, String> e : this.configOptions.entrySet())
            {

                String dc = e.getKey();
                ReplicationFactor rf = getReplicationFactor(dc);
                Guardrails.minimumReplicationFactor.guard(rf.fullReplicas, keyspaceName, false, state);
                Guardrails.maximumReplicationFactor.guard(rf.fullReplicas, keyspaceName, false, state);
                int nodeCount = dcsNodes.get(dc).size();
                // nodeCount==0 on many tests
                if (rf.fullReplicas > nodeCount && nodeCount != 0)
                {
                    String msg = "Your replication factor " + rf.fullReplicas
                                 + " for keyspace "
                                 + keyspaceName
                                 + " is higher than the number of nodes "
                                 + nodeCount
                                 + " for datacenter "
                                 + dc;
                    ClientWarn.instance.warn(msg);
                    logger.warn(msg);
                }
            }
        }
    }
}
