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

package org.apache.cassandra.distributed.test;

import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * In-JVM dtests for negotiable internode compression (COMPRESSED framing, CASSANDRA-20488).
 *
 * The framing of a connection is decided by the initiating side each time a connection is
 * (re-)established, and is logged by both peers:
 *  - initiator: "... successfully connected, version = ..., framing = ..."
 *  - acceptor:  "... messaging connection established, version = ..., framing = ..."
 *
 * The tests interrupt outbound connections and drive traffic to force fresh connection attempts,
 * then assert on the framing recorded in the logs.
 *
 * NOTE: assertions are deliberately anchored on node2's log only. Empirically, the per-node log
 * sifting of the in-JVM dtest framework does not capture the messaging event-loop log lines of
 * node1, while node2's log reliably contains both its outbound ("successfully connected") and its
 * inbound ("messaging connection established") lines - which together cover both directions.
 */
public class InternodeCompressionNegotiationTest extends TestBaseImpl
{
    private static final Map<String, Object> ZSTD_CONFIG = ImmutableMap.of("class_name", "ZstdCompressor");

    private static final String OUTBOUND_COMPRESSED = "successfully connected.*framing = COMPRESSED";
    private static final String INBOUND_COMPRESSED = "messaging connection established.*framing = COMPRESSED";
    private static final String OUTBOUND_LEGACY = "successfully connected.*framing = (LZ4|CRC)";

    @Test
    public void negotiatesCompressedFramingWhenBothConfigured() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(c -> c.with(NETWORK, GOSSIP)
                                                             .set("internode_compression", "all")
                                                             .set("internode_compression_config", ZSTD_CONFIG))
                                           .start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".t (k int PRIMARY KEY, v text)");
            writeAndReadEverywhere(cluster, 0);

            long mark = cluster.get(2).logs().mark();

            // force fresh connections (the peers' messaging versions are known by now)
            interruptOutbound(cluster, 1, 2);
            interruptOutbound(cluster, 2, 1);
            writeAndReadEverywhere(cluster, 1000);

            // node2 -> node1 negotiated COMPRESSED (initiator side)
            awaitLog(cluster.get(2), mark, OUTBOUND_COMPRESSED);
            // node1 -> node2 negotiated COMPRESSED (acceptor side)
            awaitLog(cluster.get(2), mark, INBOUND_COMPRESSED);
        }
    }

    @Test
    public void unconfiguredPeerStillAcceptsAndNeverInitiates() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(c -> {
                                               c.with(NETWORK, GOSSIP)
                                                .set("internode_compression", "all");
                                               // only node1 opts in; node2 keeps the default
                                               if (c.num() == 1)
                                                   c.set("internode_compression_config", ZSTD_CONFIG);
                                           })
                                           .start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".t (k int PRIMARY KEY, v text)");
            writeAndReadEverywhere(cluster, 0);

            long mark = cluster.get(2).logs().mark();

            interruptOutbound(cluster, 1, 2);
            interruptOutbound(cluster, 2, 1);
            writeAndReadEverywhere(cluster, 1000);

            // node1 (configured) initiates COMPRESSED; node2 accepts and decodes it while itself
            // having no internode_compression_config at all
            awaitLog(cluster.get(2), mark, INBOUND_COMPRESSED);
            // node2 (unconfigured) reconnected with legacy framing and never initiates COMPRESSED
            awaitLog(cluster.get(2), mark, OUTBOUND_LEGACY);
            assertThat(cluster.get(2).logs().grep(mark, OUTBOUND_COMPRESSED).getResult()).isEmpty();
        }
    }

    @Test
    public void storageCompatibilityModeKeepsLegacyFraming() throws Throwable
    {
        // in CASSANDRA_4 storage compatibility mode nodes advertise (and run at) a messaging version
        // below the one that introduced COMPRESSED framing, so the version gate must keep every
        // connection on legacy framing even though internode_compression_config is set
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(c -> c.with(NETWORK, GOSSIP)
                                                             .set("internode_compression", "all")
                                                             .set("internode_compression_config", ZSTD_CONFIG)
                                                             .set("storage_compatibility_mode", "CASSANDRA_4"))
                                           .start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".t (k int PRIMARY KEY, v text)");
            writeAndReadEverywhere(cluster, 0);

            long mark = cluster.get(2).logs().mark();

            interruptOutbound(cluster, 1, 2);
            interruptOutbound(cluster, 2, 1);
            writeAndReadEverywhere(cluster, 1000);

            // reconnections happened, with legacy framing (positive control) ...
            awaitLog(cluster.get(2), mark, OUTBOUND_LEGACY);
            // ... and COMPRESSED framing must never appear, in either role
            assertThat(cluster.get(2).logs().grep(mark, OUTBOUND_COMPRESSED).getResult()).isEmpty();
            assertThat(cluster.get(2).logs().grep(mark, INBOUND_COMPRESSED).getResult()).isEmpty();
        }
    }

    @Test
    public void dcScopeCompressesOnlyCrossDcConnections() throws Throwable
    {
        // the flagship configuration: internode_compression: dc + a configured compressor means
        // COMPRESSED framing exactly on cross-DC connections, while same-DC connections stay on
        // the uncompressed CRC framing
        try (Cluster cluster = init(Cluster.build()
                                           .withRacks(2, 1, 2) // 2 DCs, 1 rack each, 2 nodes per rack
                                           .withConfig(c -> c.with(NETWORK, GOSSIP)
                                                             .set("internode_compression", "dc")
                                                             .set("internode_compression_config", ZSTD_CONFIG))
                                           .start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".t (k int PRIMARY KEY, v text)");
            writeAndReadEverywhere(cluster, 0);

            // resolve the topology at runtime rather than assuming the node-to-DC distribution
            String dcOfNode2 = cluster.get(2).config().localDatacenter();
            int sameDcPeer = -1, crossDcPeer = -1;
            for (int n : new int[]{ 1, 3, 4 })
            {
                if (cluster.get(n).config().localDatacenter().equals(dcOfNode2))
                    sameDcPeer = n;
                else
                    crossDcPeer = n;
            }
            assertThat(sameDcPeer).isNotEqualTo(-1);
            assertThat(crossDcPeer).isNotEqualTo(-1);
            String sameDcIp = address(cluster, sameDcPeer);
            String crossDcIp = address(cluster, crossDcPeer);

            long mark2 = cluster.get(2).logs().mark();
            long markCross = cluster.get(crossDcPeer).logs().mark();

            interruptOutbound(cluster, 2, sameDcPeer);
            interruptOutbound(cluster, 2, crossDcPeer);
            interruptOutbound(cluster, crossDcPeer, 2);
            writeAndReadEverywhere(cluster, 1000);

            // NOTE: connection ids look like "/local:port(/localBind:ephemeral)->/remote:port-TYPE-...",
            // so patterns must anchor on the remote side ("->/ip:") - a bare ip would also match the
            // local ephemeral bind address embedded in every id
            // node2's cross-DC connection negotiated COMPRESSED, initiator and acceptor side
            awaitLog(cluster.get(2), mark2, "->/" + crossDcIp + ":.*successfully connected.*framing = COMPRESSED");
            awaitLog(cluster.get(crossDcPeer), markCross, "messaging connection established.*framing = COMPRESSED");

            // node2's same-DC connection reconnected on the uncompressed CRC framing, and never COMPRESSED
            awaitLog(cluster.get(2), mark2, "->/" + sameDcIp + ":.*successfully connected.*framing = CRC");
            assertThat(cluster.get(2).logs().grep(mark2, "->/" + sameDcIp + ":.*successfully connected.*framing = (COMPRESSED|LZ4)").getResult()).isEmpty();
        }
    }

    private static String address(Cluster cluster, int node)
    {
        return cluster.get(node).config().broadcastAddress().getAddress().getHostAddress();
    }

    private static void writeAndReadEverywhere(Cluster cluster, int base)
    {
        for (int i = base; i < base + 50; i++)
            cluster.coordinator(1 + (i % 2))
                   .execute("INSERT INTO " + KEYSPACE + ".t (k, v) VALUES (?, ?)", ConsistencyLevel.ALL, i, "v" + i);

        // read each key through the coordinator that did not write it, at ALL, to force internode reads
        for (int i = base; i < base + 50; i++)
            assertRows(cluster.coordinator(1 + ((i + 1) % 2))
                              .execute("SELECT v FROM " + KEYSPACE + ".t WHERE k = ?", ConsistencyLevel.ALL, i),
                       row("v" + i));
    }

    /** wait for the (asynchronously flushed) log to contain a line matching the pattern after the mark */
    private static void awaitLog(IInvokableInstance instance, long mark, String pattern)
    {
        for (int i = 0; i < 40; i++)
        {
            if (!instance.logs().grep(mark, pattern).getResult().isEmpty())
                return;
            Uninterruptibles.sleepUninterruptibly(250, TimeUnit.MILLISECONDS);
        }
        assertThat(instance.logs().grep(mark, pattern).getResult())
                  .describedAs("expected a log line matching '%s'", pattern)
                  .isNotEmpty();
    }

    /** immediately interrupt the outbound connections from one node to another, forcing renegotiation */
    private static void interruptOutbound(Cluster cluster, int from, int to)
    {
        String peer = cluster.get(to).config().broadcastAddress().getAddress().getHostAddress()
                      + ':' + cluster.get(to).config().broadcastAddress().getPort();
        cluster.get(from).runOnInstance(() -> {
            try
            {
                MessagingService.instance().interruptOutbound(InetAddressAndPort.getByName(peer));
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }
        });
    }
}
