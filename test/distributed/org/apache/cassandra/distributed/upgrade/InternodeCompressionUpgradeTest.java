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

package org.apache.cassandra.distributed.upgrade;

import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.Test;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Rolling-upgrade test for negotiable internode compression (COMPRESSED framing, CASSANDRA-20488):
 * 6.0 peers must never be offered COMPRESSED framing (they would fail the handshake), while fully
 * upgraded, configured peers negotiate it.
 *
 * Assertions are anchored on node2's log (see InternodeCompressionNegotiationTest for why: the
 * dtest log sifting reliably captures node2's messaging event-loop lines, covering both its
 * outbound "successfully connected" and inbound "messaging connection established" lines).
 */
public class InternodeCompressionUpgradeTest extends UpgradeTestBase
{
    private static final Map<String, Object> ZSTD_CONFIG = ImmutableMap.of("class_name", "ZstdCompressor");

    private static final String OUTBOUND_COMPRESSED = "successfully connected.*framing = COMPRESSED";
    private static final String INBOUND_COMPRESSED = "messaging connection established.*framing = COMPRESSED";

    @Test
    public void compressedFramingOnlyBetweenUpgradedNodes() throws Throwable
    {
        new TestCase()
        .nodes(2)
        .nodesToUpgradeOrdered(1, 2)
        .upgradesToCurrentFrom(v60)
        .withConfig(c -> c.with(NETWORK, GOSSIP).set("internode_compression", "all"))
        .setup(cluster -> {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, v text)"));
            traffic(cluster, 0);
        })
        .runAfterNodeUpgrade((cluster, node) -> {
            // enable the compressor on the just-upgraded node (bounce so the config applies)
            restartWithCompressionConfig(cluster, node);

            if (node == 1)
            {
                // mixed cluster: node1 = current + configured, node2 = 6.0. Traffic must flow, and
                // the 6.0 peer must never be offered COMPRESSED framing.
                traffic(cluster, 1000);
                assertThat(cluster.get(2).logs().grep(INBOUND_COMPRESSED).getResult()).isEmpty();
                assertThat(cluster.get(2).logs().grep(OUTBOUND_COMPRESSED).getResult()).isEmpty();
            }
        })
        .runAfterClusterUpgrade(cluster -> {
            // both nodes upgraded and configured: fresh connections must negotiate COMPRESSED
            long mark = cluster.get(2).logs().mark();
            interruptOutbound(cluster, 1, 2);
            interruptOutbound(cluster, 2, 1);
            traffic(cluster, 2000);

            awaitLog(cluster.get(2), mark, OUTBOUND_COMPRESSED);
            awaitLog(cluster.get(2), mark, INBOUND_COMPRESSED);
        })
        .run();
    }

    private static void restartWithCompressionConfig(UpgradeableCluster cluster, int node) throws Throwable
    {
        cluster.get(node).shutdown().get();
        cluster.get(node).config().set("internode_compression_config", ZSTD_CONFIG);
        cluster.get(node).startup();
    }

    private static void traffic(UpgradeableCluster cluster, int base)
    {
        for (int i = base; i < base + 50; i++)
            cluster.coordinator(1 + (i % 2))
                   .execute(withKeyspace("INSERT INTO %s.t (k, v) VALUES (?, ?)"), ConsistencyLevel.ALL, i, "v" + i);

        for (int i = base; i < base + 50; i++)
            assertRows(cluster.coordinator(1 + ((i + 1) % 2))
                              .execute(withKeyspace("SELECT v FROM %s.t WHERE k = ?"), ConsistencyLevel.ALL, i),
                       row("v" + i));
    }

    private static void awaitLog(IInstance instance, long mark, String pattern)
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

    /** interrupt outbound connections of an upgraded (current-version) node, forcing renegotiation */
    private static void interruptOutbound(UpgradeableCluster cluster, int from, int to)
    {
        String peer = cluster.get(to).config().broadcastAddress().getAddress().getHostAddress()
                      + ':' + cluster.get(to).config().broadcastAddress().getPort();
        ((IInvokableInstance) cluster.get(from)).runOnInstance(() -> {
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
