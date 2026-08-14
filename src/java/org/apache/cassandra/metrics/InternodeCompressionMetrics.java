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
package org.apache.cassandra.metrics;

import com.codahale.metrics.Counter;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Node-wide metrics for the negotiable-compressor internode framing
 * ({@link org.apache.cassandra.net.OutboundConnectionSettings.Framing#COMPRESSED}).
 *
 * The byte counters measure payload bytes only (excluding the constant per-frame header and
 * trailer), on both sides of the codec, so the achieved compression ratio of the traffic this
 * node sends is {@code OutboundCompressedBytes / OutboundUncompressedBytes} (and likewise for
 * the traffic it receives). This is the ratio to evaluate before/after changing
 * {@code internode_compression_config}. Frames whose payload did not shrink are stored
 * uncompressed and counted in {@code OutboundUncompressableFrames}; their bytes contribute
 * equally to both byte counters.
 */
public final class InternodeCompressionMetrics
{
    public static final String TYPE_NAME = "InternodeCompression";

    private static final MetricNameFactory factory = new DefaultNameFactory(TYPE_NAME);

    /** payload bytes handed to the outbound COMPRESSED-framing encoder, before compression */
    public static final Counter outboundUncompressedBytes = Metrics.counter(factory.createMetricName("OutboundUncompressedBytes"));

    /** payload bytes actually put on the wire by the outbound COMPRESSED-framing encoder */
    public static final Counter outboundCompressedBytes = Metrics.counter(factory.createMetricName("OutboundCompressedBytes"));

    /** frames stored uncompressed because compression did not reduce their size */
    public static final Counter outboundUncompressableFrames = Metrics.counter(factory.createMetricName("OutboundUncompressableFrames"));

    /** payload bytes received off the wire by the inbound COMPRESSED-framing decoder */
    public static final Counter inboundCompressedBytes = Metrics.counter(factory.createMetricName("InboundCompressedBytes"));

    /** payload bytes produced by the inbound COMPRESSED-framing decoder, after decompression */
    public static final Counter inboundUncompressedBytes = Metrics.counter(factory.createMetricName("InboundUncompressedBytes"));

    private InternodeCompressionMetrics()
    {
    }
}
