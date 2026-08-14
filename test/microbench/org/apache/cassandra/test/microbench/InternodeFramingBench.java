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
package org.apache.cassandra.test.microbench;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.InternodeFramingBenchAccess;

/**
 * Isolates the overhead of the negotiable-compressor internode framing machinery (CASSANDRA-20488)
 * by comparing the legacy LZ4 framing against the COMPRESSED framing configured with the same LZ4
 * algorithm: the delta between {@code encodeLegacyLz4} and {@code encodeCompressedLz4} (and the
 * decode pair) is purely the added machinery - ICompressor dispatch, metrics counters, and the
 * compressor's self-framing envelope - not compression itself. The Zstd benchmarks are context for
 * the algorithm's own cost, not part of the machinery comparison.
 */
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = "-Xmx1G")
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class InternodeFramingBench
{
    @Param({ "512", "32768" })
    int payloadSize;

    private byte[] payload;
    private Object legacyLz4Encoder;
    private Object compressedLz4Encoder;
    private Object compressedZstdEncoder;
    private Object legacyLz4DecodeState;
    private Object compressedLz4DecodeState;

    @Setup
    public void setup()
    {
        DatabaseDescriptor.daemonInitialization();

        // compressible content, so the LZ4-vs-LZ4 comparison exercises the real compressed path
        // (rather than the store-raw fallback) in both framings
        payload = new byte[payloadSize];
        for (int i = 0; i < payloadSize; i++)
            payload[i] = (byte) (i % 32);

        legacyLz4Encoder = InternodeFramingBenchAccess.legacyLz4Encoder();
        compressedLz4Encoder = InternodeFramingBenchAccess.compressedEncoder("LZ4Compressor");
        compressedZstdEncoder = InternodeFramingBenchAccess.compressedEncoder("ZstdCompressor");
        legacyLz4DecodeState = InternodeFramingBenchAccess.newDecodeState(true, null, payload);
        compressedLz4DecodeState = InternodeFramingBenchAccess.newDecodeState(false, "LZ4Compressor", payload);
    }

    @Benchmark
    public int encodeLegacyLz4()
    {
        return InternodeFramingBenchAccess.encodeOnce(legacyLz4Encoder, payload);
    }

    @Benchmark
    public int encodeCompressedLz4()
    {
        return InternodeFramingBenchAccess.encodeOnce(compressedLz4Encoder, payload);
    }

    @Benchmark
    public int encodeCompressedZstd()
    {
        return InternodeFramingBenchAccess.encodeOnce(compressedZstdEncoder, payload);
    }

    @Benchmark
    public int decodeLegacyLz4()
    {
        return InternodeFramingBenchAccess.decodeOnce(legacyLz4DecodeState);
    }

    @Benchmark
    public int decodeCompressedLz4()
    {
        return InternodeFramingBenchAccess.decodeOnce(compressedLz4DecodeState);
    }
}
