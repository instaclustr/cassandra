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
package org.apache.cassandra.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.ZstdCompressor;
import org.apache.cassandra.metrics.InternodeCompressionMetrics;
import org.apache.cassandra.net.FrameDecoder.CorruptFrame;
import org.apache.cassandra.net.FrameDecoder.Frame;
import org.apache.cassandra.net.FrameDecoder.IntactFrame;
import org.apache.cassandra.net.HandshakeProtocol.Initiate;
import org.apache.cassandra.net.OutboundConnectionSettings.Framing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.memory.BufferPools;

import io.netty.buffer.ByteBuf;

import static org.apache.cassandra.net.MessagingService.VERSION_40;
import static org.apache.cassandra.net.MessagingService.current_version;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Round-trip tests for the negotiable-compressor internode framing
 * ({@link FrameEncoderCompressed} / {@link FrameDecoderCompressed}), and for the
 * handshake bits that carry the negotiated algorithm id.
 */
public class FramingCompressedTest
{
    @BeforeClass
    public static void begin()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @AfterClass
    public static void end()
    {
        // do not leak a configured internode compressor into other tests
        InternodeCompressors.initialize(null);
    }

    private static FrameEncoder encoderFor(InternodeCompressors.Algorithm algorithm)
    {
        // default parameters; parameters are an encode-side concern and need not match the decoder
        return new FrameEncoderCompressed(InternodeCompressors.decompressor(algorithm));
    }

    private static FrameDecoder decoderFor(InternodeCompressors.Algorithm algorithm)
    {
        return InternodeCompressors.decoder(algorithm.id, GlobalBufferPoolAllocator.instance);
    }

    private static byte[] compressibleBytes(int length)
    {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; ++i)
            bytes[i] = (byte) (i % 32);
        return bytes;
    }

    private static byte[] randomBytes(Random random, int length)
    {
        byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        return bytes;
    }

    /** encode {@code bytes} into a single frame and return the raw on-wire buffer */
    private static ByteBuffer encodeFrame(FrameEncoder encoder, byte[] bytes)
    {
        FrameEncoder.Payload payload = encoder.allocator().allocate(true, bytes.length);
        payload.buffer.put(bytes);
        payload.finish();

        ByteBuf buf = encoder.encode(true, payload.buffer);
        try
        {
            ByteBuffer frame = BufferPools.forNetworking().getAtLeast(buf.readableBytes(), BufferType.OFF_HEAP);
            frame.put(buf.internalNioBuffer(buf.readerIndex(), buf.readableBytes()));
            frame.flip();
            return frame;
        }
        finally
        {
            buf.release();
        }
    }

    private static void roundTrip(FrameEncoder encoder, FrameDecoder decoder, byte[] bytes)
    {
        // decode() consumes the ShareableBytes it is given, so hand it a slice and release the parent
        ShareableBytes frames = ShareableBytes.wrap(encodeFrame(encoder, bytes));
        List<Frame> out = new ArrayList<>();
        try
        {
            decoder.decode(out, frames.slice(0, frames.get().limit()));

            Assert.assertEquals(1, out.size());
            Assert.assertTrue(out.get(0) instanceof IntactFrame);
            IntactFrame frame = (IntactFrame) out.get(0);
            Assert.assertTrue(frame.isSelfContained);

            byte[] actual = new byte[frame.contents.remaining()];
            frame.contents.get().duplicate().get(actual);
            Assert.assertArrayEquals(bytes, actual);

            Assert.assertNull(decoder.stash);
            Assert.assertTrue(decoder.frames.isEmpty());
        }
        finally
        {
            for (Frame frame : out)
                frame.release();
            frames.release();
        }
    }

    /** feed the encoded frame to the decoder in random chunks, to exercise accumulation across reads */
    private static void roundTripChunked(Random random, FrameEncoder encoder, FrameDecoder decoder, byte[] bytes)
    {
        ShareableBytes frames = ShareableBytes.wrap(encodeFrame(encoder, bytes));
        List<Frame> out = new ArrayList<>();
        try
        {
            int end = frames.get().limit();
            for (int i = 0; i < end; )
            {
                int limit = i + 1 + random.nextInt(end - i);
                decoder.decode(out, frames.slice(i, limit));
                i = limit;
            }

            Assert.assertEquals(1, out.size());
            IntactFrame frame = (IntactFrame) out.get(0);
            byte[] actual = new byte[frame.contents.remaining()];
            frame.contents.get().duplicate().get(actual);
            Assert.assertArrayEquals(bytes, actual);

            Assert.assertNull(decoder.stash);
            Assert.assertTrue(decoder.frames.isEmpty());
        }
        finally
        {
            for (Frame frame : out)
                frame.release();
            frames.release();
        }
    }

    @Test
    public void testRoundTripAllAlgorithms()
    {
        long seed = new SecureRandom().nextLong();
        Random random = new Random(seed);

        for (InternodeCompressors.Algorithm algorithm : InternodeCompressors.Algorithm.values())
        {
            FrameEncoder encoder = encoderFor(algorithm);
            FrameDecoder decoder = decoderFor(algorithm);

            // compressible payload: exercises the compressed path
            roundTrip(encoder, decoder, compressibleBytes(50_000));
            // random payload: compression does not help, exercises the stored-raw (uncompressedLength == 0) path
            roundTrip(encoder, decoder, randomBytes(random, 50_000));
            // boundaries
            roundTrip(encoder, decoder, new byte[0]);
            roundTrip(encoder, decoder, new byte[]{ 42 });
            roundTrip(encoder, decoder, compressibleBytes((1 << 17) - 1));
            roundTrip(encoder, decoder, randomBytes(random, (1 << 17) - 1));
            // chunked feeding, both payload kinds
            roundTripChunked(random, encoder, decoder, compressibleBytes(60_000));
            roundTripChunked(random, encoder, decoder, randomBytes(random, 60_000));
        }
    }

    @Test
    public void testCorruptPayloadIsDetected()
    {
        FrameEncoder encoder = encoderFor(InternodeCompressors.Algorithm.ZSTD);
        FrameDecoder decoder = decoderFor(InternodeCompressors.Algorithm.ZSTD);

        ByteBuffer frame = encodeFrame(encoder, compressibleBytes(10_000));
        // flip a payload byte (offset 8 is the first payload byte, after the 8-byte header)
        frame.put(10, (byte) (frame.get(10) + 1));

        ShareableBytes frames = ShareableBytes.wrap(frame);
        List<Frame> out = new ArrayList<>();
        try
        {
            decoder.decode(out, frames.slice(0, frames.get().limit()));
            Assert.assertEquals(1, out.size());
            Assert.assertTrue(out.get(0) instanceof CorruptFrame);
            CorruptFrame corrupt = (CorruptFrame) out.get(0);
            Assert.assertTrue(corrupt.isRecoverable());
        }
        finally
        {
            for (Frame f : out)
                f.release();
            frames.release();
        }
    }

    @Test
    public void testRegistry()
    {
        InternodeCompressors.initialize(null);
        Assert.assertFalse(InternodeCompressors.isConfigured());

        InternodeCompressors.initialize(new ParameterizedClass(ZstdCompressor.class.getName(),
                                                               Map.of("compression_level", "3")));
        Assert.assertTrue(InternodeCompressors.isConfigured());
        Assert.assertEquals(InternodeCompressors.Algorithm.ZSTD, InternodeCompressors.configuredAlgorithm());
        Assert.assertNotNull(InternodeCompressors.encoder());

        // 3 is the one id that fits the 2-bit handshake field but has no algorithm assigned
        assertThatThrownBy(() -> InternodeCompressors.decoder(3, GlobalBufferPoolAllocator.instance))
                          .isInstanceOf(IllegalArgumentException.class);

        InternodeCompressors.initialize(null);
        Assert.assertFalse(InternodeCompressors.isConfigured());
    }

    @Test
    public void testConfigurationNameForms()
    {
        // each algorithm is configurable by fully-qualified name, simple class name, or
        // case-insensitive shorthand
        for (String name : new String[]{ "org.apache.cassandra.io.compress.ZstdCompressor", "ZstdCompressor", "zstd", "ZSTD", "Zstd" })
        {
            InternodeCompressors.initialize(new ParameterizedClass(name, Map.of()));
            Assert.assertEquals(name, InternodeCompressors.Algorithm.ZSTD, InternodeCompressors.configuredAlgorithm());
        }
        for (String name : new String[]{ "org.apache.cassandra.io.compress.LZ4Compressor", "LZ4Compressor", "lz4", "LZ4" })
        {
            InternodeCompressors.initialize(new ParameterizedClass(name, Map.of()));
            Assert.assertEquals(name, InternodeCompressors.Algorithm.LZ4, InternodeCompressors.configuredAlgorithm());
        }

        // anything else is rejected at startup: real compressors outside the registry, their
        // shorthands, and unknown names alike
        for (String name : new String[]{ "SnappyCompressor", "snappy", "DeflateCompressor", "NoopCompressor", "gzip", "nonsense" })
        {
            assertThatThrownBy(() -> InternodeCompressors.initialize(new ParameterizedClass(name, Map.of())))
                              .describedAs(name)
                              .isInstanceOf(ConfigurationException.class);
        }

        InternodeCompressors.initialize(null);
    }

    @Test
    public void testAlgorithmMinimumVersionsGateCompression()
    {
        // no algorithm may predate COMPRESSED framing itself
        for (InternodeCompressors.Algorithm algorithm : InternodeCompressors.Algorithm.values())
            Assert.assertTrue(algorithm + " must not predate COMPRESSED framing",
                              algorithm.minimumVersion >= MessagingService.VERSION_70);

        // unconfigured: never compress
        InternodeCompressors.initialize(null);
        Assert.assertFalse(InternodeCompressors.mayCompress(current_version, current_version));

        InternodeCompressors.initialize(new ParameterizedClass(ZstdCompressor.class.getName(), Map.of()));
        int minimum = InternodeCompressors.Algorithm.ZSTD.minimumVersion;
        // both ends at (or above) the version that introduced the algorithm: compress
        Assert.assertTrue(InternodeCompressors.mayCompress(minimum, minimum));
        // peer too old to know the algorithm id: never offer it
        Assert.assertFalse(InternodeCompressors.mayCompress(minimum, minimum - 1));
        // this node capped below the algorithm's version (storage compatibility mode): stay legacy
        Assert.assertFalse(InternodeCompressors.mayCompress(minimum - 1, minimum));

        InternodeCompressors.initialize(null);
    }

    @Test
    public void testCompressionMetrics()
    {
        FrameEncoder encoder = encoderFor(InternodeCompressors.Algorithm.ZSTD);
        FrameDecoder decoder = decoderFor(InternodeCompressors.Algorithm.ZSTD);

        // compressible payload: byte counters move, compressed strictly smaller, no uncompressable frame
        long outUncompressed = InternodeCompressionMetrics.outboundUncompressedBytes.getCount();
        long outCompressed = InternodeCompressionMetrics.outboundCompressedBytes.getCount();
        long inUncompressed = InternodeCompressionMetrics.inboundUncompressedBytes.getCount();
        long inCompressed = InternodeCompressionMetrics.inboundCompressedBytes.getCount();
        long uncompressable = InternodeCompressionMetrics.outboundUncompressableFrames.getCount();

        roundTrip(encoder, decoder, compressibleBytes(50_000));

        long sentCompressed = InternodeCompressionMetrics.outboundCompressedBytes.getCount() - outCompressed;
        Assert.assertEquals(50_000, InternodeCompressionMetrics.outboundUncompressedBytes.getCount() - outUncompressed);
        Assert.assertTrue("compressed size should be non-zero and smaller than the payload",
                          sentCompressed > 0 && sentCompressed < 50_000);
        Assert.assertEquals(50_000, InternodeCompressionMetrics.inboundUncompressedBytes.getCount() - inUncompressed);
        Assert.assertEquals(sentCompressed, InternodeCompressionMetrics.inboundCompressedBytes.getCount() - inCompressed);
        Assert.assertEquals(uncompressable, InternodeCompressionMetrics.outboundUncompressableFrames.getCount());

        // incompressible payload: stored raw, counted as an uncompressable frame, bytes equal on both counters
        outUncompressed = InternodeCompressionMetrics.outboundUncompressedBytes.getCount();
        outCompressed = InternodeCompressionMetrics.outboundCompressedBytes.getCount();

        roundTrip(encoder, decoder, randomBytes(new Random(42), 10_000));

        Assert.assertEquals(10_000, InternodeCompressionMetrics.outboundUncompressedBytes.getCount() - outUncompressed);
        Assert.assertEquals(10_000, InternodeCompressionMetrics.outboundCompressedBytes.getCount() - outCompressed);
        Assert.assertEquals(uncompressable + 1, InternodeCompressionMetrics.outboundUncompressableFrames.getCount());
    }

    @Test
    public void testHandshakeInitiateCarriesCompressionAlgorithm() throws IOException
    {
        AcceptVersions accept = new AcceptVersions(VERSION_40, current_version);

        // COMPRESSED framing carries the algorithm id in flag bits 5-7
        Initiate initiate = new Initiate(accept, ConnectionType.SMALL_MESSAGES, Framing.COMPRESSED,
                                         InternodeCompressors.Algorithm.ZSTD.id, FBUtilities.getBroadcastAddressAndPort());
        ByteBuf encoded = initiate.encode();
        try
        {
            Initiate decoded = Initiate.maybeDecode(encoded);
            Assert.assertNotNull(decoded);
            Assert.assertEquals(initiate, decoded);
            Assert.assertEquals(Framing.COMPRESSED, decoded.framing);
            Assert.assertEquals(InternodeCompressors.Algorithm.ZSTD.id, decoded.compressionAlgorithmId);
        }
        finally
        {
            encoded.release();
        }

        // legacy framings carry 0, preserving the wire format bit-for-bit
        Initiate legacy = new Initiate(accept, ConnectionType.SMALL_MESSAGES, Framing.LZ4, 0,
                                       FBUtilities.getBroadcastAddressAndPort());
        ByteBuf legacyEncoded = legacy.encode();
        try
        {
            Initiate decoded = Initiate.maybeDecode(legacyEncoded);
            Assert.assertNotNull(decoded);
            Assert.assertEquals(legacy, decoded);
            Assert.assertEquals(0, decoded.compressionAlgorithmId);
        }
        finally
        {
            legacyEncoded.release();
        }

        // the reserved flag bit 7 must be rejected, not silently ignored: that is what allows a
        // future release to assign it without risking misinterpretation by this version.
        // flags are the second big-endian int of the message, so bit 7 (0x80) lives in byte 7
        Initiate valid = new Initiate(accept, ConnectionType.SMALL_MESSAGES, Framing.COMPRESSED,
                                      InternodeCompressors.Algorithm.ZSTD.id, FBUtilities.getBroadcastAddressAndPort());
        ByteBuf tampered = valid.encode();
        try
        {
            tampered.setByte(7, tampered.getByte(7) | 0x80);
            assertThatThrownBy(() -> Initiate.maybeDecode(tampered))
                              .isInstanceOf(IOException.class)
                              .hasMessageContaining("reserved");
        }
        finally
        {
            tampered.release();
        }
    }

    @Test
    public void testDecodersAreIndependentPerAlgorithm()
    {
        // a frame encoded with ZSTD must fail cleanly (not corrupt silently) when decoded as LZ4;
        // in practice this cannot happen, since both sides derive the codec from the negotiated id
        FrameEncoder encoder = encoderFor(InternodeCompressors.Algorithm.ZSTD);
        FrameDecoder decoder = decoderFor(InternodeCompressors.Algorithm.LZ4);

        ShareableBytes frames = ShareableBytes.wrap(encodeFrame(encoder, compressibleBytes(10_000)));
        List<Frame> out = new ArrayList<>();
        try
        {
            // any exception is acceptable; the CRC has already proven the bytes intact, so this
            // can only be a codec mismatch, which must not be silently misinterpreted
            assertThatThrownBy(() -> decoder.decode(out, frames.slice(0, frames.get().limit())))
                              .isNotNull();
        }
        finally
        {
            for (Frame f : out)
                f.release();
            frames.release();
        }
    }

}
