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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.netty.buffer.ByteBuf;

import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.schema.CompressionParams;

/**
 * Public bridge for {@link org.apache.cassandra.test.microbench.InternodeFramingBench}: the frame
 * codec API ({@link FrameEncoder#encode}, the codec constructors, {@link ShareableBytes}) is
 * deliberately package-private, while the benchmark class must live in the microbench package for
 * the ant runner's include pattern to find it. This class exposes exactly the operations the
 * benchmark measures, keeping the hot-path calls direct (no reflection on the measured path).
 */
public final class InternodeFramingBenchAccess
{
    private InternodeFramingBenchAccess()
    {
    }

    public static Object legacyLz4Encoder()
    {
        return FrameEncoderLZ4.fastInstance;
    }

    public static Object compressedEncoder(String algorithm)
    {
        return new FrameEncoderCompressed(CompressionParams.createCompressor(new ParameterizedClass(algorithm, Collections.emptyMap())));
    }

    /** allocate a payload, fill it with {@code data}, encode it and release the result; returns the frame size */
    public static int encodeOnce(Object encoder, byte[] data)
    {
        FrameEncoder frameEncoder = (FrameEncoder) encoder;
        FrameEncoder.Payload payload = frameEncoder.allocator().allocate(true, data.length);
        payload.buffer.put(data);
        payload.finish();
        ByteBuf buf = frameEncoder.encode(true, payload.buffer);
        int length = buf.readableBytes();
        buf.release();
        return length;
    }

    /** an opaque decode fixture: a decoder plus a pre-encoded frame to decode from */
    public static Object newDecodeState(boolean legacyLz4, String algorithm, byte[] data)
    {
        FrameEncoder encoder;
        FrameDecoder decoder;
        if (legacyLz4)
        {
            encoder = FrameEncoderLZ4.fastInstance;
            decoder = FrameDecoderLZ4.fast(GlobalBufferPoolAllocator.instance);
        }
        else
        {
            encoder = (FrameEncoder) compressedEncoder(algorithm);
            decoder = new FrameDecoderCompressed(GlobalBufferPoolAllocator.instance,
                                                 CompressionParams.createCompressor(new ParameterizedClass(algorithm, Collections.emptyMap())));
        }

        FrameEncoder.Payload payload = encoder.allocator().allocate(true, data.length);
        payload.buffer.put(data);
        payload.finish();
        ByteBuf buf = encoder.encode(true, payload.buffer);
        byte[] frame = new byte[buf.readableBytes()];
        buf.getBytes(buf.readerIndex(), frame);
        buf.release();
        return new DecodeState(decoder, frame);
    }

    /** decode the pre-encoded frame once; returns the decoded payload size */
    public static int decodeOnce(Object state)
    {
        DecodeState decodeState = (DecodeState) state;
        java.nio.ByteBuffer wire = GlobalBufferPoolAllocator.instance.get(decodeState.frame.length);
        wire.put(decodeState.frame);
        wire.flip();
        ShareableBytes bytes = ShareableBytes.wrap(wire);

        List<FrameDecoder.Frame> out = decodeState.out;
        out.clear();
        decodeState.decoder.decode(out, bytes);

        int decoded = 0;
        for (FrameDecoder.Frame frame : out)
        {
            decoded += ((FrameDecoder.IntactFrame) frame).contents.remaining();
            frame.release();
        }
        return decoded;
    }

    private static final class DecodeState
    {
        final FrameDecoder decoder;
        final byte[] frame;
        final List<FrameDecoder.Frame> out = new ArrayList<>(1);

        DecodeState(FrameDecoder decoder, byte[] frame)
        {
            this.decoder = decoder;
            this.frame = frame;
        }
    }
}
