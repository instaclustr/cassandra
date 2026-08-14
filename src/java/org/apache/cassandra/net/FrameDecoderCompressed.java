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
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.zip.CRC32;

import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.metrics.InternodeCompressionMetrics;

import io.netty.channel.ChannelPipeline;

import static org.apache.cassandra.utils.Crc.crc24;
import static org.apache.cassandra.utils.Crc.crc32;
import static org.apache.cassandra.utils.Crc.updateCrc32;

/**
 * Counterpart of {@link FrameEncoderCompressed}: decodes frames whose payload was compressed with the
 * {@link ICompressor} negotiated during the internode handshake. The frame layout is identical to the
 * LZ4 framing described in {@link FrameDecoderLZ4}; only the payload compression algorithm differs.
 */
public final class FrameDecoderCompressed extends FrameDecoderWith8bHeader
{
    private static final int HEADER_LENGTH = 8;
    private static final int TRAILER_LENGTH = 4;
    private static final int HEADER_AND_TRAILER_LENGTH = 12;

    private static int compressedLength(long header8b)
    {
        return ((int) header8b) & 0x1FFFF;
    }
    private static int uncompressedLength(long header8b)
    {
        return ((int) (header8b >>> 17)) & 0x1FFFF;
    }
    private static boolean isSelfContained(long header8b)
    {
        return 0 != (header8b & (1L << 34));
    }
    private static int headerCrc(long header8b)
    {
        return ((int) (header8b >>> 40)) & 0xFFFFFF;
    }

    private final ICompressor compressor;

    FrameDecoderCompressed(BufferPoolAllocator allocator, ICompressor compressor)
    {
        super(allocator);
        this.compressor = compressor;
    }

    final long readHeader(ByteBuffer frame, int begin)
    {
        long header8b = frame.getLong(begin);
        if (frame.order() == ByteOrder.BIG_ENDIAN)
            header8b = Long.reverseBytes(header8b);
        return header8b;
    }

    final CorruptFrame verifyHeader(long header8b)
    {
        int computeLengthCrc = crc24(header8b, 5);
        int readLengthCrc = headerCrc(header8b);

        return readLengthCrc == computeLengthCrc ? null : CorruptFrame.unrecoverable(readLengthCrc, computeLengthCrc);
    }

    final int frameLength(long header8b)
    {
        return compressedLength(header8b) + HEADER_AND_TRAILER_LENGTH;
    }

    final Frame unpackFrame(ShareableBytes bytes, int begin, int end, long header8b)
    {
        ByteBuffer input = bytes.get();

        boolean isSelfContained = isSelfContained(header8b);
        int uncompressedLength = uncompressedLength(header8b);

        CRC32 crc = crc32();
        int readFullCrc = input.getInt(end - TRAILER_LENGTH);
        if (input.order() == ByteOrder.BIG_ENDIAN)
            readFullCrc = Integer.reverseBytes(readFullCrc);

        updateCrc32(crc, input, begin + HEADER_LENGTH, end - TRAILER_LENGTH);
        int computeFullCrc = (int) crc.getValue();

        if (readFullCrc != computeFullCrc)
            return CorruptFrame.recoverable(isSelfContained, uncompressedLength, readFullCrc, computeFullCrc);

        int compressedPayloadLength = end - begin - HEADER_AND_TRAILER_LENGTH;
        InternodeCompressionMetrics.inboundCompressedBytes.inc(compressedPayloadLength);
        InternodeCompressionMetrics.inboundUncompressedBytes.inc(uncompressedLength == 0 ? compressedPayloadLength : uncompressedLength);

        if (uncompressedLength == 0)
        {
            // the payload was stored uncompressed (compression did not reduce its size)
            return new IntactFrame(isSelfContained, bytes.slice(begin + HEADER_LENGTH, end - TRAILER_LENGTH));
        }

        ByteBuffer out = allocator.get(uncompressedLength);
        try
        {
            // ICompressor advances buffer positions. `input` is shared with the outer decode loop and
            // any following frames in the same network read, so its position must not be mutated: the
            // duplicate view is a deliberate allocation, not an avoidable one. `out` is exclusively
            // ours, so decompress into it directly and flip for wrapping below.
            ByteBuffer src = input.duplicate();
            src.position(begin + HEADER_LENGTH);
            src.limit(end - TRAILER_LENGTH);
            compressor.uncompress(src, out);
            if (out.position() != uncompressedLength)
                throw new IOException("Decompressed size mismatch: expected " + uncompressedLength + " but got " + out.position());
            out.flip();
            return new IntactFrame(isSelfContained, ShareableBytes.wrap(out));
        }
        catch (IOException e)
        {
            allocator.put(out);
            throw new RuntimeException("Failed to decompress frame", e);
        }
        catch (Throwable t)
        {
            allocator.put(out);
            throw t;
        }
    }

    void decode(Collection<Frame> into, ShareableBytes bytes)
    {
        decode(into, bytes, HEADER_LENGTH);
    }

    void addLastTo(ChannelPipeline pipeline)
    {
        pipeline.addLast("frameDecoderCompressed", this);
    }

    @Override
    public String toString()
    {
        return "FrameDecoderCompressed(" + compressor.getClass().getSimpleName() + ')';
    }
}
