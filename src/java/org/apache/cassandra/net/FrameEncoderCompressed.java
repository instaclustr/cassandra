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
import java.util.zip.CRC32;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.metrics.InternodeCompressionMetrics;
import org.apache.cassandra.utils.ByteBufferUtil;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;

import static org.apache.cassandra.utils.Crc.crc24;
import static org.apache.cassandra.utils.Crc.crc32;

/**
 * A frame encoder that compresses payloads with a configurable {@link ICompressor}, negotiated by algorithm id
 * during the internode handshake ({@link OutboundConnectionSettings.Framing#COMPRESSED}).
 *
 * The on-wire frame layout is identical to the LZ4 framing (see {@link FrameDecoderLZ4}): an 8-byte header
 * carrying the 17-bit compressed length, 17-bit uncompressed length, self-contained flag and a CRC24 of the
 * header, followed by the payload and a CRC32 trailer. The only difference is that the payload bytes are
 * produced by the negotiated {@link ICompressor} (which may itself embed algorithm-specific metadata, e.g.
 * LZ4Compressor's leading length prefix), and an uncompressed length of 0 signals that the payload is stored
 * uncompressed because compression did not reduce its size.
 *
 * {@link FrameEncoderLZ4} is deliberately left untouched: it defines the wire format for the legacy LZ4
 * framing id, which remains the default and must stay byte-compatible with all supported peers.
 */
@ChannelHandler.Sharable
public final class FrameEncoderCompressed extends FrameEncoder
{
    private static final int HEADER_LENGTH = 8;
    public static final int HEADER_AND_TRAILER_LENGTH = 12;

    private final ICompressor compressor;

    FrameEncoderCompressed(ICompressor compressor)
    {
        this.compressor = compressor;
    }

    private static void writeHeader(ByteBuffer frame, boolean isSelfContained, long compressedLength, long uncompressedLength)
    {
        long header5b = compressedLength | (uncompressedLength << 17);
        if (isSelfContained)
            header5b |= 1L << 34;

        long crc = crc24(header5b, 5);

        long header8b = header5b | (crc << 40);
        if (frame.order() == ByteOrder.BIG_ENDIAN)
            header8b = Long.reverseBytes(header8b);

        frame.putLong(0, header8b);
    }

    public ByteBuf encode(boolean isSelfContained, ByteBuffer in)
    {
        ByteBuffer frame = null;
        try
        {
            int uncompressedLength = in.remaining();
            if (uncompressedLength >= 1 << 17)
                throw new IllegalArgumentException("Maximum uncompressed payload size is 128KiB");
            final int payloadLength = uncompressedLength;

            int maxOutputLength = compressor.initialCompressedBufferLength(uncompressedLength);
            frame = bufferPool.getAtLeast(HEADER_AND_TRAILER_LENGTH + maxOutputLength, BufferType.OFF_HEAP);

            // ICompressor is position/limit based and advances positions. Both buffers are exclusively
            // owned here, so rather than allocating duplicate views on this hot path we mutate their
            // positions directly: `frame` is positioned past the header for the compressed output, and
            // `in` has its original position captured for the store-raw fallback (ByteBufferUtil.copyBytes
            // is absolute and the buffer pool ignores position, so nothing else needs it restored).
            final int inPosition = in.position();
            frame.position(HEADER_LENGTH);
            frame.limit(HEADER_LENGTH + maxOutputLength);
            try
            {
                compressor.compress(in, frame);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Failed to compress frame", e);
            }
            int compressedLength = frame.position() - HEADER_LENGTH;

            if (compressedLength >= uncompressedLength)
            {
                // compression did not help; store raw, signalled by uncompressedLength == 0
                ByteBufferUtil.copyBytes(in, inPosition, frame, HEADER_LENGTH, uncompressedLength);
                compressedLength = uncompressedLength;
                uncompressedLength = 0;
                if (payloadLength > 0)
                    InternodeCompressionMetrics.outboundUncompressableFrames.inc();
            }

            InternodeCompressionMetrics.outboundUncompressedBytes.inc(payloadLength);
            InternodeCompressionMetrics.outboundCompressedBytes.inc(compressedLength);

            writeHeader(frame, isSelfContained, compressedLength, uncompressedLength);

            CRC32 crc = crc32();
            frame.position(HEADER_LENGTH);
            frame.limit(compressedLength + HEADER_LENGTH);
            crc.update(frame);

            int frameCrc = (int) crc.getValue();
            if (frame.order() == ByteOrder.BIG_ENDIAN)
                frameCrc = Integer.reverseBytes(frameCrc);
            int frameLength = compressedLength + HEADER_AND_TRAILER_LENGTH;

            frame.limit(frameLength);
            frame.putInt(frameCrc);
            frame.position(0);

            bufferPool.putUnusedPortion(frame);
            return GlobalBufferPoolAllocator.wrap(frame);
        }
        catch (Throwable t)
        {
            if (frame != null)
                bufferPool.put(frame);
            throw t;
        }
        finally
        {
            bufferPool.put(in);
        }
    }

    @Override
    public String toString()
    {
        return "FrameEncoderCompressed(" + compressor.getClass().getSimpleName() + ')';
    }
}
