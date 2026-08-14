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

import java.util.Collections;

import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.LocalizeString;

/**
 * Registry of the compression algorithms usable with {@link OutboundConnectionSettings.Framing#COMPRESSED}
 * internode framing.
 *
 * Each algorithm has a small, wire-stable id that is exchanged during the internode handshake
 * (2 bits of the {@link HandshakeProtocol.Initiate} connection flags, so ids must be in [1, 3];
 * 0 is reserved to mean "unset"). Compression parameters such as the level are deliberately NOT
 * negotiated: they only affect the compressing side, so each node applies its locally-configured
 * parameters when encoding, and decodes with a default-constructed instance of the negotiated
 * algorithm.
 *
 * The set of algorithms is fixed: negotiation is by id, so custom {@link ICompressor}
 * implementations cannot be used for internode framing (unlike sstable/commitlog compression).
 */
public final class InternodeCompressors
{
    public enum Algorithm
    {
        // id 0 is reserved: it is what peers without COMPRESSED framing send in the handshake bits,
        // and the Initiate decode enforces (framing == COMPRESSED) <=> (id != 0). One future algorithm
        // can take id 3; ids are append-only once released. Growing beyond id 3 requires widening the
        // field into the reserved handshake flag bit 7, gated on the messaging version that does so.
        // There is deliberately no NoopCompressor entry: uncompressed internode traffic is what
        // internode_compression: none (the CRC framing) already provides, without the negotiation.
        LZ4(1, "org.apache.cassandra.io.compress.LZ4Compressor", MessagingService.VERSION_70),
        ZSTD(2, "org.apache.cassandra.io.compress.ZstdCompressor", MessagingService.VERSION_70);

        /** wire-stable id, exchanged in the handshake; must fit in 2 bits and must never be reused */
        public final int id;
        /**
         * Fully-qualified name of the {@link ICompressor} implementation. A name rather than a
         * {@code Class<?>} so that loading this enum does not force classloading of every compressor
         * (some have static initializers that e.g. touch native libraries) - the same convention as
         * {@link org.apache.cassandra.io.compress.CompressorRegistry}. The class is only resolved
         * when an algorithm is actually configured or negotiated.
         */
        public final String compressorClassName;
        /** the simple class name, accepted in internode_compression_config (e.g. "ZstdCompressor") */
        public final String simpleName;
        /** the case-insensitive shorthand accepted in internode_compression_config (e.g. "zstd") */
        public final String abbreviation;
        /**
         * The messaging version whose registry first contained this algorithm. A peer below this
         * version does not know the id and would fail the handshake if offered it, so COMPRESSED
         * framing with this algorithm is only offered when both ends are at least at this version
         * (see {@link #mayCompress}). Algorithms added in a future release MUST set this to the
         * messaging version that introduces them - never to an older version.
         */
        public final int minimumVersion;

        Algorithm(int id, String compressorClassName, int minimumVersion)
        {
            if (id < 1 || id > 0x3)
                throw new IllegalStateException("Algorithm id must fit in the 2-bit handshake field (1..3): " + id);
            this.id = id;
            this.compressorClassName = compressorClassName;
            this.simpleName = compressorClassName.substring(compressorClassName.lastIndexOf('.') + 1);
            this.abbreviation = LocalizeString.toLowerCaseLocalized(simpleName.endsWith("Compressor")
                                                                    ? simpleName.substring(0, simpleName.length() - "Compressor".length())
                                                                    : simpleName);
            this.minimumVersion = minimumVersion;
        }

        /** cached because {@link #values()} defensively clones the array on every call */
        private static final Algorithm[] VALUES = values();

        /** id-indexed lookup for the connection path; index 0 stays null (the reserved sentinel) */
        private static final Algorithm[] BY_ID = new Algorithm[4];
        static
        {
            for (Algorithm algorithm : VALUES)
                BY_ID[algorithm.id] = algorithm;
        }

        public static Algorithm forId(int id)
        {
            return id > 0 && id < BY_ID.length ? BY_ID[id] : null;
        }

        public static Algorithm forCompressor(ICompressor compressor)
        {
            for (Algorithm algorithm : VALUES)
                if (algorithm.compressorClassName.equals(compressor.getClass().getName()))
                    return algorithm;
            return null;
        }
    }

    private static final class Configured
    {
        final Algorithm algorithm;
        final ICompressor compressor;
        // Built lazily on first use: constructing a FrameEncoder initializes FrameEncoder/BufferPools,
        // which read further DatabaseDescriptor state and therefore must not run while the config is
        // still being applied (initialize() is invoked from DatabaseDescriptor.applySimpleConfig).
        volatile FrameEncoderCompressed encoder;

        Configured(Algorithm algorithm, ICompressor compressor)
        {
            this.algorithm = algorithm;
            this.compressor = compressor;
        }
    }

    private static volatile Configured configured;

    /** decode-side compressors by algorithm id, created with default parameters (parameters are encode-side only) */
    private static final ICompressor[] decompressors = new ICompressor[8];

    private InternodeCompressors()
    {
    }

    /**
     * Resolve and validate the configured internode compressor; invoked from DatabaseDescriptor at startup
     * (and from tests). A null config disables COMPRESSED framing entirely, preserving legacy behaviour.
     */
    public static void initialize(ParameterizedClass config) throws ConfigurationException
    {
        if (config == null)
        {
            configured = null;
            return;
        }

        ICompressor compressor = CompressionParams.createCompressor(new ParameterizedClass(canonicalClassName(config.class_name),
                                                                                           config.parameters));
        Algorithm algorithm = Algorithm.forCompressor(compressor);
        if (algorithm == null)
            throw new ConfigurationException("internode_compression_config class " + compressor.getClass().getName() +
                                             " is not supported for internode framing; supported compressors: " +
                                             supportedNames());
        if (!compressor.supports(BufferType.OFF_HEAP))
            throw new ConfigurationException("internode_compression_config class " + compressor.getClass().getName() +
                                             " does not support direct buffers, which internode framing requires");

        configured = new Configured(algorithm, compressor);
    }

    /**
     * Resolve a configured name - the fully-qualified class name, the simple class name (e.g.
     * "ZstdCompressor"), or the case-insensitive shorthand (e.g. "zstd") - to the algorithm's
     * canonical class name. Unrecognised names are returned unchanged so that the subsequent
     * construction and whitelist check report the error with full context.
     */
    private static String canonicalClassName(String configured)
    {
        if (configured != null)
        {
            for (Algorithm algorithm : Algorithm.VALUES)
                if (algorithm.compressorClassName.equals(configured)
                    || algorithm.simpleName.equals(configured)
                    || algorithm.abbreviation.equalsIgnoreCase(configured))
                    return algorithm.compressorClassName;
        }
        return configured;
    }

    private static String supportedNames()
    {
        StringBuilder names = new StringBuilder();
        for (Algorithm algorithm : Algorithm.VALUES)
        {
            if (names.length() > 0)
                names.append(", ");
            names.append(algorithm.simpleName).append(" (").append(algorithm.abbreviation).append(')');
        }
        return names.toString();
    }

    /** @return true if an internode compressor has been configured, enabling COMPRESSED framing */
    public static boolean isConfigured()
    {
        return configured != null;
    }

    /**
     * @return whether COMPRESSED framing with the configured algorithm may be offered on a connection,
     * given this node's current messaging version and the peer's known messaging version. False when no
     * compressor is configured, or when either end runs below the version that introduced the algorithm
     * (a peer below it does not know the algorithm id; this node may be below it when capped by storage
     * compatibility mode).
     */
    public static boolean mayCompress(int selfVersion, int peerVersion)
    {
        Configured current = configured;
        return current != null
               && selfVersion >= current.algorithm.minimumVersion
               && peerVersion >= current.algorithm.minimumVersion;
    }

    public static Algorithm configuredAlgorithm()
    {
        Configured current = configured;
        if (current == null)
            throw new IllegalStateException("internode_compression_config is not set");
        return current.algorithm;
    }

    /** like {@link #configuredAlgorithm()}, but returns null when no compressor is configured */
    public static Algorithm configuredAlgorithmOrNull()
    {
        Configured current = configured;
        return current == null ? null : current.algorithm;
    }

    static FrameEncoderCompressed encoder()
    {
        Configured current = configured;
        if (current == null)
            throw new IllegalStateException("internode_compression_config is not set");
        FrameEncoderCompressed encoder = current.encoder;
        if (encoder == null)
        {
            synchronized (current)
            {
                encoder = current.encoder;
                if (encoder == null)
                    current.encoder = encoder = new FrameEncoderCompressed(current.compressor);
            }
        }
        return encoder;
    }

    static FrameDecoderCompressed decoder(int algorithmId, BufferPoolAllocator allocator)
    {
        Algorithm algorithm = Algorithm.forId(algorithmId);
        if (algorithm == null)
            throw new IllegalArgumentException("Unknown internode compression algorithm id " + algorithmId);
        return new FrameDecoderCompressed(allocator, decompressor(algorithm));
    }

    static synchronized ICompressor decompressor(Algorithm algorithm)
    {
        ICompressor decompressor = decompressors[algorithm.id];
        if (decompressor == null)
            decompressors[algorithm.id] = decompressor =
                CompressionParams.createCompressor(new ParameterizedClass(algorithm.compressorClassName,
                                                                          Collections.emptyMap()));
        return decompressor;
    }
}
