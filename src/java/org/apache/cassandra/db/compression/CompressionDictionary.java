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

package org.apache.cassandra.db.compression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.Objects;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.compress.ZstdDictionaryCompressor;

public interface CompressionDictionary extends AutoCloseable
{
    /**
     * Get the dictionary id
     *
     * @return dictionary id
     */
    DictId dictId();

    /**
     * Get the raw bytes of the compression dictionary
     *
     * @return raw compression dictionary
     */
    byte[] rawDictionary();

    /**
     * Get checkum of this dictionary.
     *
     * @return checksum of this dictionary
     */
    int checksum();

    /**
     * Get size of raw dictionary.
     *
     * @return size of raw dictionary, in bytes
     */
    int size();

    /**
     * Get the kind of the compression algorithm
     *
     * @return compression algorithm kind
     */
    default Kind kind()
    {
        return dictId().kind;
    }

    /**
     * Write compression dictionary to file
     *
     * @param out file output stream
     * @throws IOException on any I/O exception when writing to the file
     */
    default void serialize(DataOutput out) throws IOException
    {
        DictId dictId = dictId();
        int ordinal = dictId.kind.ordinal();
        out.writeByte(ordinal);
        out.writeLong(dictId.id);
        byte[] dict = rawDictionary();
        out.writeInt(dict.length);
        out.write(dict);
        int checksum = calculateChecksum((byte) ordinal, dictId.id, dict);
        out.writeInt(checksum);
    }

    /**
     * A factory method to create concrete CompressionDictionary from the file content
     *
     * @param input   file input stream
     * @param manager compression dictionary manager that caches the dictionaries
     * @return compression dictionary; otherwise, null if there is no dictionary
     * @throws IOException on any I/O exception when reading from the file
     */
    @Nullable
    static CompressionDictionary deserialize(DataInput input, @Nullable CompressionDictionaryManager manager) throws IOException
    {
        int kindOrdinal;
        try
        {
            kindOrdinal = input.readByte();
        }
        catch (EOFException eof)
        {
            // no dictionary
            return null;
        }

        if (kindOrdinal < 0 || kindOrdinal >= Kind.values().length)
        {
            throw new IOException("Invalid compression dictionary kind: " + kindOrdinal);
        }
        Kind kind = Kind.values()[kindOrdinal];
        long id = input.readLong();
        DictId dictId = new DictId(kind, id);

        if (manager != null)
        {
            CompressionDictionary dictionary = manager.get(dictId);
            if (dictionary != null)
            {
                return dictionary;
            }
        }

        int length = input.readInt();
        byte[] dict = new byte[length];
        input.readFully(dict);
        int checksum = input.readInt();
        int calculatedChecksum = calculateChecksum((byte) kindOrdinal, id, dict);
        if (checksum != calculatedChecksum)
            throw new IOException("Compression dictionary checksum does not match. " +
                                  "Expected: " + checksum + "; actual: " + calculatedChecksum);

        CompressionDictionary dictionary = kind.createDictionary(dictId, dict);

        // update the dictionary manager if it exists
        if (manager != null)
        {
            manager.add(dictionary);
        }

        return dictionary;
    }

    static LightweightCompressionDictionary createFromRowLightweight(UntypedResultSet.Row row)
    {
        String kindStr = row.getString("kind");
        long dictId = row.getLong("dict_id");
        int checksum = row.getInt("checksum");
        int size = row.getInt("size");

        try
        {
            return new LightweightCompressionDictionary(new DictId(CompressionDictionary.Kind.valueOf(kindStr), dictId),
                                                        checksum, size);
        }
        catch (IllegalArgumentException ex)
        {
            throw new IllegalStateException(kindStr + " compression dictionary is not created for dict id " + dictId);
        }
    }

    static CompressionDictionary createFromRow(UntypedResultSet.Row row)
    {
        String kindStr = row.getString("kind");
        long dictId = row.getLong("dict_id");
        int checksum = row.getInt("checksum");
        int size = row.getInt("size");

        try
        {
            Kind kind = CompressionDictionary.Kind.valueOf(kindStr);
            return kind.createDictionary(new DictId(kind, dictId), row.getByteArray("dict"), checksum, size);
        }
        catch (IllegalArgumentException ex)
        {
            throw new IllegalStateException(kindStr + " compression dictionary is not created for dict id " + dictId);
        }
    }

    @SuppressWarnings("UnstableApiUsage")
    static int calculateChecksum(byte kindOrdinal, long dictId, byte[] dict)
    {
        Hasher hasher = Hashing.crc32c().newHasher();
        hasher.putByte(kindOrdinal);
        hasher.putLong(dictId);
        hasher.putBytes(dict);
        return hasher.hash().asInt();
    }

    // Defines the compression dictionary kind, as well as serves as a factory for creating components of dictionary compression
    enum Kind
    {
        // Order matters: the enum ordinal is serialized
        ZSTD
        {
            public CompressionDictionary createDictionary(DictId dictId, byte[] dict)
            {
                return new ZstdCompressionDictionary(dictId, dict);
            }

            @Override
            public CompressionDictionary createDictionary(DictId dictId, byte[] dict, int checksum, int size)
            {
                return new ZstdCompressionDictionary(dictId, dict, checksum, size);
            }

            @Override
            public ICompressor createCompressor(CompressionDictionary dictionary)
            {
                Preconditions.checkArgument(dictionary instanceof ZstdCompressionDictionary,
                                            "Expected dictionary to be ZstdCompressionDictionary; actual: %s",
                                            dictionary.getClass().getSimpleName());
                return ZstdDictionaryCompressor.create((ZstdCompressionDictionary) dictionary);
            }

            @Override
            public ICompressionDictionaryTrainer createTrainer(String keyspaceName,
                                                               String tableName,
                                                               CompressionDictionaryTrainingConfig config,
                                                               ICompressor compressor)
            {
                Preconditions.checkArgument(compressor instanceof ZstdDictionaryCompressor,
                                            "Expected compressor to be ZstdDictionaryCompressor; actual: %s",
                                            compressor.getClass().getSimpleName());
                return new ZstdDictionaryTrainer(keyspaceName, tableName, config, ((ZstdDictionaryCompressor) compressor).compressionLevel());
            }
        };

        /**
         * Creates a compression dictionary instance for this kind
         *
         * @param dictId the dictionary identifier
         * @param dict the raw dictionary bytes
         * @return a compression dictionary instance
         */
        public abstract CompressionDictionary createDictionary(CompressionDictionary.DictId dictId, byte[] dict);

        /**
         * Creates a compression dictionary instance for this kind
         *
         * @param dictId the dictionary identifier
         * @param dict the raw dictionary bytes
         * @param checksum checksum of this dictionary
         * @param size size of raw dictionary bytes
         * @return a compression dictionary instance
         */
        public abstract CompressionDictionary createDictionary(CompressionDictionary.DictId dictId, byte[] dict, int checksum, int size);

        /**
         * Creates a dictionary compressor for this kind
         *
         * @param dictionary the compression dictionary to use for compression
         * @return a dictionary compressor instance
         */
        public abstract ICompressor createCompressor(CompressionDictionary dictionary);

        /**
         * Creates a dictionary trainer for this kind
         *
         * @param keyspaceName the keyspace name
         * @param tableName the table name
         * @param config the training configuration
         * @param compressor the compressor to use for training
         * @return a dictionary trainer instance
         */
        public abstract ICompressionDictionaryTrainer createTrainer(String keyspaceName,
                                                                    String tableName,
                                                                    CompressionDictionaryTrainingConfig config,
                                                                    ICompressor compressor);
    }

    final class DictId
    {
        public final Kind kind;
        public final long id; // A value of negative or 0 means no dictionary

        public DictId(Kind kind, long id)
        {
            this.kind = kind;
            this.id = id;
        }

        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof DictId)) return false;
            DictId dictId = (DictId) o;
            return id == dictId.id && kind == dictId.kind;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(kind, id);
        }

        @Override
        public String toString()
        {
            return "DictId{" +
                   "kind=" + kind +
                   ", id=" + id +
                   '}';
        }
    }

    /**
     * The purpose of lightweight dictionary is to not carry the actual dictionary bytes for performance reasons.
     * Handy for situations when retrieval from the database does not need to contain dictionary
     * or the instatiation of a proper dictionary object is not desirable or unnecessary for other,
     * mostly performance-related, reasons.
     */
    class LightweightCompressionDictionary implements CompressionDictionary
    {
        private final DictId dictId;
        private final int checksum;
        private final int size;

        public LightweightCompressionDictionary(DictId dictId, int checksum, int size)
        {
            this.dictId = dictId;
            this.checksum = checksum;
            this.size = size;
        }

        @Override
        public DictId dictId()
        {
            return dictId;
        }

        @Override
        public byte[] rawDictionary()
        {
            return null;
        }

        @Override
        public int checksum()
        {
            return checksum;
        }

        @Override
        public int size()
        {
            return size;
        }

        @Override
        public void serialize(DataOutput out) throws IOException
        {
            throw new UnsupportedOperationException("Lightweight compression dictionary is not meant to be serialized.");
        }

        @Override
        public void close() throws Exception
        {
            // intentionally empty
        }
    }
}
