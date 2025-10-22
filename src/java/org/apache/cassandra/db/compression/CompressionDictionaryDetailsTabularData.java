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

import javax.management.openmbean.ArrayType;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

public class CompressionDictionaryDetailsTabularData
{
    public static final String KEYSPACE_NAME = "Keyspace";
    public static final String TABLE_NAME = "Table";
    public static final String DICT_ID_NAME = "DictId";
    public static final String DICT_NAME = "Dict";
    public static final String KIND_NAME = "Kind";
    public static final String CHECKSUM_NAME = "Checksum";
    public static final String SIZE_NAME = "Size";


    private static final String[] ITEM_NAMES = new String[]{ KEYSPACE_NAME,
                                                             TABLE_NAME,
                                                             DICT_ID_NAME,
                                                             DICT_NAME,
                                                             KIND_NAME,
                                                             CHECKSUM_NAME,
                                                             SIZE_NAME };

    private static final String[] ITEM_DESCS = new String[]{ "keyspace",
                                                             "table",
                                                             "dictionary_id",
                                                             "dictionary_bytes",
                                                             "kind",
                                                             "checksum",
                                                             "size" };

    private static final String TYPE_NAME = "DictionaryDetails";
    private static final String ROW_DESC = "DictionaryDetails";
    private static final OpenType<?>[] ITEM_TYPES;
    private static final CompositeType COMPOSITE_TYPE;
    public static final TabularType TABULAR_TYPE;

    static
    {
        try
        {
            ITEM_TYPES = new OpenType[]{ SimpleType.STRING, // keyspace
                                         SimpleType.STRING, // table
                                         SimpleType.LONG, // dict id
                                         new ArrayType<String[]>(SimpleType.BYTE, true), // dict bytes
                                         SimpleType.STRING, // kind
                                         SimpleType.INTEGER, // checksum
                                         SimpleType.INTEGER }; // size of dict bytes

            COMPOSITE_TYPE = new CompositeType(TYPE_NAME, ROW_DESC, ITEM_NAMES, ITEM_DESCS, ITEM_TYPES);
            TABULAR_TYPE = new TabularType(TYPE_NAME, ROW_DESC, COMPOSITE_TYPE, ITEM_NAMES);
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void from(String keyspace,
                            String table,
                            CompressionDictionary dictionary,
                            TabularDataSupport result)
    {
        result.put(from(keyspace, table, dictionary));
    }

    public static CompositeData from(String keyspace, String table, CompressionDictionary dictionary)
    {
        try
        {
            return new CompositeDataSupport(COMPOSITE_TYPE,
                                            ITEM_NAMES,
                                            new Object[]
                                            {
                                            keyspace,
                                            table,
                                            dictionary.dictId().id,
                                            dictionary.rawDictionary(),
                                            dictionary.kind().name(),
                                            // why am I computing this here? It should be stored already, 0 for now
                                            dictionary.rawDictionary() == null ? 0 :
                                            CompressionDictionary.calculateChecksum((byte) dictionary.kind().ordinal(),
                                                                                    dictionary.dictId().id,
                                                                                    dictionary.rawDictionary()),
                                            // why am I computing this here? It should be stored already, 0 for now
                                            dictionary.rawDictionary() == null ? 0 : dictionary.rawDictionary().length,
                                            });
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static CompositeData from(CompressionDictionaryPojo pojo)
    {
        try
        {
            return new CompositeDataSupport(COMPOSITE_TYPE,
                                            ITEM_NAMES,
                                            new Object[]
                                            {
                                            pojo.keyspace,
                                            pojo.table,
                                            pojo.dictId,
                                            pojo.dict,
                                            pojo.kind,
                                            pojo.checksum,
                                            pojo.size
                                            });
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Deserializes data to convenience object to work further with.
     *
     * @param compositeData data to create pojo from
     * @return deserialized composite data to convenience object
     * @throws IllegalArgumentException if values in deserialized object are invalid.
     * @see CompressionDictionaryPojo#validate()
     */
    public static CompressionDictionaryPojo from(CompositeData compositeData)
    {
        String keyspace = (String) compositeData.get(CompressionDictionaryDetailsTabularData.KEYSPACE_NAME);
        String table = (String) compositeData.get(CompressionDictionaryDetailsTabularData.TABLE_NAME);
        long dictId = (Long) compositeData.get(CompressionDictionaryDetailsTabularData.DICT_ID_NAME);
        byte[] dictionaryBytes = (byte[]) compositeData.get(CompressionDictionaryDetailsTabularData.DICT_NAME);
        String kind = (String) compositeData.get(CompressionDictionaryDetailsTabularData.KIND_NAME);
        int checksum = (Integer) compositeData.get(CompressionDictionaryDetailsTabularData.CHECKSUM_NAME);
        int size = (Integer) compositeData.get(CompressionDictionaryDetailsTabularData.SIZE_NAME);

        CompressionDictionaryPojo pojo = new CompressionDictionaryPojo();
        pojo.keyspace = keyspace;
        pojo.table = table;
        pojo.dictId = dictId;
        pojo.dict = dictionaryBytes;
        pojo.kind = kind;
        pojo.checksum = checksum;
        pojo.size = size;

        pojo.validate();

        return pojo;
    }

    public static class CompressionDictionaryPojo
    {
        public String keyspace;
        public String table;
        public long dictId;
        public byte[] dict;
        public String kind;
        public int checksum;
        public int size;

        /**
         * Dictionary is valid if, keyspace and table are specified, dictionary id is strictly positive integer,
         * dictionary byte array is not nor not empty,
         * kind corresponds to {@code Kind}, checksum and size are bigger than 0.
         */
        public void validate()
        {
            if (keyspace == null)
                throw new IllegalArgumentException();
            if (table == null)
                throw new IllegalArgumentException();
            if (dictId <= 0)
                throw new IllegalArgumentException("Provided dictionary id is lower than 0, it is '" + dictId + "'.'");
            if (dict == null || dict.length == 0)
                throw new IllegalArgumentException("Provided dictionary byte array is null or empty.");
            if (kind == null)
                throw new IllegalArgumentException("Provided kind is null.");
            try
            {
                CompressionDictionary.Kind.valueOf(kind);
            }
            catch (IllegalArgumentException ex)
            {
                throw new IllegalArgumentException("There is no such Kind as '" + kind + "'.");
            }
            if (checksum <= 0)
                throw new IllegalArgumentException("Checksum has to be strictly positive number, it is '" + checksum + "'.");
            if (size <= 0)
                throw new IllegalArgumentException("Size has to be strictly positive number, it is '" + size + "'.");
        }
    }
}
