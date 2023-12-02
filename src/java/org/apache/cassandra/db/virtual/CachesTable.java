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
package org.apache.cassandra.db.virtual;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.metrics.CacheMetrics;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.Pair;

final class CachesTable extends AbstractCacheTable<CacheMetrics>
{
    public static final String TABLE_NAME = "caches";
    public static final String TABLE_DESCRIPTION = "system caches";

    public static final String NAME_COLUMN = "name";
    public static final String CAPACITY_BYTES_COLUMN = "capacity_bytes";
    public static final String SIZE_BYTES_COLUMN = "size_bytes";
    public static final String ENTRY_COUNT_COLUMN = "entry_count";
    public static final String REQUEST_COUNT_COLUMN = "request_count";
    public static final String HIT_COUNT_COLUMN = "hit_count";
    public static final String HIT_RATIO_COLUMN = "hit_ratio";
    public static final String RECENT_REQUEST_RATE_PER_SECOND_COLUMN = "recent_request_rate_per_second";
    public static final String RECENT_HIT_RATE_PER_SECOND_COLUMN = "recent_hit_rate_per_second";

    private static final Collection<Supplier<Optional<Pair<String, CacheMetrics>>>> DEFAULT_METRICS_SUPPLIERS = Set.of(
    () -> Optional.ofNullable(ChunkCache.instance).map(instance -> Pair.create("chunks", instance.metrics)),
    () -> Optional.of(Pair.create("counters", CacheService.instance.counterCache.getMetrics())),
    () -> Optional.of(Pair.create("keys", CacheService.instance.keyCache.getMetrics())),
    () -> Optional.of(Pair.create("rows", CacheService.instance.rowCache.getMetrics()))
    );

    @VisibleForTesting
    CachesTable(String keyspace, Collection<Supplier<Optional<Pair<String, CacheMetrics>>>> metricsSuppliers)
    {
        super(TableMetadata.builder(keyspace, TABLE_NAME)
                           .comment(TABLE_DESCRIPTION)
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(NAME_COLUMN, UTF8Type.instance)
                           .addRegularColumn(CAPACITY_BYTES_COLUMN, LongType.instance)
                           .addRegularColumn(SIZE_BYTES_COLUMN, LongType.instance)
                           .addRegularColumn(ENTRY_COUNT_COLUMN, Int32Type.instance)
                           .addRegularColumn(REQUEST_COUNT_COLUMN, LongType.instance)
                           .addRegularColumn(HIT_COUNT_COLUMN, LongType.instance)
                           .addRegularColumn(HIT_RATIO_COLUMN, DoubleType.instance)
                           .addRegularColumn(RECENT_REQUEST_RATE_PER_SECOND_COLUMN, LongType.instance)
                           .addRegularColumn(RECENT_HIT_RATE_PER_SECOND_COLUMN, LongType.instance)
                           .build(),
              metricsSuppliers);
    }


    CachesTable(String keyspace)
    {
        this(keyspace, DEFAULT_METRICS_SUPPLIERS);
    }

    @Override
    protected void addRow(SimpleDataSet result, String name, CacheMetrics metrics)
    {
        result.row(name)
              .column(CAPACITY_BYTES_COLUMN, metrics.capacity.getValue())
              .column(SIZE_BYTES_COLUMN, metrics.size.getValue())
              .column(ENTRY_COUNT_COLUMN, metrics.entries.getValue())
              .column(REQUEST_COUNT_COLUMN, metrics.requests.getCount())
              .column(HIT_COUNT_COLUMN, metrics.hits.getCount())
              .column(HIT_RATIO_COLUMN, metrics.hitRate.getValue())
              .column(RECENT_REQUEST_RATE_PER_SECOND_COLUMN, (long) metrics.requests.getFifteenMinuteRate())
              .column(RECENT_HIT_RATE_PER_SECOND_COLUMN, (long) metrics.hits.getFifteenMinuteRate());
    }
}
