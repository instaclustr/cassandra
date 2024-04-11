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

import java.util.Iterator;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Pair;

public class GuardrailThresholdsTable extends AbstractMutableVirtualTable
{
    private static final Logger logger = LoggerFactory.getLogger(GuardrailThresholdsTable.class);

    private static final String NAME_COLUMN = "name";
    private static final String WARN_COLUMN = "warn";
    private static final String FAIL_COLUMN = "fail";

    public GuardrailThresholdsTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "guardrails_threshold")
                           .comment("Guardrails configuration table for thresholds")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(NAME_COLUMN, UTF8Type.instance)
                           .addRegularColumn(WARN_COLUMN, LongType.instance)
                           .addRegularColumn(FAIL_COLUMN, LongType.instance)
                           .build());
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        for (Map.Entry<String, Pair<BiConsumer<Number, Number>, Supplier<Pair<Number, Number>>>> entries : Guardrails.thresholds.entrySet())
        {
            String guardrailName = entries.getKey();
            Pair<Number, Number> thresholds = entries.getValue().right.get();
            result.row(guardrailName).column(WARN_COLUMN, thresholds.left.longValue()).column(FAIL_COLUMN, thresholds.right.longValue());
        }

        return result;
    }

    @Override
    public void apply(PartitionUpdate update)
    {
        ColumnValues partitionKey = ColumnValues.from(metadata(), update.partitionKey());

        String key = partitionKey.value(0);

        Pair<BiConsumer<Number, Number>, Supplier<Pair<Number, Number>>> setterAndGetter = Guardrails.thresholds.get(key);
        if (setterAndGetter == null)
            throw new InvalidRequestException(String.format("there is no such guardrail with name %s", partitionKey.value(0)));

        Iterator<Row> iterator = update.iterator();
        Row row = iterator.next();

        Cell<?> warnCell = row.getCell(ColumnMetadata.regularColumn(metadata().keyspace, metadata().name, WARN_COLUMN, IntegerType.instance));
        Cell<?> failCell = row.getCell(ColumnMetadata.regularColumn(metadata().keyspace, metadata().name, FAIL_COLUMN, IntegerType.instance));

        if (warnCell == null || failCell == null)
            throw new InvalidRequestException("both warn and fail columns must be specified for updates");

        Long warnValue = ColumnValue.from(warnCell).value();
        Long failValue = ColumnValue.from(failCell).value();

        BiConsumer<Number, Number> setter = setterAndGetter.left;
        try
        {
            setter.accept(warnValue, failValue);
        }
        catch (Exception ex)
        {
            throw new InvalidRequestException(ex.getMessage());
        }
    }
}
