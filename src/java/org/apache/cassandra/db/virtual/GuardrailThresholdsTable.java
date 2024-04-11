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

import static java.lang.String.format;
import static org.apache.cassandra.schema.SchemaConstants.VIRTUAL_GUARDRAILS;

public class GuardrailThresholdsTable extends AbstractMutableVirtualTable
{
    public static final String TABLE_NAME = "thresholds";

    public static final String NAME_COLUMN = "name";
    public static final String WARN_COLUMN = "warn";
    public static final String FAIL_COLUMN = "fail";

    public GuardrailThresholdsTable()
    {
        this(VIRTUAL_GUARDRAILS);
    }

    public GuardrailThresholdsTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, TABLE_NAME)
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

        for (Map.Entry<String, Pair<BiConsumer<Number, Number>, Pair<Supplier<Number>, Supplier<Number>>>> entry : Guardrails.getThresholdGuardails().entrySet())
        {
            String guardrailName = entry.getKey();
            Pair<Supplier<Number>, Supplier<Number>> getter = entry.getValue().right;

            if (getter == null)
                continue;

            result.row(guardrailName)
                  .column(WARN_COLUMN, getter.left.get().longValue())
                  .column(FAIL_COLUMN, getter.right.get().longValue());
        }

        return result;
    }

    @Override
    public void apply(PartitionUpdate update)
    {
        ColumnValues partitionKey = ColumnValues.from(metadata(), update.partitionKey());

        String key = partitionKey.value(0);

        Pair<BiConsumer<Number, Number>, Pair<Supplier<Number>, Supplier<Number>>> setterAndGetter = Guardrails.getThresholdGuardails().get(key);
        if (setterAndGetter == null)
            throw new InvalidRequestException(format("there is no such guardrail with name %s", key));

        Iterator<Row> iterator = update.iterator();
        Row row = iterator.next();

        Cell<?> warnCell = row.getCell(ColumnMetadata.regularColumn(metadata().keyspace, metadata().name, WARN_COLUMN, IntegerType.instance));
        Cell<?> failCell = row.getCell(ColumnMetadata.regularColumn(metadata().keyspace, metadata().name, FAIL_COLUMN, IntegerType.instance));

        if (warnCell == null || failCell == null)
            throw new InvalidRequestException("both warn and fail columns must be specified for updates");

        Long warnValue = ColumnValue.from(warnCell).value();
        Long failValue = ColumnValue.from(failCell).value();

        BiConsumer<Number, Number> setter = setterAndGetter.left;
        if (setter == null)
            throw new InvalidRequestException(format("There is not any associated setter for guardrail %s", key));

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
