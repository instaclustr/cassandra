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

package org.apache.cassandra.db.virtual.walker;

import org.apache.cassandra.db.virtual.model.Column;
import org.apache.cassandra.db.virtual.model.StorageOperationsRow;
import org.apache.cassandra.utils.TimeUUID;

/**
 * The {@link StorageOperationsRow} row metadata and data walker.
 *
 * @see StorageOperationsRow
 */
public class StorageOperationsWalker implements RowWalker<StorageOperationsRow>
{
    @Override
    public void visitMeta(MetadataVisitor visitor)
    {
        visitor.accept(Column.Type.PARTITION_KEY, "operation_id", TimeUUID.class);
        visitor.accept(Column.Type.REGULAR, "operation_type", String.class);
        visitor.accept(Column.Type.REGULAR, "keyspaces", String.class);
        visitor.accept(Column.Type.REGULAR, "tables", String.class);
        visitor.accept(Column.Type.REGULAR, "operation_result", String.class);
        visitor.accept(Column.Type.REGULAR, "operation_result_by_table", String.class);
        visitor.accept(Column.Type.REGULAR, "processed_by_keyspace", String.class);
    }

    @Override
    public void visitRow(StorageOperationsRow row, RowMetadataVisitor visitor)
    {
        visitor.accept(Column.Type.PARTITION_KEY, "operation_id", TimeUUID.class, row::operationId);
        visitor.accept(Column.Type.REGULAR, "operation_type", String.class, row::operationType);
        visitor.accept(Column.Type.REGULAR, "keyspaces", String.class, row::keyspaces);
        visitor.accept(Column.Type.REGULAR, "tables", String.class, row::tables);
        visitor.accept(Column.Type.REGULAR, "operation_result", String.class, row::operationResult);
        visitor.accept(Column.Type.REGULAR, "operation_result_by_table", String.class, row::operationResultByTable);
        visitor.accept(Column.Type.REGULAR, "processed_by_keyspace", String.class, row::processedByKeyspace);
    }

    @Override
    public int count(Column.Type type)
    {
        switch (type)
        {
            case PARTITION_KEY:
                return 1;
            case CLUSTERING:
                return 0;
            case REGULAR:
                return 6;
            default:
                throw new IllegalStateException("Unknown column type: " + type);
        }
    }
}
