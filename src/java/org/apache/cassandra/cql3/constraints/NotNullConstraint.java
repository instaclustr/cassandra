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

package org.apache.cassandra.cql3.constraints;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.schema.ColumnMetadata;

public class NotNullConstraint extends ConstraintFunction
{
    public static final String FUNCTION_NAME = "NOT_NULL";

    private final ColumnIdentifier columnName;

    public NotNullConstraint(ColumnIdentifier columnName)
    {
        this.columnName = columnName;
    }

    @Override
    public String getName()
    {
        return FUNCTION_NAME;
    }

    @Override
    public String getColumnName()
    {
        return columnName.toString();
    }

    @Override
    public void validate(ColumnMetadata columnMetadata) throws InvalidConstraintDefinitionException
    {
        // every column is valid to check against nullity
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof NotNullConstraint))
            return false;

        NotNullConstraint other = (NotNullConstraint) o;

        return columnName.equals(other.columnName);
    }
}
