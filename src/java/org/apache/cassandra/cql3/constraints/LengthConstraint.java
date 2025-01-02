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


import java.util.Set;

import com.google.common.collect.Sets;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

public class LengthConstraint implements ConstraintFunction
{
    public static String FUNCTION_NAME = "LENGTH";

    @Override
    public String getName()
    {
        return FUNCTION_NAME;
    }

    private static Set SUPPORTED_TYPES = Sets.newHashSet(UTF8Type.class, AsciiType.class, BytesType.class);

    @Override
    public void evaluate(ColumnIdentifier columnName, Operator relationType, String term, Object columnValue)
    {
        int valueLength = stripColumnValue((String) columnValue).length();
        int sizeConstraint = Integer.parseInt(term);

        switch (relationType)
        {
            case EQ:
                if (valueLength != sizeConstraint)
                    throw new ConstraintViolationException(columnName + " value length should be exactly " + sizeConstraint);
                break;
            case NEQ:
                if (valueLength == sizeConstraint)
                    throw new ConstraintViolationException(columnName + " value length different than " + sizeConstraint);
                break;
            case GT:
                if (valueLength <= sizeConstraint)
                    throw new ConstraintViolationException(columnName + " value length should be larger than " + sizeConstraint);
                break;
            case LT:
                if (valueLength >= sizeConstraint)
                    throw new ConstraintViolationException(columnName + " value length should be smaller than " + sizeConstraint);
                break;
            case GTE:
                if (valueLength < sizeConstraint)
                    throw new ConstraintViolationException(columnName + " value length should be larger or equal than " + sizeConstraint);
                break;
            case LTE:
                if (valueLength > sizeConstraint)
                    throw new ConstraintViolationException(columnName + " value length should be smaller or equala than " + sizeConstraint);
                break;
            default:
                throw new ConstraintViolationException("Invalid relation type: " + relationType);
        }
    }

    @Override
    public void validate(ColumnIdentifier columnName, Operator relationType, String term, TableMetadata tableMetadata)
    {
        ColumnMetadata columnMetadata = tableMetadata.getColumn(columnName);
        if (!SUPPORTED_TYPES.contains(columnMetadata.type.getClass()))
            throw new InvalidConstraintDefinitionException("Column type not supported");
    }

    /**
     * Removes initial and ending quotes from a column value
     *
     * @param columnValue
     * @return
     */
    private String stripColumnValue(String columnValue)
    {
        if (columnValue.startsWith("'") && columnValue.endsWith("'"))
            return columnValue.substring(1, columnValue.length() - 1);
        return columnValue;
    }
}
