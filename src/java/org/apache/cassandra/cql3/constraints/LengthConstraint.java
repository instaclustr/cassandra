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

import com.google.common.collect.Sets;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.ColumnMetadata;

import java.nio.ByteBuffer;
import java.util.Set;

public class LengthConstraint implements ConstraintFunction
{
    public static final String FUNCTION_NAME = "LENGTH";
    private static final Set<Class<?>> SUPPORTED_TYPES = Sets.newHashSet(UTF8Type.class,
                                                                         AsciiType.class,
                                                                         BytesType.class);

    private final ColumnIdentifier columnName;

    public LengthConstraint(ColumnIdentifier columnName)
    {
        this.columnName = columnName;
    }

    @Override
    public String getName()
    {
        return FUNCTION_NAME;
    }

    @Override
    public void evaluate(Class<? extends AbstractType> valueType, Operator relationType, String term, Object columnValue)
    {
        int valueLength = getValueSize(columnValue, valueType);
        int sizeConstraint = Integer.parseInt(term);

        switch (relationType)
        {
            case EQ:
                if (valueLength != sizeConstraint)
                    throw new ConstraintViolationException(columnName + " value length should be exactly " + sizeConstraint);
                break;
            case NEQ:
                if (valueLength == sizeConstraint)
                    throw new ConstraintViolationException(columnName + " value length should be different from " + sizeConstraint);
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
    public void validate(ColumnMetadata columnMetadata)
    {
        Class<? extends AbstractType> valueType = columnMetadata.type.getClass();
        if (!SUPPORTED_TYPES.contains(valueType))
            throw invalidConstraintDefinitionException(valueType);
    }

    private int getValueSize(Object value, Class<? extends AbstractType> valueType)
    {
        if (valueType == BytesType.class)
        {
            ByteBuffer bb = (ByteBuffer) value;
            return bb.remaining();
        }

        if (valueType == AsciiType.class || valueType == UTF8Type.class)
            return ((String) value).length();

        throw invalidConstraintDefinitionException(valueType);
    }

    private InvalidConstraintDefinitionException invalidConstraintDefinitionException(Class<? extends AbstractType> valueType)
    {
        throw new InvalidConstraintDefinitionException("Column type not supported. " +
                                                       "Supported types are " + SUPPORTED_TYPES +
                                                       ", given type " + valueType);
    }
}
