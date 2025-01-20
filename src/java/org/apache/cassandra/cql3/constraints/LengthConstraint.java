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

import java.nio.ByteBuffer;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

public class LengthConstraint implements ConstraintFunction
{
    private static final AbstractType<?>[] SUPPORTED_TYPES = new AbstractType[] { BytesType.instance, UTF8Type.instance, AsciiType.instance };

    public static final String FUNCTION_NAME = "LENGTH";

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

        ByteBuffer buffera = ByteBufferUtil.bytes(valueLength);
        ByteBuffer bufferb = ByteBufferUtil.bytes(sizeConstraint);

        if (!relationType.isSatisfiedBy(Int32Type.instance, buffera, bufferb))
            throw new ConstraintViolationException(columnName + " does not satisfy length constraint. "
                                                   + valueLength + " should be " + relationType + ' ' + term);
    }

    @Override
    public void validate(ColumnMetadata columnMetadata)
    {
        boolean supported = false;
        AbstractType<?> unwrapped = columnMetadata.type.unwrap();
        for (AbstractType<?> supportedType : SUPPORTED_TYPES)
        {
            if (supportedType == unwrapped)
            {
                supported = true;
                break;
            }
        }

        if (!supported)
            throw invalidConstraintDefinitionException(columnMetadata.type.getClass());
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
        throw new InvalidConstraintDefinitionException("Column type " + valueType + " is not supported.");
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof LengthConstraint))
            return false;

        LengthConstraint other = (LengthConstraint) o;

        return columnName.equals(other.columnName);
    }
}
