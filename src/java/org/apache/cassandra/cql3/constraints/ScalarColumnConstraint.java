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


import java.io.IOException;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;

public class ScalarColumnConstraint implements ColumnConstraint<ScalarColumnConstraint>
{
    public final ColumnIdentifier param;
    public final Operator relationType;
    public final String term;

    final static Serializer serializer = new Serializer();

    public final static class Raw
    {
        public final ColumnIdentifier param;
        public final Operator relationType;
        public final String term;

        public Raw(ColumnIdentifier param, Operator relationType, String term)
        {
            this.param = param;
            this.relationType = relationType;
            this.term = term;
        }

        public ScalarColumnConstraint prepare()
        {
            return new ScalarColumnConstraint(param, relationType, term);
        }
    }

    private ScalarColumnConstraint(ColumnIdentifier param, Operator relationType, String term)
    {
        this.param = param;
        this.relationType = relationType;
        this.term = term;
    }

    @Override
    public void evaluate(Class<? extends AbstractType> valueType, Object columnValue)
    {
        Number columnValueNumber;
        float sizeConstraint;

        try
        {
            columnValueNumber = (Number) columnValue;
            sizeConstraint = Float.parseFloat(term);
        }
        catch (NumberFormatException exception)
        {
            throw new ConstraintViolationException(param + " and " + term + " need to be numbers.");
        }

        switch (relationType)
        {
            case EQ:
                if (Float.compare(columnValueNumber.floatValue(), sizeConstraint) != 0)
                    throw new ConstraintViolationException(param + " value should be exactly " + sizeConstraint);
                break;
            case NEQ:
                if (Double.compare(columnValueNumber.floatValue(), sizeConstraint) == 0)
                    throw new ConstraintViolationException(param + " value should be different from " + sizeConstraint);
                break;
            case GT:
                if (columnValueNumber.floatValue() <= sizeConstraint)
                    throw new ConstraintViolationException(param + " value should be larger than " + sizeConstraint);
                break;
            case LT:
                if (columnValueNumber.floatValue() >= sizeConstraint)
                    throw new ConstraintViolationException(param + " value should be smaller than " + sizeConstraint);
                break;
            case GTE:
                if (columnValueNumber.floatValue() < sizeConstraint)
                    throw new ConstraintViolationException(param + " value should be larger or equal than " + sizeConstraint);
                break;
            case LTE:
                if (columnValueNumber.floatValue() > sizeConstraint)
                    throw new ConstraintViolationException(param + " value should be smaller or equal than " + sizeConstraint);
                break;
            default:
                throw new ConstraintViolationException("Invalid relation type: " + relationType);
        }
    }

    @Override
    public void validate(ColumnMetadata columnMetadata) throws InvalidConstraintDefinitionException
    {
        if (!columnMetadata.type.isNumber())
            throw new InvalidConstraintDefinitionException(param + " is not a number");
    }

    @Override
    public String toString()
    {
        return param + " " + relationType + " " + term;
    }

    @Override
    public IVersionedSerializer<ScalarColumnConstraint> serializer()
    {
        return serializer;
    }

    @Override
    public void appendCqlTo(CqlBuilder builder)
    {
        builder.append(toString());
    }

    private static class Serializer implements IVersionedSerializer<ScalarColumnConstraint>
    {
        @Override
        public void serialize(ScalarColumnConstraint columnConstraint, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(columnConstraint.param.toString());
            out.writeUTF(columnConstraint.relationType.toString());
            out.writeUTF(columnConstraint.term);
        }

        @Override
        public ScalarColumnConstraint deserialize(DataInputPlus in, int version) throws IOException
        {
            ColumnIdentifier param = new ColumnIdentifier(in.readUTF(), true);
            Operator relationType = Operator.valueOf(in.readUTF());
            return new ScalarColumnConstraint(param, relationType, in.readUTF());
        }

        @Override
        public long serializedSize(ScalarColumnConstraint columnConstraint, int version)
        {
            return TypeSizes.sizeof(columnConstraint.term)
                   + TypeSizes.sizeof(columnConstraint.relationType.toString())
                   + TypeSizes.sizeof(columnConstraint.param.toString());
        }
    }
}
