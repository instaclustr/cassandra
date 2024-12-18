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

package org.apache.cassandra.cql3;


import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;


public class ColumnConstraintScalar implements ColumnConstraint
{
    public final ColumnIdentifier param;
    public final Operator relationType;
    public final String term;

    public final static Serializer serializer = new Serializer();

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

        public ColumnConstraintScalar prepare()
        {
            return new ColumnConstraintScalar(param, relationType, term);
        }
    }

    public ColumnConstraintScalar(ColumnIdentifier param, Operator relationType, String term)
    {
        this.param = param;
        this.relationType = relationType;
        this.term = term;
    }

    public void evaluate(Object columnValue)
    {
        Number columnValueNumber;
        float sizeConstraint;

        try
        {
            columnValueNumber = (Number) columnValue;
            sizeConstraint = Float.parseFloat(term);
        }
        catch (final NumberFormatException exception)
        {
            throw new ConstraintViolationException(param + " and " + term + " need to be numbers.");
        }

        switch (relationType)
        {
            case EQ:
                if (Float.compare(columnValueNumber.floatValue(), sizeConstraint) != 0)
                    throw new ConstraintViolationException(param + " value length should be exactly " + sizeConstraint);
                break;
            case NEQ:
                if (Double.compare(columnValueNumber.floatValue(), sizeConstraint) == 0)
                    throw new ConstraintViolationException(param + " value length different than " + sizeConstraint);
                break;
            case GT:
                if (columnValueNumber.floatValue() <= sizeConstraint)
                    throw new ConstraintViolationException(param + " value length should be larger than " + sizeConstraint);
                break;
            case LT:
                if (columnValueNumber.floatValue() >= sizeConstraint)
                    throw new ConstraintViolationException(param + " value length should be smaller than " + sizeConstraint);
                break;
            case GTE:
                if (columnValueNumber.floatValue() < sizeConstraint)
                    throw new ConstraintViolationException(param + " value length should be larger or equal than " + sizeConstraint);
                break;
            case LTE:
                if (columnValueNumber.floatValue() > sizeConstraint)
                    throw new ConstraintViolationException(param + " value length should be smaller or equal than " + sizeConstraint);
                break;
            default:
                throw new ConstraintViolationException("Invalid relation type: " + relationType);
        }
    }

    @Override
    public void validate(ColumnMetadata columnMetadata, TableMetadata tableMetadata) throws ConstraintInvalidException
    {
        if (!(columnMetadata.type instanceof org.apache.cassandra.db.marshal.NumberType))
            throw new ConstraintInvalidException(param + " is not a number");
    }

    @Override
    public String toString()
    {
        return String.format("%s %s %s", param, relationType, term);
    }

    @Override
    public IVersionedSerializer<ColumnConstraint> getSerializer()
    {
        return serializer;
    }

    @Override
    public void appendCqlTo(CqlBuilder builder) {
        builder.append(toString());
    }

    public static class Serializer implements IVersionedSerializer<ColumnConstraint>
    {
        @Override
        public void serialize(ColumnConstraint columnConstraint, DataOutputPlus out, int version) throws IOException
        {
            ColumnConstraintScalar condition = (ColumnConstraintScalar) columnConstraint;
            out.writeUTF(condition.param.toString());
            out.writeUTF(condition.relationType.toString());
            out.writeUTF(condition.term);
        }

        @Override
        public ColumnConstraint deserialize(DataInputPlus in, int version) throws IOException
        {
            ColumnIdentifier param = new ColumnIdentifier(in.readUTF(), true);
            Operator relationType = Operator.valueOf(in.readUTF());
            return new ColumnConstraintScalar(param, relationType, in.readUTF());
        }

        @Override
        public long serializedSize(ColumnConstraint columnConstraint, int version)
        {
            ColumnConstraintScalar condition = (ColumnConstraintScalar) columnConstraint;
            return TypeSizes.sizeof(condition.term)
                   + TypeSizes.sizeof(condition.relationType.toString())
                   + TypeSizes.sizeof(condition.param.toString());
        }
    }
}
