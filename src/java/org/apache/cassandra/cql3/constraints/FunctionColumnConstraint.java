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

public class FunctionColumnConstraint implements ColumnConstraint
{
    public static final Serializer serializer = new Serializer();

    public final ConstraintFunction function;
    public final ColumnIdentifier columnName;
    public final Operator relationType;
    public final String term;

    public final static class Raw
    {
        public final ConstraintFunction function;
        public final ColumnIdentifier columnName;
        public final Operator relationType;
        public final String term;

        public Raw(ColumnIdentifier functionName, ColumnIdentifier columnName, Operator relationType, String term)
        {
            this.relationType = relationType;
            this.columnName = columnName;
            this.term = term;
            function = createConstraintFunction(functionName.toCQLString(), columnName);
        }

        public FunctionColumnConstraint prepare()
        {
            return new FunctionColumnConstraint(function, columnName, relationType, term);
        }
    }

    private static ConstraintFunction createConstraintFunction(String functionName, ColumnIdentifier columnName)
    {
        if (LengthConstraint.FUNCTION_NAME.equalsIgnoreCase(functionName))
            return new LengthConstraint(columnName);
        throw new InvalidConstraintDefinitionException("Unrecognized constraint function: " + functionName);
    }

    private FunctionColumnConstraint(ConstraintFunction function, ColumnIdentifier columnName, Operator relationType, String term)
    {
        this.function = function;
        this.columnName = columnName;
        this.relationType = relationType;
        this.term = term;
    }

    @Override
    public void appendCqlTo(CqlBuilder builder)
    {
        builder.append(toString());
    }

    @Override
    public IVersionedSerializer<ColumnConstraint> serializer()
    {
        return serializer;
    }

    @Override
    public void evaluate(Class<? extends AbstractType> valueType, Object columnValue)
    {
        function.evaluate(valueType, relationType, term, columnValue);
    }

    @Override
    public void validate(ColumnMetadata columnMetadata)
    {
        validateArgs(columnMetadata);
        function.validate(columnMetadata);
    }

    void validateArgs(ColumnMetadata columnMetadata)
    {
        if (!columnMetadata.name.equals(columnName))
            throw new InvalidConstraintDefinitionException("Function parameter should be the column name");
    }

    @Override
    public String toString()
    {
        return function.getName() + "(" + columnName + ") " + relationType + " " + term;
    }

    public static class Serializer implements IVersionedSerializer<ColumnConstraint>
    {
        @Override
        public void serialize(ColumnConstraint columnConstraint, DataOutputPlus out, int version) throws IOException
        {
            FunctionColumnConstraint condition = (FunctionColumnConstraint) columnConstraint;
            out.writeUTF(condition.function.getName());
            out.writeUTF(condition.columnName.toCQLString());
            out.writeUTF(condition.relationType.toString());
            out.writeUTF(condition.term);
        }

        @Override
        public ColumnConstraint deserialize(DataInputPlus in, int version) throws IOException
        {
            String functionName = in.readUTF();
            ConstraintFunction function;
            String columnNameString = in.readUTF();
            ColumnIdentifier columnName = new ColumnIdentifier(columnNameString, true);
            try
            {
                function = createConstraintFunction(functionName, columnName);
            }
            catch (Exception e)
            {
                throw new IOException(e);
            }
            String relationTypeString = in.readUTF();
            Operator relationType = Operator.valueOf(relationTypeString);
            final String term = in.readUTF();
            return new FunctionColumnConstraint(function, columnName, relationType, term);
        }

        @Override
        public long serializedSize(ColumnConstraint columnConstraint, int version)
        {
            FunctionColumnConstraint condition = (FunctionColumnConstraint) columnConstraint;

            return TypeSizes.sizeof(condition.function.getClass().getName())
                   + TypeSizes.sizeof(condition.columnName.toCQLString())
                   + TypeSizes.sizeof(condition.term)
                   + TypeSizes.sizeof(condition.relationType.toString());
        }
    }
}
