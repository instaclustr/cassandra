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

import org.apache.cassandra.cql3.terms.Constants;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;


public class FunctionColumnConstraint implements ColumnConstraint
{
    public final ConstraintFunctionExpression function;
    public final Operator relationType;
    public final String term;

    public static Serializer serializer = new Serializer();

    public final static class Raw
    {
        public final ConstraintFunctionExpression function;
        public final Operator relationType;
        public final String term;

        public Raw(ColumnIdentifier functionName, ColumnIdentifier columnName, Operator relationType, String term)
        {
            this.relationType = relationType;
            this.term = term;
            if (LengthConstraint.FUNCTION_NAME.equals(functionName.toCQLString().toUpperCase()))
                this.function = new ConstraintFunctionExpression(new LengthConstraint(), columnName);
            else
                throw new ConstraintInvalidException("Invalid constraint function");
        }

        public FunctionColumnConstraint prepare()
        {
            return new FunctionColumnConstraint(function, relationType, term);
        }
    }

    @Override
    public void appendCqlTo(CqlBuilder builder) {
        builder.append(toString());
    }

    public FunctionColumnConstraint(ConstraintFunctionExpression function, Operator relationType, String term)
    {
        this.function = function;
        this.relationType = relationType;
        this.term = term;
    }

    @Override
    public IVersionedSerializer<ColumnConstraint> getSerializer()
    {
        return serializer;
    }

    @Override
    public void evaluate(Object columnValue)
    {
        if (function != null)
            function.evaluate(relationType, term, columnValue);
    }

    @Override
    public void validate(ColumnMetadata columnMetadata, TableMetadata tableMetadata)
    {
        if (columnMetadata != null)
            validateArgs(columnMetadata);
        if (function != null)
            function.validateConstraint(relationType, term, tableMetadata);
    }

    void validateArgs(ColumnMetadata columnMetadata)
    {
        if (function == null)
            throw new ConstraintInvalidException("Function parameter should be the column name");

        if (!columnMetadata.name.equals(function.columnName))
            throw new ConstraintInvalidException("Function parameter should be the column name");
    }

    @Override
    public String toString()
    {
        return String.format("%s %s %s", function, relationType, term);
    }

    public static class Serializer implements IVersionedSerializer<ColumnConstraint>
    {
        @Override
        public void serialize(ColumnConstraint columnConstraint, DataOutputPlus out, int version) throws IOException
        {
            FunctionColumnConstraint condition = (FunctionColumnConstraint) columnConstraint;
            ConstraintFunctionExpression.serializer.serialize(condition.function, out, version);
            out.writeUTF(condition.relationType.toString());
            out.writeUTF(condition.term);
        }

        @Override
        public ColumnConstraint deserialize(DataInputPlus in, int version) throws IOException
        {
            ConstraintFunctionExpression constraintFunctionExpression = ConstraintFunctionExpression.serializer.deserialize(in, version);
            Operator relationType = Operator.valueOf(in.readUTF());
            final String term = in.readUTF();
            return new FunctionColumnConstraint(constraintFunctionExpression, relationType, term);
        }

        @Override
        public long serializedSize(ColumnConstraint columnConstraint, int version)
        {
            FunctionColumnConstraint condition = (FunctionColumnConstraint) columnConstraint;
            return TypeSizes.sizeof(condition.term)
                   + TypeSizes.sizeof(condition.relationType.toString())
                   + ConstraintFunctionExpression.serializer.serializedSize(condition.function, version);
        }
    }
}
