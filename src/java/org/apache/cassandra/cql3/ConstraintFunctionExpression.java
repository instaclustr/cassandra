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
import java.util.Map;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedAsymmetricSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;

public class ConstraintFunctionExpression
{
    public final ConstraintFunction constraintFunction;
    public final ColumnIdentifier columnName;

    public static final Serializer serializer = new Serializer();

    public static final Map<String, ConstraintFunction> supportedConstraintFunctions = Map.of(
        LengthConstraint.class.getName(), new LengthConstraint()
    );

    public ConstraintFunctionExpression(ConstraintFunction constraintFunction, ColumnIdentifier columnName)
    {
        this.constraintFunction = constraintFunction;
        this.columnName = columnName;
    }


    public void evaluate(Operator relationType, String term, Object columnValues)
    {
        constraintFunction.evaluate(columnName, relationType, term, columnValues);
    }

    public void validateConstraint(Operator relationType, String term, TableMetadata tableMetadata)
    {
        constraintFunction.validate(columnName, relationType, term, tableMetadata);
    }

    public String toCqlString()
    {
        return toString();
    }

    @Override
    public String toString()
    {
        return String.format("%s(%s)", constraintFunction.getName(), columnName);
    }

    public final static class Serializer implements IVersionedAsymmetricSerializer<ConstraintFunctionExpression, ConstraintFunctionExpression>
    {
        @Override
        public void serialize(ConstraintFunctionExpression constraintFunctionExpression, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(constraintFunctionExpression.constraintFunction.getClass().getName());
            out.writeUTF(constraintFunctionExpression.columnName.toCQLString());
        }

        @Override
        public ConstraintFunctionExpression deserialize(DataInputPlus in, int version) throws IOException
        {
            String executorClass = in.readUTF();
            ConstraintFunction executor;
            try
            {
                executor = supportedConstraintFunctions.get(executorClass);
            }
            catch (Exception e)
            {
                throw new IOException(e);
            }
            ColumnIdentifier columnName = new ColumnIdentifier(in.readUTF(), true);
            return new ConstraintFunctionExpression(executor, columnName);
        }

        @Override
        public long serializedSize(ConstraintFunctionExpression constraintFunctionExpression, int version)
        {
            return TypeSizes.sizeof(constraintFunctionExpression.constraintFunction.getClass().getName())
                    + TypeSizes.sizeof(constraintFunctionExpression.columnName.toCQLString());
        }
    }
}
