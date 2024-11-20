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
import java.util.Set;

import com.google.common.base.Objects;

import org.apache.cassandra.cql3.terms.Term;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedAsymmetricSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

public class CqlConstraint
{
    public final ConstraintCondition constraintCondition;

    public static Serializer serializer = new Serializer();

    public final static class Raw
    {
        public final ConstraintCondition constraintCondition;

        public Raw(ConstraintCondition constraintCondition)
        {
            this.constraintCondition = constraintCondition;
        }

        public CqlConstraint prepare()
        {
            return new CqlConstraint(constraintCondition);
        }
    }

    public CqlConstraint(ConstraintCondition constraintCondition)
    {
        this.constraintCondition = constraintCondition;
    }

    public void appendCqlTo(CqlBuilder builder)
    {
        builder.append(toString());
    }

    public void checkConstraint(Map<String, Term.Raw> columnValues, ColumnMetadata columnMetadata, TableMetadata tableMetadata)
    {
        constraintCondition.evaluate(columnValues, columnMetadata, tableMetadata);
    }

    public void validateConstraint(Map<String, ColumnMetadata> columnMetadata, TableMetadata tableMetadata)
    {
        constraintCondition.validate(columnMetadata, tableMetadata);
    }

    @Override
    public String toString()
    {
        return constraintCondition.toString();
    }

    public String toCqlString()
    {
        return toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(constraintCondition);
    }

    @Override
    public boolean equals(final Object obj) {
        return obj instanceof CqlConstraint
               && Objects.equal(constraintCondition, ((CqlConstraint) obj).constraintCondition);
    }

    public static class Serializer implements IVersionedAsymmetricSerializer<CqlConstraint, CqlConstraint>
    {

        @Override
        public void serialize(CqlConstraint cqlConstraint, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(cqlConstraint.constraintCondition.getClass().toString());
            cqlConstraint.constraintCondition.getSerializer().serialize(cqlConstraint.constraintCondition, out, version);
        }

        @Override
        public CqlConstraint deserialize(DataInputPlus in, int version) throws IOException
        {
            String columnConstraintClassName = in.readUTF();
            ConstraintCondition condition = ConstraintSerializerFactory.getCqlConditionSerializer(columnConstraintClassName)
                                                                       .deserialize(in, version);
            return new CqlConstraint(condition);
        }

        @Override
        public long serializedSize(CqlConstraint cqlConstraint, int version)
        {
            return TypeSizes.sizeof(cqlConstraint.constraintCondition.getClass().toString())
                   + cqlConstraint.constraintCondition.getSerializer().serializedSize(cqlConstraint.constraintCondition, version);
        }

        public long serializedSetSize(Set<CqlConstraint> cqlConstraintSet, int version)
        {
            long size = 4; // IntSize needed as the set always serializes an int for the number of items in the set
            if (cqlConstraintSet != null)
                for (CqlConstraint constraint : cqlConstraintSet)
                    size += serializedSize(constraint, version);
            return size;
        }
    }

    public static class ConstraintSerializerFactory
    {
        public static IVersionedAsymmetricSerializer<ConstraintCondition, ConstraintCondition> getCqlConditionSerializer(String columnConstraintClassName)
        {
            if (columnConstraintClassName.equals(CqlConstraintFunctionCondition.class.getName()))
                return CqlConstraintFunctionCondition.serializer;
            else if (columnConstraintClassName.equals(ConstraintScalarCondition.class.getName()))
                return ConstraintScalarCondition.serializer;
            throw new IllegalArgumentException(String.format("Condition %s needs to have an implemented serializer", columnConstraintClassName));
        }
    }
}
