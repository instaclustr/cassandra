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


import org.apache.cassandra.io.IVersionedAsymmetricSerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// group of constraints for the column
public class ColumnConstraints implements ColumnConstraint
{

    public static Serializer serializer = new Serializer();

    private final List<ColumnConstraint> constraints;

    public ColumnConstraints(List<ColumnConstraint> constraints) {
        this.constraints = constraints;
    }

    @Override
    public IVersionedSerializer<ColumnConstraint> getSerializer() {
        return null;
    }

    @Override
    public void appendCqlTo(CqlBuilder builder) {
        for (ColumnConstraint constraint : constraints)
        {
            constraint.appendCqlTo(builder);
        }
    }

    @Override
    public void evaluate(Object columnValue) throws ConstraintViolationException {

        for (ColumnConstraint constraint : constraints)
        {
            constraint.evaluate(columnValue);
        }
    }

    public List<ColumnConstraint> getConstraints()
    {
        return constraints;
    }

    public boolean isEmpty()
    {
        return constraints.isEmpty();
    }


    @Override
    public void validate(ColumnMetadata columnMetadata, TableMetadata tableMetadata) throws ConstraintInvalidException {
        for (ColumnConstraint constraint : constraints)
        {
            constraint.validate(columnMetadata, tableMetadata);
        }
    }

    public static class Noop extends ColumnConstraints
    {
        public static Noop INSTANCE = new Noop();

        public Noop() {
            super(new ArrayList<>());
        }
    }

    public final static class Raw
    {
        private final List<ColumnConstraint> constraints;

        public Raw(List<ColumnConstraint> constraints)
        {
            this.constraints = constraints;
        }

        public Raw()
        {
            this.constraints = new ArrayList<>();
        }

        public void addConstraint(ColumnConstraint constraint)
        {
            this.constraints.add(constraint);
        }

        public ColumnConstraints prepare()
        {
            return new ColumnConstraints(constraints);
        }
    }

    public static class Serializer implements IVersionedSerializer<ColumnConstraint>
    {
        @Override
        public void serialize(ColumnConstraint columnConstraint, DataOutputPlus out, int version) throws IOException
        {
            ColumnConstraints constraints = (ColumnConstraints) columnConstraint;
            out.writeInt(constraints.getConstraints().size());

            for (ColumnConstraint constraint : constraints.getConstraints()) {
                out.writeUTF(constraint.getClass().toString());
                constraint.getSerializer().serialize(constraint, out, version);
            }
        }

        @Override
        public ColumnConstraints deserialize(DataInputPlus in, int version) throws IOException
        {
            List<ColumnConstraint> columnConstraints = new ArrayList<>();
            int numberOfConstraints = in.readInt();
            for (int i = 0; i < numberOfConstraints; i++)
            {
                String columnConstraintClassName = in.readUTF();
                ColumnConstraint constraint = ConstraintSerializerFactory.getCqlConditionSerializer(columnConstraintClassName)
                        .deserialize(in, version);
                columnConstraints.add(constraint);
            }
            return new ColumnConstraints(columnConstraints);
        }

        @Override
        public long serializedSize(ColumnConstraint columnConstraint, int version)
        {
            ColumnConstraints constraints = (ColumnConstraints) columnConstraint;
            long constraintsSize = 0;
            for (ColumnConstraint constraint : constraints.getConstraints())
                constraintsSize += constraint.getSerializer().serializedSize(constraint, version);
            return constraintsSize;
        }
    }

    public static class ConstraintSerializerFactory
    {
        public static IVersionedAsymmetricSerializer<ColumnConstraint, ColumnConstraint> getCqlConditionSerializer(String columnConstraintClassName)
        {
            if (columnConstraintClassName.equals(FunctionColumnConstraint.class.getName()))
                return FunctionColumnConstraint.serializer;
            else if (columnConstraintClassName.equals(ColumnConstraintScalar.class.getName()))
                return ColumnConstraintScalar.serializer;
            throw new IllegalArgumentException(String.format("Condition %s needs to have an implemented serializer", columnConstraintClassName));
        }
    }
}
