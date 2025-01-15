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

import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.DynamicCompositeType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

// group of constraints for the column
public class ColumnConstraints implements ColumnConstraint<ColumnConstraints>
{
    public static final Serializer serializer = new Serializer();
    public static final Noop NO_OP = new Noop();

    private static final Set<Class<? extends AbstractType>> UNSUPPORTED_TYPES = ImmutableSet.of(MapType.class,
                                                                                                TupleType.class,
                                                                                                UserType.class,
                                                                                                CompositeType.class,
                                                                                                DynamicCompositeType.class);



    private final List<ColumnConstraint<?>> constraints;
    private final int constraintsSize;
    private final boolean isEmpty;

    public ColumnConstraints(List<ColumnConstraint<?>> constraints)
    {
        this.constraints = new ArrayList<>(constraints);
        // These are catched values to avoid making the calculations every time
        this.constraintsSize = constraints.size();
        this.isEmpty = constraints.isEmpty();
    }

    @Override
    public IVersionedSerializer<ColumnConstraints> serializer()
    {
        return serializer;
    }

    @Override
    public void appendCqlTo(CqlBuilder builder)
    {
        for (ColumnConstraint<?> constraint : constraints)
            constraint.appendCqlTo(builder);
    }

    @Override
    public void evaluate(Class<? extends AbstractType> valueType, Object columnValue) throws ConstraintViolationException
    {
        for (ColumnConstraint<?> constraint : constraints)
            constraint.evaluate(valueType, columnValue);
    }

    public List<ColumnConstraint<?>> getConstraints()
    {
        return constraints;
    }

    public boolean isEmpty()
    {
        return isEmpty;
    }

    public int getSize()
    {
        return constraintsSize;
    }

    @Override
    public void validate(ColumnMetadata columnMetadata) throws InvalidConstraintDefinitionException
    {
        if (UNSUPPORTED_TYPES.contains(columnMetadata.type.getClass()))
            throw new InvalidConstraintDefinitionException("Constraint cannot be defined on column '"
                                                           + columnMetadata.name + "' with type: "
                                                           + columnMetadata.type.asCQL3Type());

        for (ColumnConstraint<?> constraint : constraints)
            constraint.validate(columnMetadata);
    }

    @Override
    public ConstraintType getConstraintType()
    {
        return ConstraintType.COMPOSED;
    }

    public static class Noop extends ColumnConstraints
    {
        private Noop()
        {
            super(Collections.emptyList());
        }

        @Override
        public boolean isEmpty()
        {
            return true;
        }

        @Override
        public int getSize()
        {
            return 0;
        }
    }

    public final static class Raw
    {
        private final List<ColumnConstraint<?>> constraints;

        public Raw(List<ColumnConstraint<?>> constraints)
        {
            this.constraints = constraints;
        }

        public Raw()
        {
            this.constraints = Collections.emptyList();
        }

        public ColumnConstraints prepare()
        {
            if (constraints.isEmpty())
                return NO_OP;
            return new ColumnConstraints(constraints);
        }
    }

    public static class Serializer implements IVersionedSerializer<ColumnConstraints>
    {
        @Override
        public void serialize(ColumnConstraints columnConstraint, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(columnConstraint.getSize());
            for (ColumnConstraint constraint : columnConstraint.getConstraints())
            {
                // We serialize the serializer position in the enum to save space
                out.writeInt(constraint.getConstraintType().ordinal());
                constraint.serializer().serialize(constraint, out, version);
            }
        }

        @Override
        public ColumnConstraints deserialize(DataInputPlus in, int version) throws IOException
        {
            List<ColumnConstraint<?>> columnConstraints = new ArrayList<>();
            int numberOfConstraints = in.readInt();
            for (int i = 0; i < numberOfConstraints; i++)
            {
                int serializerPosition = in.readShort();
                ColumnConstraint<?> constraint = (ColumnConstraint<?>) ConstraintType
                                                                       .getSerializer(serializerPosition)
                                                                       .deserialize(in, version);
                columnConstraints.add(constraint);
            }
            return new ColumnConstraints(columnConstraints);
        }

        @Override
        public long serializedSize(ColumnConstraints columnConstraint, int version)
        {
            long constraintsSize = 0;
            for (ColumnConstraint constraint : columnConstraint.getConstraints())
                constraintsSize += constraint.serializer().serializedSize(constraint, version);
            return constraintsSize;
        }
    }
}
