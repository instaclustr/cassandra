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
import java.util.List;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;

import static java.lang.String.format;

// TODO DO NOT COMMIT
public class Enumeration extends UnaryConstraintFunction
{
    private static final List<AbstractType<?>> SUPPORTED_TYPES = List.of(UTF8Type.instance, AsciiType.instance);

    public static final String FUNCTION_NAME = "ENUM";

    public Enumeration(List<String> args)
    {
        super(FUNCTION_NAME, args);
    }

    @Override
    public void internalEvaluate(AbstractType<?> valueType, Operator relationType, String term, ByteBuffer columnValue)
    {
        if (!args.contains(valueType.getString(columnValue)))
        {
            throw new ConstraintViolationException(format("Value for column '%s' violated %s constraint as its value is not one of %s.",
                                                          columnName.toCQLString(),
                                                          name,
                                                          args));
        }
    }

    @Override
    public List<AbstractType<?>> getSupportedTypes()
    {
        return SUPPORTED_TYPES;
    }

    @Override
    public boolean isParameterless()
    {
        return false;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof Enumeration))
            return false;

        Enumeration other = (Enumeration) o;

        return columnName.equals(other.columnName) && name.equals(other.name);
    }
}
