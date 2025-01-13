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

package org.apache.cassandra.contraints;

import org.junit.Test;

import org.apache.cassandra.cql3.constraints.ColumnConstraintScalar;
import org.apache.cassandra.cql3.constraints.ColumnConstraints;
import org.apache.cassandra.cql3.constraints.FunctionColumnConstraint;

import static org.junit.Assert.assertEquals;

public class ColumnConsraintsTest
{
    private static final Object[][] EXPECTED_VALUES =
    {
    { "FUNCTION", FunctionColumnConstraint.serializer },
    { "SCALAR", ColumnConstraintScalar.serializer }
    };

    @Test
    public void testEnumCodesAndNames()
    {
        ColumnConstraints.ConstraintsSerializers[] values = ColumnConstraints.ConstraintsSerializers.values();

        for (int i = 0; i < values.length; i++)
        {
            assertEquals("Column Constraint Serializer mismatch in the enum " +
                         values[i], EXPECTED_VALUES[i][0], values[i].name());
            assertEquals("Column Constraint Serializer mismatch in the enum for value " +
                         values[i], EXPECTED_VALUES[i][1], ColumnConstraints.ConstraintsSerializers.getSerializer(i));
        }

        assertEquals("Column Constraint Serializer enum constants has changed. Update the test.",
                     EXPECTED_VALUES.length, values.length);
    }
}
