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

import org.apache.cassandra.cql3.constraints.InvalidConstraintDefinitionException;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class NotNullConstraintTest extends CqlConstraintValidationTester
{
    @Test
    public void testInvalidSpecificationOfNotNullConstraintOnPrimaryKeys() throws Throwable
    {
        assertThatThrownBy(() -> createTable("CREATE TABLE %s (pk int CHECK NOT_NULL(pk) PRIMARY KEY)"))
        .isInstanceOf(InvalidRequestException.class)
        .hasRootCauseInstanceOf(InvalidConstraintDefinitionException.class)
        .hasRootCauseMessage("NOT_NULL constraint can not be specified on a partition key column 'pk'");

        assertThatThrownBy(() -> createTable("CREATE TABLE %s (pk int, cl int CHECK NOT_NULL(cl), PRIMARY KEY (pk, cl))"))
        .isInstanceOf(InvalidRequestException.class)
        .hasRootCauseInstanceOf(InvalidConstraintDefinitionException.class)
        .hasRootCauseMessage("NOT_NULL constraint can not be specified on a clustering key column 'cl'");
    }
}
