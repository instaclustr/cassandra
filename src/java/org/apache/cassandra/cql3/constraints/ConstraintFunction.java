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


import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Interface to be implemented by functions that are executed as part of CQL constraints.
 */
public interface ConstraintFunction
{
    /**
     * This method returns the function name to be executed.
     *
     * @return
     */
    String getName();

    /**
     * Method that provides the execution of the condition. It can either succeed or throw a {@link ConstraintViolationException}.
     *
     * @param columnName
     * @param relationType
     * @param term
     * @param columnValue
     */
    void evaluate(ColumnIdentifier columnName, Operator relationType, String term, Object columnValue) throws ConstraintViolationException;

    /**
     * Method that validates that a condition is valid. This method is called when the CQL constraint is created to determine
     * if the CQL statement is valid or needs to be rejected as invalid throwing a {@link InvalidConstraintDefinitionException}
     * @param columnName
     * @param relationType
     * @param term
     * @param tableMetadata
     */
    void validate(ColumnIdentifier columnName, Operator relationType, String term, TableMetadata tableMetadata) throws InvalidConstraintDefinitionException;
}
