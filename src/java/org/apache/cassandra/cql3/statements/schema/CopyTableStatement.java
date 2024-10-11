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

package org.apache.cassandra.cql3.statements.schema;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.schema.Triggers;
import org.apache.cassandra.schema.UserFunctions;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.Event.SchemaChange;

/**
 * {@code CREATE TABLE [IF NOT EXISTS] <newtable> LIKE <oldtable> WITH <property> = <value>}
 */
public final class CopyTableStatement extends AlterSchemaStatement
{
    private final String sourceKeyspace;
    private final String sourceTableName;
    private final String targetKeyspace;
    private final String targetTableName;
    private final boolean ifNotExists;
    private final TableAttributes attrs;

    public CopyTableStatement(String sourceKeyspace,
                              String targetKeyspace,
                              String sourceTableName,
                              String targetTableName,
                              boolean ifNotExists,
                              TableAttributes attrs)
    {
        super(targetKeyspace);
        this.sourceKeyspace = sourceKeyspace;
        this.targetKeyspace = targetKeyspace;
        this.sourceTableName = sourceTableName;
        this.targetTableName = targetTableName;
        this.ifNotExists = ifNotExists;
        this.attrs = attrs;
    }

    @Override
    SchemaChange schemaChangeEvent(Keyspaces.KeyspacesDiff diff)
    {
        return new SchemaChange(SchemaChange.Change.CREATED, SchemaChange.Target.TABLE, targetKeyspace, targetTableName);
    }

    @Override
    public void authorize(ClientState client)
    {
        client.ensureTablePermission(sourceKeyspace, sourceTableName, Permission.SELECT);
        client.ensureAllTablesPermission(targetKeyspace, Permission.CREATE);
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.CREATE_TABLE_LIKE, targetKeyspace, targetTableName);
    }

    @Override
    public Keyspaces apply(ClusterMetadata metadata)
    {
        Keyspaces schema = metadata.schema.getKeyspaces();
        KeyspaceMetadata sourceKeyspaceMeta = schema.getNullable(sourceKeyspace);
        TableMetadata sourceTableMeta = sourceKeyspaceMeta.getTableNullable(sourceTableName);

        if (null == sourceKeyspaceMeta)
            throw ire("Source Keyspace '%s' doesn't exist", sourceKeyspace);

        if (null == sourceTableMeta)
            throw ire("Souce Table '%s'.'%s' doesn't exist", sourceKeyspace, sourceTableName);

        if (sourceTableMeta.isIndex())
            throw ire("Cannot use CREATE TABLE LIKE on a index table '%s'.'%s'.", sourceKeyspace, sourceTableName);

        if (sourceTableMeta.isView())
            throw ire("Cannot use CREATE TABLE LIKE on a materialized view '%s'.'%s'.", sourceKeyspace, sourceTableName);

        KeyspaceMetadata targetKeyspaceMeta = schema.getNullable(targetKeyspace);
        if (null == targetKeyspaceMeta)
            throw ire("Target Keyspace '%s' doesn't exist", targetKeyspace);

        if (targetKeyspaceMeta.hasTable(targetTableName))
        {
            if(ifNotExists)
                return schema;

            throw new AlreadyExistsException(targetKeyspace, targetTableName);
        }
        // todo support udt for differenet ks latter
        if (!sourceKeyspace.equalsIgnoreCase(targetKeyspace) && !sourceKeyspaceMeta.types.isEmpty())
            throw ire("Cannot use CREATE TABLE LIKE across different keyspace when source table have UDTs.");

        String sourceCQLString = sourceTableMeta.toCqlString(false, false, true, false);
        // add all user functions to be able to give a good error message to the user if the alter references
        // a function from another keyspace
        UserFunctions.Builder ufBuilder = UserFunctions.builder().add();
        for (KeyspaceMetadata ksm : schema)
            ufBuilder.add(ksm.userFunctions);

        TableMetadata.Builder targetBuilder = CreateTableStatement.parse(sourceCQLString,
                                                                         targetKeyspace,
                                                                         targetTableName,
                                                                         sourceKeyspaceMeta.types,
                                                                         ufBuilder.build())
                                                                  .indexes(Indexes.none())
                                                                  .triggers(Triggers.none());

        TableParams originalParams = targetBuilder.build().params;
        TableParams newTableParams = attrs.asAlteredTableParams(originalParams);
        TableMetadata table = targetBuilder.params(newTableParams)
                                           .id(TableId.get(metadata))
                                           .build();
        table.validate();

        if (targetKeyspaceMeta.replicationStrategy.hasTransientReplicas()
            && table.params.readRepair != ReadRepairStrategy.NONE)
        {
            throw ire("read_repair must be set to 'NONE' for transiently replicated keyspaces");
        }

        if (!table.params.compression.isEnabled())
            Guardrails.uncompressedTablesEnabled.ensureEnabled(state);

        return schema.withAddedOrUpdated(targetKeyspaceMeta.withSwapped(targetKeyspaceMeta.tables.with(table)));
    }

    public final static class Raw extends CQLStatement.Raw
    {
        private final QualifiedName oldName;
        private final QualifiedName newName;
        private final boolean ifNotExists;
        public final TableAttributes attrs = new TableAttributes();

        public Raw(QualifiedName newName, QualifiedName oldName, boolean ifNotExists)
        {
            this.newName = newName;
            this.oldName = oldName;
            this.ifNotExists = ifNotExists;
        }

        @Override
        public CQLStatement prepare(ClientState state)
        {
            String oldKeyspace = oldName.hasKeyspace() ? oldName.getKeyspace() : state.getKeyspace();
            String newKeyspace = newName.hasKeyspace() ? newName.getKeyspace() : state.getKeyspace();
            return new CopyTableStatement(oldKeyspace, newKeyspace, oldName.getName(), newName.getName(), ifNotExists, attrs);
        }
    }
}
