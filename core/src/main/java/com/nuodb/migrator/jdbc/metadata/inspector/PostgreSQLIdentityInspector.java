/**
 * Copyright (c) 2012, NuoDB, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of NuoDB, Inc. nor the names of its contributors may
 *       be used to endorse or promote products derived from this software
 *       without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL NUODB, INC. BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.nuodb.migrator.jdbc.metadata.inspector;

import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Sequence;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.SelectQuery;
import com.nuodb.migrator.jdbc.query.StatementCallback;
import com.nuodb.migrator.jdbc.query.StatementFactory;
import com.nuodb.migrator.jdbc.query.StatementTemplate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.nuodb.migrator.jdbc.metadata.MetaDataType.IDENTITY;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.TABLE;
import static com.nuodb.migrator.jdbc.metadata.inspector.PostgreSQLColumn.initColumn;

/**
 * @author Sergey Bushik
 */
public class PostgreSQLIdentityInspector extends InspectorBase<Table, TableInspectionScope> {

    public PostgreSQLIdentityInspector() {
        super(IDENTITY, TABLE, TableInspectionScope.class);
    }

    @Override
    public void inspectObjects(final InspectionContext inspectionContext,
                               final Collection<? extends Table> tables) throws SQLException {
        Dialect dialect = inspectionContext.getDialect();
        for (Table table : tables) {
            for (final Column column : table.getColumns()) {
                initColumn(inspectionContext, column);
                Sequence sequence = column.getSequence();
                if (sequence == null) {
                    continue;
                }
                final SelectQuery selectQuery = new SelectQuery();
                selectQuery.column("*");
                String schemaName = dialect.getIdentifier(table.getSchema().getName(), null);
                String sequenceName = dialect.getIdentifier(sequence.getName(), null);
                selectQuery.from(schemaName + "." + sequenceName);

                StatementTemplate template = new StatementTemplate(inspectionContext.getConnection());
                template.execute(
                        new StatementFactory<PreparedStatement>() {
                            @Override
                            public PreparedStatement create(Connection connection) throws SQLException {
                                return connection.prepareStatement(selectQuery.toString());
                            }
                        },
                        new StatementCallback<PreparedStatement>() {
                            @Override
                            public void process(PreparedStatement statement) throws SQLException {
                                inspect(inspectionContext, column, statement.executeQuery());
                            }
                        }
                );
            }
        }
    }

    protected void inspect(InspectionContext inspectionContext, Column column,
                           ResultSet identities) throws SQLException {
        if (identities.next()) {
            Sequence sequence = new Sequence();
            sequence.setName(identities.getString("SEQUENCE_NAME"));
            sequence.setLastValue(identities.getLong("LAST_VALUE"));
            sequence.setStartWith(identities.getLong("START_VALUE"));
            sequence.setMinValue(identities.getLong("MIN_VALUE"));
            sequence.setMaxValue(identities.getLong("MAX_VALUE"));
            sequence.setIncrementBy(identities.getLong("INCREMENT_BY"));
            sequence.setCache(identities.getInt("CACHE_VALUE"));
            sequence.setCycle("T".equalsIgnoreCase(identities.getString("IS_CYCLED")));
            column.setSequence(sequence);
            inspectionContext.getInspectionResults().addObject(sequence);
        }
    }


    @Override
    public void inspectScope(InspectionContext inspectionContext,
                             TableInspectionScope inspectionScope) throws SQLException {
        throw new InspectorException("Not implemented yet");
    }

    @Override
    public boolean supports(InspectionContext inspectionContext, InspectionScope inspectionScope) {
        return false;
    }
}
