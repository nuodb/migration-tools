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
import com.nuodb.migrator.jdbc.metadata.AutoIncrement;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Sequence;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.StatementCallback;
import com.nuodb.migrator.jdbc.query.StatementFactory;
import com.nuodb.migrator.jdbc.query.StatementTemplate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.nuodb.migrator.jdbc.metadata.MetaDataType.AUTO_INCREMENT;
import static com.nuodb.migrator.jdbc.metadata.inspector.PostgreSQLColumn.adopt;
import static java.lang.String.format;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;

/**
 * @author Sergey Bushik
 */
public class PostgreSQLAutoIncrementInspector extends InspectorBase<Table, TableInspectionScope> {

    private static final String QUERY = "SELECT * FROM %s.%s";

    public PostgreSQLAutoIncrementInspector() {
        super(AUTO_INCREMENT, TableInspectionScope.class);
    }

    @Override
    public void inspectObjects(final InspectionContext inspectionContext,
                               final Collection<? extends Table> tables) throws SQLException {
        Dialect dialect = inspectionContext.getDialect();
        for (Table table : tables) {
            for (final Column column : table.getColumns()) {
                Sequence sequence = adopt(inspectionContext, column).getSequence();
                if (sequence != null) {
                    continue;
                }
                final String query = format(QUERY,
                        dialect.getIdentifier(table.getSchema().getName(), null),
                        dialect.getIdentifier(sequence.getName(), null));
                StatementTemplate template = new StatementTemplate(inspectionContext.getConnection());
                template.execute(
                        new StatementFactory<PreparedStatement>() {
                            @Override
                            public PreparedStatement create(Connection connection) throws SQLException {
                                return connection.prepareStatement(query, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
                            }
                        },
                        new StatementCallback<PreparedStatement>() {
                            @Override
                            public void execute(PreparedStatement statement) throws SQLException {
                                inspect(inspectionContext, column, statement.executeQuery());
                            }
                        }
                );
                column.setDefaultValue(null);
            }
        }
    }

    protected void inspect(InspectionContext inspectionContext, Column column,
                           ResultSet autoIncrements) throws SQLException {
        if (autoIncrements.next()) {
            Sequence sequence = new AutoIncrement();
            sequence.setName(autoIncrements.getString("SEQUENCE_NAME"));
            sequence.setLastValue(autoIncrements.getLong("LAST_VALUE"));
            sequence.setStartWith(autoIncrements.getLong("START_VALUE"));
            sequence.setMinValue(autoIncrements.getLong("MIN_VALUE"));
            sequence.setMaxValue(autoIncrements.getLong("MAX_VALUE"));
            sequence.setIncrementBy(autoIncrements.getLong("INCREMENT_BY"));
            sequence.setCache(autoIncrements.getInt("CACHE_VALUE"));
            sequence.setCycle("T".equalsIgnoreCase(autoIncrements.getString("IS_CYCLED")));
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
