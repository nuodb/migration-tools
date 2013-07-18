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

import com.nuodb.migrator.jdbc.metadata.Check;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.StatementCallback;
import com.nuodb.migrator.jdbc.query.StatementFactory;
import com.nuodb.migrator.jdbc.query.StatementTemplate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.nuodb.migrator.jdbc.JdbcUtils.close;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.CHECK;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;

/**
 * @author Sergey Bushik
 */
public class PostgreSQLCheckInspector extends InspectorBase<Table, TableInspectionScope> {

    private static final String QUERY =
            "SELECT *\n" +
            "FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS CCU\n" +
            "INNER JOIN INFORMATION_SCHEMA.CHECK_CONSTRAINTS AS CC ON\n" +
            "CCU.TABLE_CATALOG = CC.CONSTRAINT_CATALOG\n" +
            "AND CCU.TABLE_SCHEMA = CC.CONSTRAINT_SCHEMA\n" +
            "AND CCU.CONSTRAINT_NAME = CC.CONSTRAINT_NAME\n" +
            "WHERE CCU.TABLE_SCHEMA = ? AND CCU.TABLE_NAME = ?";

    public PostgreSQLCheckInspector() {
        super(CHECK, TableInspectionScope.class);
    }

    @Override
    public void inspectObjects(final InspectionContext inspectionContext,
                               final Collection<? extends Table> tables) throws SQLException {
        StatementTemplate template = new StatementTemplate(inspectionContext.getConnection());
        template.execute(
                new StatementFactory<PreparedStatement>() {
                    @Override
                    public PreparedStatement create(Connection connection) throws SQLException {
                        return connection.prepareStatement(QUERY, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
                    }
                },
                new StatementCallback<PreparedStatement>() {
                    @Override
                    public void process(PreparedStatement statement) throws SQLException {
                        for (Table table : tables) {
                            statement.setString(1, table.getSchema().getName());
                            statement.setString(2, table.getName());
                            ResultSet checkConstraints = statement.executeQuery();
                            try {
                                inspect(inspectionContext, table, checkConstraints);
                            } finally {
                                close(checkConstraints);
                            }
                        }
                    }
                }
        );
    }

    protected void inspect(InspectionContext inspectionContext, Table table, ResultSet checks) throws SQLException {
        while (checks.next()) {
            Check check = new Check(checks.getString("CONSTRAINT_NAME"));
            check.setText(checks.getString("CHECK_CLAUSE"));
            table.addCheck(check);
            inspectionContext.getInspectionResults().addObject(check);
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
