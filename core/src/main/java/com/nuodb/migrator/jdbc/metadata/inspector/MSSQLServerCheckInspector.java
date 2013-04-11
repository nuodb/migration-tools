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
import static com.nuodb.migrator.jdbc.metadata.inspector.InspectionResultsUtils.addTable;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;

/**
 * @author Sergey Bushik
 */
public class MSSQLServerCheckInspector extends TableInspectorBase<Table, TableInspectionScope> {

    public static final String QUERY =
            "SELECT CTU.TABLE_CATALOG, CTU.TABLE_SCHEMA, CTU.TABLE_NAME, CC.CHECK_CLAUSE, CTU.CONSTRAINT_NAME\n" +
            "FROM INFORMATION_SCHEMA.CONSTRAINT_TABLE_USAGE AS CTU\n" +
            "INNER JOIN INFORMATION_SCHEMA.CHECK_CONSTRAINTS AS CC ON CTU.TABLE_CATALOG=CC.CONSTRAINT_CATALOG\n" +
            "AND CTU.TABLE_SCHEMA=CC.CONSTRAINT_SCHEMA\n" +
            "AND CTU.CONSTRAINT_NAME=CC.CONSTRAINT_NAME\n" +
            "WHERE CTU.TABLE_CATALOG=? AND CTU.TABLE_SCHEMA=? AND CTU.TABLE_NAME=?";

    public MSSQLServerCheckInspector() {
        super(CHECK, TableInspectionScope.class);
    }

    @Override
    protected Collection<? extends TableInspectionScope> createInspectionScopes(Collection<? extends Table> tables) {
        return createTableInspectionScopes(tables);
    }

    @Override
    protected void inspectScopes(final InspectionContext inspectionContext,
                                 final Collection<? extends TableInspectionScope> inspectionScopes) throws SQLException {
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
                    public void execute(PreparedStatement statement) throws SQLException {
                        for (TableInspectionScope inspectionScope : inspectionScopes) {
                            statement.setString(1, inspectionScope.getCatalog());
                            statement.setString(2, inspectionScope.getSchema());
                            statement.setString(3, inspectionScope.getTable());
                            ResultSet checks = statement.executeQuery();
                            try {
                                inspect(inspectionContext, checks);
                            } finally {
                                close(checks);
                            }
                        }
                    }
                }
        );
    }

    protected void inspect(InspectionContext inspectionContext, ResultSet checks) throws SQLException {
        InspectionResults inspectionResults = inspectionContext.getInspectionResults();
        while (checks.next()) {
            Table table = addTable(inspectionResults,
                    checks.getString("TABLE_CATALOG"), checks.getString("TABLE_SCHEMA"), checks.getString("TABLE_NAME"));
            Check check = new Check(checks.getString("CONSTRAINT_NAME"));
            check.setClause(checks.getString("CHECK_CLAUSE"));
            table.addCheck(check);
            inspectionResults.addObject(check);
        }
    }

    @Override
    protected boolean supports(TableInspectionScope inspectionScope) {
        return inspectionScope.getCatalog() != null && inspectionScope.getSchema() != null && inspectionScope.getTable() != null;
    }
}