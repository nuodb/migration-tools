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
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.StatementCallback;
import com.nuodb.migrator.jdbc.query.StatementFactory;
import com.nuodb.migrator.jdbc.query.StatementTemplate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.regex.Pattern;

import static com.nuodb.migrator.jdbc.JdbcUtils.close;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.CHECK;
import static com.nuodb.migrator.jdbc.metadata.inspector.InspectionResultsUtils.addTable;
import static com.nuodb.migrator.jdbc.metadata.inspector.NuoDBInspectorUtils.validateInspectionScope;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.util.regex.Pattern.compile;
import static java.util.regex.Pattern.quote;

/**
 * @author Sergey Bushik
 */
public class NuoDBCheckInspector extends TableInspectorBase<Table, TableInspectionScope> {

    public static final String QUERY =
            "SELECT T.SCHEMA, T.TABLENAME, T.CONSTRAINTNAME, T.CONSTRAINTTEXT FROM SYSTEM.TABLECONSTRAINTS AS T " +
            "INNER JOIN SYSTEM.FIELDS AS F ON T.SCHEMA = F.SCHEMA AND T.TABLENAME = F.TABLENAME " +
            "WHERE T.SCHEMA=? AND T.TABLENAME=?";

    public NuoDBCheckInspector() {
        super(CHECK, TableInspectionScope.class);
    }

    @Override
    public void inspectScope(InspectionContext inspectionContext,
                             TableInspectionScope inspectionScope) throws SQLException {
        validateInspectionScope(inspectionScope);
        super.inspectScope(inspectionContext, inspectionScope);
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
                            statement.setString(1, inspectionScope.getSchema());
                            statement.setString(2, inspectionScope.getTable());
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

    private void inspect(InspectionContext inspectionContext, ResultSet checks) throws SQLException {
        InspectionResults inspectionResults = inspectionContext.getInspectionResults();
        while (checks.next()) {
            Table table = addTable(inspectionResults, null, checks.getString("SCHEMA"), checks.getString("TABLENAME"));
            Pattern pattern = compile(quote(table.getName() + "$constraint") + "\\d+");
            String constraint = checks.getString("CONSTRAINTNAME");
            Check check = new Check(constraint);
            check.setTable(table);
            check.setText(checks.getString("CONSTRAINTTEXT"));
            if (pattern.matcher(constraint).matches()) {
                table.addCheck(check);
            } else {
                Column column = table.addColumn(constraint);
                column.addCheck(check);
            }
            inspectionResults.addObject(check);
        }
    }

    @Override
    protected boolean supports(TableInspectionScope inspectionScope) {
        return inspectionScope.getSchema() != null && inspectionScope.getTable() != null;
    }
}
