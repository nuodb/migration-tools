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
import com.nuodb.migrator.jdbc.query.StatementCallback;
import com.nuodb.migrator.jdbc.query.StatementFactory;
import com.nuodb.migrator.jdbc.query.StatementTemplate;
import com.nuodb.migrator.jdbc.type.JdbcTypeDesc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.nuodb.migrator.jdbc.JdbcUtils.close;
import static com.nuodb.migrator.jdbc.metadata.DefaultValue.valueOf;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.COLUMN;
import static com.nuodb.migrator.jdbc.metadata.inspector.InspectionResultsUtils.addTable;
import static com.nuodb.migrator.jdbc.metadata.inspector.NuoDBInspectorUtils.validateInspectionScope;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;

/**
 * @author Sergey Bushik
 */
public class NuoDBColumnInspector extends TableInspectorBase<Table, TableInspectionScope> {

    public static final String QUERY =
            "SELECT * FROM SYSTEM.FIELDS AS F INNER JOIN SYSTEM.DATATYPES AS D ON F.DATATYPE = D.ID\n" +
            "WHERE F.SCHEMA=? AND F.TABLENAME=? ORDER BY F.FIELDPOSITION ASC";

    public NuoDBColumnInspector() {
        super(COLUMN, TableInspectionScope.class);
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
        final StatementTemplate template = new StatementTemplate(inspectionContext.getConnection());
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
                        for (TableInspectionScope inspectionScope : inspectionScopes) {
                            statement.setString(1, inspectionScope.getSchema());
                            statement.setString(2, inspectionScope.getTable());
                            ResultSet columns = statement.executeQuery();
                            try {
                                inspect(inspectionContext, columns);
                            } finally {
                                close(columns);
                            }
                        }
                    }
                }
        );
    }

    private void inspect(InspectionContext inspectionContext, ResultSet columns) throws SQLException {
        InspectionResults inspectionResults = inspectionContext.getInspectionResults();
        Dialect dialect = inspectionContext.getDialect();
        while (columns.next()) {
            Table table = addTable(inspectionResults, null, columns.getString("SCHEMA"), columns.getString("TABLENAME"));

            Column column = table.addColumn(columns.getString("FIELD"));
            JdbcTypeDesc typeDescAlias = dialect.getJdbcTypeAlias(
                    columns.getInt("JDBCTYPE"), columns.getString("NAME"));
            column.setTypeCode(typeDescAlias.getTypeCode());
            column.setTypeName(typeDescAlias.getTypeName());

            int columnSize = columns.getInt("LENGTH");
            column.setSize(columnSize);
            column.setPrecision(columnSize);
            column.setDefaultValue(valueOf(columns.getString("DEFAULTVALUE")));
            column.setScale(columns.getInt("SCALE"));
            column.setComment(columns.getString("REMARKS"));
            column.setPosition(columns.getInt("FIELDPOSITION"));
            column.setNullable(columns.getInt("FLAGS") == 0);

            String sequence = columns.getString("GENERATOR_SEQUENCE");
            if (sequence != null) {
                column.setSequence(new Sequence(sequence));
            }
            column.setAutoIncrement(sequence != null);

            inspectionResults.addObject(column);
        }
    }

    @Override
    protected boolean supports(TableInspectionScope inspectionScope) {
        return inspectionScope.getSchema() != null && inspectionScope.getTable() != null;
    }
}
