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
package com.nuodb.migration.jdbc.metadata.inspector;

import com.nuodb.migration.jdbc.metadata.Column;
import com.nuodb.migration.jdbc.metadata.DefaultValue;
import com.nuodb.migration.jdbc.metadata.MetaDataType;
import com.nuodb.migration.jdbc.metadata.Table;
import com.nuodb.migration.jdbc.query.StatementCallback;
import com.nuodb.migration.jdbc.query.StatementCreator;
import com.nuodb.migration.jdbc.query.StatementTemplate;
import com.nuodb.migration.jdbc.type.JdbcTypeDesc;
import com.nuodb.migration.jdbc.type.JdbcTypeRegistry;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.nuodb.migration.jdbc.JdbcUtils.close;
import static com.nuodb.migration.jdbc.metadata.inspector.NuoDBInspectorUtils.validateInspectionScope;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;

/**
 * @author Sergey Bushik
 */
public class NuoDBColumnInspector extends TableInspectorBase<Table, TableInspectionScope> {

    public static final String QUERY =
            "SELECT * FROM SYSTEM.FIELDS INNER JOIN SYSTEM.DATATYPES ON FIELDS.DATATYPE = DATATYPES.ID\n" +
            "WHERE SCHEMA=? AND TABLENAME=?";

    public NuoDBColumnInspector() {
        super(MetaDataType.COLUMN, TableInspectionScope.class);
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
                new StatementCreator<PreparedStatement>() {
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
        InspectionResults results = inspectionContext.getInspectionResults();
        JdbcTypeRegistry typeRegistry = inspectionContext.getDialect().getJdbcTypeRegistry();
        while (columns.next()) {
            Table table = InspectionResultsUtils.addTable(results, null, columns.getString("SCHEMA"),
                    columns.getString("TABLENAME"));

            Column column = table.createColumn(columns.getString("FIELD"));
            JdbcTypeDesc typeDescAlias = typeRegistry.getJdbcTypeDescAlias(
                    columns.getInt("JDBCTYPE"), columns.getString("NAME"));
            column.setTypeCode(typeDescAlias.getTypeCode());
            column.setTypeName(typeDescAlias.getTypeName());

            int columnSize = columns.getInt("LENGTH");
            column.setSize(columnSize);
            column.setPrecision(columnSize);
            String defaultValue = columns.getString("DEFAULTVALUE");
            column.setDefaultValue(defaultValue != null ? new DefaultValue(defaultValue) : null);
            column.setScale(columns.getInt("SCALE"));
            column.setComment(columns.getString("REMARKS"));
            column.setPosition(columns.getInt("FIELDPOSITION"));
            column.setNullable(columns.getInt("FLAGS") == 0);
            column.setAutoIncrement(columns.getString("GENERATOR_SEQUENCE") != null);

            results.addObject(column);
        }
    }

    @Override
    protected boolean supports(TableInspectionScope inspectionScope) {
        return inspectionScope.getSchema() != null && inspectionScope.getTable() != null;
    }
}
