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

import com.nuodb.migrator.jdbc.metadata.AutoIncrement;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Sequence;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.JdbcUtils.close;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.AUTO_INCREMENT;
import static com.nuodb.migrator.jdbc.metadata.inspector.InspectionResultsUtils.addTable;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * @author Sergey Bushik
 */
public class MSSQLServerAutoIncrementInspector extends TableInspectorBase<Table, TableInspectionScope> {

    public MSSQLServerAutoIncrementInspector() {
        super(AUTO_INCREMENT, TableInspectionScope.class);
    }

    @Override
    protected Collection<? extends TableInspectionScope> createInspectionScopes(Collection<? extends Table> tables) {
        return createTableInspectionScopes(tables);
    }

    @Override
    protected void inspectScopes(final InspectionContext inspectionContext,
                                 final Collection<? extends TableInspectionScope> inspectionScopes) throws SQLException {
        final StatementTemplate template = new StatementTemplate(inspectionContext.getConnection());
        for (TableInspectionScope inspectionScope : inspectionScopes) {
            final Collection<String> parameters = newArrayList();
            final SelectQuery selectQuery = createSelectQuery(inspectionScope, parameters);
            template.execute(
                    new StatementFactory<PreparedStatement>() {
                        @Override
                        public PreparedStatement create(Connection connection) throws SQLException {
                            return connection.prepareStatement(selectQuery.toString(),
                                    TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
                        }
                    },
                    new StatementCallback<PreparedStatement>() {
                        @Override
                        public void execute(PreparedStatement statement) throws SQLException {
                            int parameter = 1;
                            for (Iterator<String> iterator = parameters.iterator(); iterator.hasNext(); ) {
                                statement.setString(parameter++, iterator.next());
                            }
                            ResultSet autoIncrements = null;
                            try {
                                autoIncrements = statement.executeQuery();
                                inspect(inspectionContext, autoIncrements);
                            } finally {
                                close(autoIncrements);
                            }
                        }
                    }
            );
        }
    }

    protected SelectQuery createSelectQuery(TableInspectionScope inspectionScope, Collection<String> parameters) {
        SelectQuery selectColumn = new SelectQuery();
        if (isEmpty(inspectionScope.getCatalog())) {
            selectColumn.column("DB_NAME() AS TABLE_CATALOG");
        } else {
            selectColumn.column("? AS TABLE_CATALOG");
            parameters.add(inspectionScope.getCatalog());
        }
        selectColumn.column("SCHEMAS.NAME AS TABLE_SCHEMA");
        selectColumn.column("TABLES.NAME AS TABLE_NAME");
        selectColumn.column("COLUMNS.NAME AS COLUMN_NAME");

        String catalog = isEmpty(inspectionScope.getCatalog()) ? "" : (inspectionScope.getCatalog() + ".");
        selectColumn.from(catalog + "SYS.SCHEMAS");
        selectColumn.innerJoin(catalog + "SYS.TABLES", "SCHEMAS.SCHEMA_ID=TABLES.SCHEMA_ID");
        selectColumn.innerJoin(catalog + "SYS.COLUMNS", "COLUMNS.OBJECT_ID=TABLES.OBJECT_ID");

        if (!isEmpty(inspectionScope.getSchema())) {
            selectColumn.where("SCHEMAS.NAME=?");
            parameters.add(inspectionScope.getSchema());
        }
        if (!isEmpty(inspectionScope.getTable())) {
            selectColumn.where("TABLES.NAME=?");
            parameters.add(inspectionScope.getTable());
        }
        selectColumn.where("IS_IDENTITY=1");

        SelectQuery selectTable = new SelectQuery();
        selectTable.column("C.*");
        selectTable.column(
                "QUOTENAME(TABLE_CATALOG) + '.' + QUOTENAME(TABLE_SCHEMA) + '.' + QUOTENAME(TABLE_NAME) AS TABLE_QUALIFIED_NAME");
        selectTable.from("(" + selectColumn + ") C");

        SelectQuery selectIdentity = new SelectQuery();
        selectIdentity.column("C.*");
        selectIdentity.column("IDENT_SEED(TABLE_QUALIFIED_NAME) AS START_WITH");
        selectIdentity.column("IDENT_CURRENT(TABLE_QUALIFIED_NAME) AS LAST_VALUE");
        selectIdentity.column("IDENT_INCR(TABLE_QUALIFIED_NAME) AS INCREMENT_BY");
        selectIdentity.from("(" + selectTable + ") C");
        return selectIdentity;
    }

    private void inspect(InspectionContext context, ResultSet autoIncrements) throws SQLException {
        InspectionResults inspectionResults = context.getInspectionResults();
        if (autoIncrements.next()) {
            Table table = addTable(inspectionResults,
                    autoIncrements.getString("TABLE_CATALOG"),
                    autoIncrements.getString("TABLE_SCHEMA"),
                    autoIncrements.getString("TABLE_NAME"));
            Sequence sequence = new AutoIncrement();
            sequence.setStartWith(autoIncrements.getLong("START_WITH"));
            sequence.setLastValue(autoIncrements.getLong("LAST_VALUE"));
            sequence.setIncrementBy(autoIncrements.getLong("INCREMENT_BY"));
            Column column = table.addColumn(autoIncrements.getString("COLUMN_NAME"));
            column.setSequence(sequence);
            inspectionResults.addObject(sequence);
        }
    }
}
