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
import com.nuodb.migrator.jdbc.query.SelectQuery;
import com.nuodb.migrator.jdbc.query.StatementCallback;
import com.nuodb.migrator.jdbc.query.StatementFactory;
import com.nuodb.migrator.jdbc.query.StatementTemplate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.JdbcUtils.close;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.CHECK;
import static com.nuodb.migrator.jdbc.metadata.inspector.InspectionResultsUtils.addTable;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.trim;

/**
 * @author Sergey Bushik
 */
public class DB2CheckInspector extends TableInspectorBase<Table, TableInspectionScope> {

    public DB2CheckInspector() {
        super(CHECK, TableInspectionScope.class);
    }

    @Override
    protected Collection<? extends TableInspectionScope> createInspectionScopes(Collection<? extends Table> tables) {
        return createTableInspectionScopes(tables);
    }

    @Override
    protected void inspectScopes(final InspectionContext inspectionContext,
                                 Collection<? extends TableInspectionScope> inspectionScopes) throws SQLException {
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
                        public void process(PreparedStatement statement) throws SQLException {
                            int index = 1;
                            for (String parameter : parameters) {
                                statement.setString(index++, parameter);
                            }
                            ResultSet checks = null;
                            try {
                                checks = statement.executeQuery();
                                inspect(inspectionContext, checks);
                            } finally {
                                close(checks);
                            }
                        }
                    }
            );
        }

    }

    protected SelectQuery createSelectQuery(TableInspectionScope inspectionScope, Collection<String> parameters) {
        SelectQuery selectQuery = new SelectQuery();
        selectQuery.column("TABSCHEMA", "TABNAME", "CONSTNAME", "TEXT");
        selectQuery.from("SYSCAT.CHECKS");
        selectQuery.where("TYPE = 'C'");
        if (!isEmpty(inspectionScope.getSchema())) {
            selectQuery.where("TABSCHEMA=?");
            parameters.add(inspectionScope.getSchema());
        }
        if (!isEmpty(inspectionScope.getTable())) {
            selectQuery.where("TABNAME=?");
            parameters.add(inspectionScope.getTable());
        }
        return selectQuery;
    }

    protected void inspect(InspectionContext inspectionContext, ResultSet checks) throws SQLException {
        InspectionResults inspectionResults = inspectionContext.getInspectionResults();
        while (checks.next()) {
            Table table = addTable(inspectionResults, null, trim(checks.getString("TABSCHEMA")),
                    trim(checks.getString("TABNAME")));
            Check check = new Check(checks.getString("CONSTNAME"), checks.getString("TEXT"));
            table.addCheck(check);
            inspectionResults.addObject(check);
        }
    }
}
