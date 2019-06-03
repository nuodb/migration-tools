/**
 * Copyright (c) 2015, NuoDB, Inc.
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
import com.nuodb.migrator.jdbc.query.ParameterizedQuery;
import com.nuodb.migrator.jdbc.query.Query;
import com.nuodb.migrator.jdbc.query.SelectQuery;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.CHECK;
import static com.nuodb.migrator.jdbc.metadata.inspector.InspectionResultsUtils.addTable;
import static com.nuodb.migrator.utils.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.containsAny;

/**
 * @author Sergey Bushik
 */
public class OracleCheckInspector extends TableInspectorBase<Table, TableInspectionScope> {

    public OracleCheckInspector() {
        super(CHECK, TableInspectionScope.class);
    }

    @Override
    protected Query createQuery(InspectionContext inspectionContext, TableInspectionScope tableInspectionScope) {
        Collection<Object> parameters = newArrayList();
        SelectQuery query = new SelectQuery();
        query.columns("C.CONSTRAINT_NAME", "CC.COLUMN_NAME", "C.SEARCH_CONDITION", "C.TABLE_NAME", "C.OWNER");
        query.from("SYS.ALL_CONS_COLUMNS CC");
        query.join("SYS.ALL_CONSTRAINTS C",
                "CC.OWNER=C.OWNER AND CC.TABLE_NAME=C.TABLE_NAME AND " + "CC.CONSTRAINT_NAME=C.CONSTRAINT_NAME");
        query.where("C.CONSTRAINT_TYPE='C'");
        query.where("C.STATUS='ENABLED'");
        String schema = tableInspectionScope.getSchema();
        if (!isEmpty(schema)) {
            query.where(containsAny(schema, "%") ? "C.OWNER LIKE ? ESCAPE '/'" : "C.OWNER=?");
            parameters.add(schema);
        }
        String table = tableInspectionScope.getTable();
        if (!isEmpty(table)) {
            query.where("C.TABLE_NAME=?");
            parameters.add(table);
        }
        return new ParameterizedQuery(query, parameters);
    }

    @Override
    protected void processResultSet(InspectionContext inspectionContext, ResultSet checks) throws SQLException {
        InspectionResults inspectionResults = inspectionContext.getInspectionResults();
        while (checks.next()) {
            String condition = checks.getString("SEARCH_CONDITION");
            if (!condition.endsWith("IS NOT NULL")) {
                Table table = addTable(inspectionResults, null, checks.getString("OWNER"),
                        checks.getString("TABLE_NAME"));
                Check check = new Check(checks.getString("CONSTRAINT_NAME"));
                check.setText(condition);
                table.addCheck(check);
                inspectionResults.addObject(check);
            }
        }
    }

    @Override
    protected boolean supportsScope(TableInspectionScope tableInspectionScope) {
        return tableInspectionScope.getSchema() != null && tableInspectionScope.getTable() != null;
    }
}
