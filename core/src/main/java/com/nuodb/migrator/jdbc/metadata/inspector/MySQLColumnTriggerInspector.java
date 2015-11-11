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

import com.nuodb.migrator.jdbc.metadata.*;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.query.ParameterizedQuery;
import com.nuodb.migrator.jdbc.query.Query;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.COLUMN_TRIGGER;
import static com.nuodb.migrator.jdbc.metadata.inspector.InspectionResultsUtils.addTable;
import static com.nuodb.migrator.jdbc.query.Queries.newQuery;
import static com.nuodb.migrator.jdbc.query.QueryUtils.where;
import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static java.util.regex.Pattern.compile;
import static org.apache.commons.lang3.StringUtils.containsAny;

/**
 * @author Sergey Bushik
 */
public class MySQLColumnTriggerInspector extends TableInspectorBase<Table, TableInspectionScope> {

    private static final String COLUMN_TRIGGER_REGEX = "ON (\\w+) (.*)";
    private static final Pattern COLUMN_TRIGGER_PATTERN = compile(COLUMN_TRIGGER_REGEX, CASE_INSENSITIVE);

    private static final String QUERY = "SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, EXTRA FROM INFORMATION_SCHEMA.COLUMNS";

    public MySQLColumnTriggerInspector() {
        super(COLUMN_TRIGGER, TableInspectionScope.class);
    }

    @Override
    protected Query createQuery(InspectionContext inspectionContext, TableInspectionScope tableInspectionScope) {
        StringBuilder query = new StringBuilder(QUERY);
        Collection<String> filters = newArrayList();
        Collection<Object> parameters = newArrayList();

        String catalog = tableInspectionScope.getCatalog();
        if (catalog != null) {
            filters.add(containsAny(tableInspectionScope.getCatalog(), "%") ? "TABLE_SCHEMA LIKE ?" : "TABLE_SCHEMA=?");
            parameters.add(catalog);
        } else {
            filters.add("TABLE_SCHEMA=DATABASE()");
        }

        String table = tableInspectionScope.getTable();
        if (table != null) {
            filters.add(containsAny(tableInspectionScope.getCatalog(), "%") ? "TABLE_NAME LIKE ?" : "TABLE_NAME=?");
            parameters.add(table);
        }

        filters.add("EXTRA LIKE 'ON %'");
        where(query, filters, "AND");
        return new ParameterizedQuery(newQuery(query.toString()), parameters);
    }

    @Override
    protected void processResultSet(InspectionContext inspectionContext, ResultSet triggers) throws SQLException {
        InspectionResults inspectionResults = inspectionContext.getInspectionResults();
        while (triggers.next()) {
            Matcher matcher = COLUMN_TRIGGER_PATTERN.matcher(triggers.getString("EXTRA"));
            if (matcher.matches()) {
                Table table = addTable(inspectionResults, triggers.getString("TABLE_SCHEMA"), null,
                        triggers.getString("TABLE_NAME"));
                ColumnTrigger columnTrigger = new ColumnTrigger();
                Column column = table.addColumn(triggers.getString("COLUMN_NAME"));
                columnTrigger.setColumn(column);

                columnTrigger.setTriggerEvent(TriggerEvent.fromAlias(matcher.group(1)));
                columnTrigger.setTriggerTime(TriggerTime.BEFORE);
                columnTrigger.setTriggerBody(matcher.group(2));

                table.addTrigger(columnTrigger);
                inspectionResults.addObject(columnTrigger);
            }
        }
    }
}
