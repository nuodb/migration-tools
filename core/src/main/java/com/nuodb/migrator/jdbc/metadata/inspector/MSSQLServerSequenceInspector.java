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

import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Sequence;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.ParameterizedQuery;
import com.nuodb.migrator.jdbc.query.Query;
import com.nuodb.migrator.jdbc.query.SelectQuery;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.SEQUENCE;
import static com.nuodb.migrator.jdbc.metadata.inspector.InspectionResultsUtils.addTable;
import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * @author Sergey Bushik
 */
public class MSSQLServerSequenceInspector extends TableInspectorBase<Table, TableInspectionScope> {

    public MSSQLServerSequenceInspector() {
        super(SEQUENCE, TableInspectionScope.class);
    }

    @Override
    protected Query createQuery(InspectionContext inspectionContext, TableInspectionScope tableInspectionScope) {
        Collection<Object> parameters = newArrayList();
        SelectQuery selectColumn = new SelectQuery();
        if (isEmpty(tableInspectionScope.getCatalog())) {
            selectColumn.column("DB_NAME() AS TABLE_CATALOG");
        } else {
            selectColumn.column("? AS TABLE_CATALOG");
            parameters.add(tableInspectionScope.getCatalog());
        }
        selectColumn.column("SCHEMAS.NAME AS TABLE_SCHEMA");
        selectColumn.column("TABLES.NAME AS TABLE_NAME");
        selectColumn.column("COLUMNS.NAME AS COLUMN_NAME");

        String catalog = isEmpty(tableInspectionScope.getCatalog()) ? "" : (tableInspectionScope.getCatalog() + ".");
        selectColumn.from(catalog + "SYS.SCHEMAS");
        selectColumn.innerJoin(catalog + "SYS.TABLES", "SCHEMAS.SCHEMA_ID=TABLES.SCHEMA_ID");
        selectColumn.innerJoin(catalog + "SYS.COLUMNS", "COLUMNS.OBJECT_ID=TABLES.OBJECT_ID");

        if (!isEmpty(tableInspectionScope.getSchema())) {
            selectColumn.where("SCHEMAS.NAME=?");
            parameters.add(tableInspectionScope.getSchema());
        }
        if (!isEmpty(tableInspectionScope.getTable())) {
            selectColumn.where("TABLES.NAME=?");
            parameters.add(tableInspectionScope.getTable());
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
        return new ParameterizedQuery(selectIdentity, parameters);
    }

    @Override
    protected void processResultSet(InspectionContext inspectionContext, ResultSet sequences) throws SQLException {
        InspectionResults inspectionResults = inspectionContext.getInspectionResults();
        if (sequences.next()) {
            Table table = addTable(inspectionResults,
                    sequences.getString("TABLE_CATALOG"),
                    sequences.getString("TABLE_SCHEMA"),
                    sequences.getString("TABLE_NAME"));
            Sequence sequence = new Sequence();
            sequence.setStartWith(sequences.getLong("START_WITH"));
            sequence.setLastValue(sequences.getLong("LAST_VALUE"));
            sequence.setIncrementBy(sequences.getLong("INCREMENT_BY"));
            Column column = table.addColumn(sequences.getString("COLUMN_NAME"));
            column.setSequence(sequence);
            column.getTable().getSchema().addSequence(sequence);
            inspectionResults.addObject(sequence);
        }
    }
}
