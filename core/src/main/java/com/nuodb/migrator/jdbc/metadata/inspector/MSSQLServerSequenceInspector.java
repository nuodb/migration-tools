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
            selectColumn.column("db_name() as table_catalog");
        } else {
            selectColumn.column("? as table_catalog");
            parameters.add(tableInspectionScope.getCatalog());
        }
        selectColumn.column("schemas.name as table_schema");
        selectColumn.column("tables.name as table_name");
        selectColumn.column("columns.name as column_name");

        String catalog = isEmpty(tableInspectionScope.getCatalog()) ? "" : (tableInspectionScope.getCatalog() + ".");
        selectColumn.from(catalog + "sys.schemas");
        selectColumn.innerJoin(catalog + "sys.tables", "schemas.schema_id=tables.schema_id");
        selectColumn.innerJoin(catalog + "sys.columns", "columns.object_id=tables.object_id");

        if (!isEmpty(tableInspectionScope.getSchema())) {
            selectColumn.where("schemas.name=?");
            parameters.add(tableInspectionScope.getSchema());
        }
        if (!isEmpty(tableInspectionScope.getTable())) {
            selectColumn.where("tables.name=?");
            parameters.add(tableInspectionScope.getTable());
        }
        selectColumn.where("is_identity=1");

        SelectQuery selectTable = new SelectQuery();
        selectTable.column("c.*");
        selectTable.column(
                "quotename(table_catalog) + '.' + quotename(table_schema) + '.' + quotename(table_name) as table_qualified_name");
        selectTable.from("(" + selectColumn + ") c");

        SelectQuery selectIdentity = new SelectQuery();
        selectIdentity.column("c.*");
        selectIdentity.column("ident_seed(table_qualified_name) as start_with");
        selectIdentity.column("ident_current(table_qualified_name) as last_value");
        selectIdentity.column("ident_incr(table_qualified_name) as increment_by");
        selectIdentity.from("(" + selectTable + ") c");
        return new ParameterizedQuery(selectIdentity, parameters);
    }

    @Override
    protected void processResultSet(InspectionContext inspectionContext, ResultSet sequences) throws SQLException {
        InspectionResults inspectionResults = inspectionContext.getInspectionResults();
        if (sequences.next()) {
            Table table = addTable(inspectionResults, sequences.getString("table_catalog"),
                    sequences.getString("table_schema"), sequences.getString("table_name"));
            Sequence sequence = new Sequence();
            sequence.setStartWith(sequences.getLong("start_with"));
            sequence.setLastValue(sequences.getLong("last_value"));
            sequence.setIncrementBy(sequences.getLong("increment_by"));
            Column column = table.addColumn(sequences.getString("column_name"));
            column.setSequence(sequence);
            column.getTable().getSchema().addSequence(sequence);
            inspectionResults.addObject(sequence);
        }
    }
}
