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

import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Sequence;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.ParameterizedQuery;
import com.nuodb.migrator.jdbc.query.Query;
import com.nuodb.migrator.jdbc.type.JdbcType;
import com.nuodb.migrator.jdbc.type.JdbcTypeDesc;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.metadata.DefaultValue.valueOf;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.COLUMN;
import static com.nuodb.migrator.jdbc.metadata.inspector.InspectionResultsUtils.addTable;
import static com.nuodb.migrator.jdbc.metadata.inspector.NuoDBColumn.getJdbcType;
import static com.nuodb.migrator.jdbc.query.Queries.newQuery;

/**
 * @author Sergey Bushik
 */
public class NuoDBColumnInspector extends TableInspectorBase<Table, TableInspectionScope> {

    private static final String QUERY =
            "SELECT * FROM SYSTEM.FIELDS AS F INNER JOIN SYSTEM.DATATYPES AS D ON F.DATATYPE = D.ID\n" +
                    "WHERE F.SCHEMA=? AND F.TABLENAME=? ORDER BY F.FIELDPOSITION ASC";

    public NuoDBColumnInspector() {
        super(COLUMN, TableInspectionScope.class);
    }

    @Override
    protected Query createQuery(InspectionContext inspectionContext, TableInspectionScope tableInspectionScope) {
        Collection<Object> parameters = newArrayList();
        parameters.add(tableInspectionScope.getSchema());
        parameters.add(tableInspectionScope.getTable());
        return new ParameterizedQuery(newQuery(QUERY), parameters);
    }

    @Override
    protected void processResultSet(InspectionContext inspectionContext, ResultSet columns) throws SQLException {
        InspectionResults inspectionResults = inspectionContext.getInspectionResults();
        Dialect dialect = inspectionContext.getDialect();
        while (columns.next()) {
            Table table = addTable(inspectionResults, null, columns.getString("SCHEMA"),
                    columns.getString("TABLENAME"));

            Column column = table.addColumn(columns.getString("FIELD"));
            JdbcType jdbcType = new JdbcType();

            // Get the field type from databaseMataData
            DatabaseMetaData databaseMataData = inspectionContext.getConnection().getMetaData();
            ResultSet rowMetaData = databaseMataData.getColumns(null, columns.getString("SCHEMA"), columns.getString("TABLENAME"), columns.getString("FIELD"));
            rowMetaData.next();
            JdbcTypeDesc typeDescAlias = dialect.getJdbcTypeAlias(
                    rowMetaData.getInt("DATA_TYPE"), rowMetaData.getString("TYPE_NAME"));

            jdbcType.setTypeCode(typeDescAlias.getTypeCode());
            jdbcType.setTypeName(typeDescAlias.getTypeName());

            jdbcType.setSize((long)columns.getInt("LENGTH"));
            jdbcType.setPrecision(columns.getInt("PRECISION"));
            jdbcType.setScale(columns.getInt("SCALE"));
            column.setJdbcType(getJdbcType(jdbcType, columns.getString("ENUMERATION")));

            column.setDefaultValue(valueOf(columns.getString("DEFAULTVALUE")));
            column.setComment(columns.getString("REMARKS"));
            column.setPosition(columns.getInt("FIELDPOSITION"));
            column.setNullable(columns.getInt("FLAGS") == 0);

            String identifier = columns.getString("GENERATOR_SEQUENCE");
            if (identifier != null) {
                Sequence sequence = new Sequence(identifier);
                column.setSequence(sequence);
                column.getTable().getSchema().addSequence(sequence);
            }
            column.setAutoIncrement(identifier != null);

            inspectionResults.addObject(column);
        }
    }

    @Override
    protected boolean supportsScope(TableInspectionScope tableInspectionScope) {
        return tableInspectionScope.getSchema() != null && tableInspectionScope.getTable() != null;
    }
}
