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

import com.nuodb.migrator.jdbc.dialect.NuoDBDialect;
import com.nuodb.migrator.jdbc.metadata.Column;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.Collection;

import static com.google.common.collect.Iterables.get;
import static com.nuodb.migrator.jdbc.metadata.DefaultValue.valueOf;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.COLUMN;
import static com.nuodb.migrator.jdbc.metadata.MetaDataUtils.createColumn;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings({ "unchecked", "ConstantConditions" })
public class NuoDBColumnInspectorTest extends InspectorTestBase {

    public NuoDBColumnInspectorTest() {
        super(NuoDBColumnInspector.class);
    }

    @Override
    @BeforeMethod
    public void setUp() throws Exception {
        super.setUp();
        willResolveDialect(getInspectionManager(), new NuoDBDialect());
    }

    @Test
    public void testInspect() throws Exception {
        PreparedStatement query = mock(PreparedStatement.class);
        given(getConnection().prepareStatement(anyString(), anyInt(), anyInt())).willReturn(query);

        String catalogName = null;
        String schemaName = "schema";
        String tableName = "table";
        String columnName = "column";
        Integer typeCode = Types.VARCHAR;
        String typeName = "string";
        Integer columnSize = 4;
        Integer precision = 0;
        String defaultValue = null;
        Integer scale = 0;
        String comment = "remarks";
        Boolean nullable = false;
        Boolean autoIncrement = true;
        Integer position = 1;

        ResultSet resultSet = mock(ResultSet.class);
        given(query.executeQuery()).willReturn(resultSet);
        given(resultSet.next()).willReturn(true, false);
        given(resultSet.getString("SCHEMA")).willReturn(schemaName);
        given(resultSet.getString("TABLENAME")).willReturn(tableName);
        given(resultSet.getString("FIELD")).willReturn(columnName);
        given(resultSet.getInt("JDBCTYPE")).willReturn(typeCode);
        given(resultSet.getString("NAME")).willReturn(typeName);

        // Get the field type from databaseMataData
        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        given(getConnection().getMetaData()).willReturn(metaData);
        ResultSet columnsResultSet = mock(ResultSet.class);
        given(metaData.getColumns(anyString(), anyString(), anyString(), anyString())).willReturn(columnsResultSet);
        given(columnsResultSet.next()).willReturn(true, false);
        given(columnsResultSet.getInt("DATA_TYPE")).willReturn(typeCode);
        given(columnsResultSet.getString("TYPE_NAME")).willReturn(typeName);
        given(columnsResultSet.getString("COLUMN_NAME")).willReturn(columnName);

        given(resultSet.getInt("LENGTH")).willReturn(columnSize);
        given(resultSet.getString("DEFAULTVALUE")).willReturn(defaultValue);
        given(resultSet.getInt("SCALE")).willReturn(scale);
        given(resultSet.getString("REMARKS")).willReturn(comment);
        given(resultSet.getInt("PRECISION")).willReturn(precision);
        given(resultSet.getInt("FIELDPOSITION")).willReturn(position);
        given(resultSet.getInt("FLAGS")).willReturn(nullable ? 0 : 1);
        given(resultSet.getString("GENERATOR_SEQUENCE")).willReturn(autoIncrement ? "sequence" : null);

        TableInspectionScope inspectionScope = new TableInspectionScope(catalogName, schemaName, tableName);
        InspectionResults inspectionResults = getInspectionManager().inspect(getConnection(), inspectionScope, COLUMN);
        verifyInspectScope(getInspector(), inspectionScope);

        Collection<Column> columns = inspectionResults.getObjects(COLUMN);
        assertNotNull(columns);
        assertEquals(columns.size(), 1);

        Column column = createColumn(catalogName, schemaName, tableName, columnName);
        column.setTypeName(typeName);
        column.setTypeCode(typeCode);
        column.setSize(columnSize.longValue());
        column.setPrecision(precision);
        column.setDefaultValue(valueOf(defaultValue));
        column.setScale(scale);
        column.setPosition(position);
        column.setComment(comment);
        column.setNullable(nullable);
        column.setAutoIncrement(autoIncrement);

        assertEquals(get(columns, 0), column);
    }
}
