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

import com.google.common.collect.Iterables;
import com.nuodb.migrator.jdbc.metadata.*;
import org.testng.annotations.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.nuodb.migrator.jdbc.metadata.MetaDataType.AUTO_INCREMENT;
import static com.nuodb.migrator.jdbc.metadata.inspector.AssertUtils.assertTable;
import static com.nuodb.migrator.jdbc.metadata.inspector.MSSQLServerAutoIncrementInspector.QUERY;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class MSSQLServerAutoIncrementInspectorTest extends InspectorTestBase {

    public MSSQLServerAutoIncrementInspectorTest() {
        super(MSSQLServerAutoIncrementInspector.class);
    }

    @Test
    public void testInspect() throws SQLException {
        PreparedStatement query = mock(PreparedStatement.class);
        given(getConnection().prepareStatement(eq(QUERY), anyInt(), anyInt())).willReturn(query);

        ResultSet resultSet = mock(ResultSet.class);
        given(query.executeQuery()).willReturn(resultSet);

        String catalogName = "catalog";
        String schemaName = "schema";
        String tableName = "table";
        Long startWith = 1L;
        Long lastValue = 100L;
        Long incrementBy = 1L;
        String columnName = "column";

        given(resultSet.next()).willReturn(true, false);
        given(resultSet.getString("TABLE_CATALOG")).willReturn(catalogName);
        given(resultSet.getString("TABLE_SCHEMA")).willReturn(schemaName);
        given(resultSet.getString("TABLE_NAME")).willReturn(tableName);
        given(resultSet.getLong("START_WITH")).willReturn(startWith);
        given(resultSet.getLong("LAST_VALUE")).willReturn(lastValue);
        given(resultSet.getLong("INCREMENT_BY")).willReturn(incrementBy);
        given(resultSet.getString("COLUMN_NAME")).willReturn(columnName);

        TableInspectionScope inspectionScope = new TableInspectionScope(catalogName, schemaName, tableName);
        InspectionResults inspectionResults = getInspectionManager().inspect(inspectionScope, AUTO_INCREMENT);

        Collection<AutoIncrement> autoIncrements = inspectionResults.getObjects(AUTO_INCREMENT);
        assertNotNull(autoIncrements);
        assertEquals(autoIncrements.size(), 1);

        AutoIncrement autoIncrement = Iterables.get(autoIncrements, 0);
        assertEquals(autoIncrement.getStartWith(), startWith);
        assertEquals(autoIncrement.getLastValue(), lastValue);
        assertEquals(autoIncrement.getIncrementBy(), incrementBy);

        Column column = autoIncrement.getColumn();
        assertNotNull(column);
        assertEquals(column.getName(), columnName);

        assertTable(catalogName, schemaName, tableName, column.getTable());
    }
}
