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
import com.nuodb.migrator.jdbc.dialect.NuoDBDialect;
import com.nuodb.migrator.jdbc.metadata.Check;
import org.testng.annotations.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.nuodb.migrator.jdbc.metadata.MetaDataType.CHECK;
import static com.nuodb.migrator.jdbc.metadata.inspector.AssertUtils.assertTable;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class NuoDBCheckInspectorTest extends InspectorTestBase {

    public NuoDBCheckInspectorTest() {
        super(NuoDBCheckInspector.class, new NuoDBDialect());
    }

    @Test
    public void testInspect() throws SQLException {
        PreparedStatement query = mock(PreparedStatement.class);
        given(getConnection().prepareStatement(anyString(), anyInt(), anyInt())).willReturn(query);

        ResultSet resultSet = mock(ResultSet.class);
        given(query.executeQuery()).willReturn(resultSet);

        given(resultSet.next()).willReturn(true, true, false);

        String catalogName = null;
        String schemaName = "schema";
        String tableName = "table";
        String checkName1 = "table$constraint1";
        String checkName2 = "column";
        String checkClause1 = "column != null";
        String checkClause2 = "column > 0";
        given(resultSet.getString("SCHEMA")).willReturn(schemaName);
        given(resultSet.getString("TABLENAME")).willReturn(tableName);
        given(resultSet.getString("CONSTRAINTNAME")).willReturn(checkName1, checkName2);
        given(resultSet.getString("CONSTRAINTTEXT")).willReturn(checkClause1, checkClause2);

        TableInspectionScope inspectionScope = new TableInspectionScope(catalogName, schemaName, tableName);
        InspectionResults inspectionResults = getInspectionManager().inspect(inspectionScope, CHECK);

        Collection<Check> checks = inspectionResults.getObjects(CHECK);
        assertNotNull(checks);
        assertEquals(checks.size(), 2);

        Check check = Iterables.get(checks, 0);
        assertTable(null, schemaName, tableName, check.getTable());
        assertEquals(check.getName(), checkName1);
        assertEquals(check.getClause(), checkClause1);
        Collection columns = check.getColumns();
        assertTrue(columns.isEmpty());

        check = Iterables.get(checks, 1);
        assertTable(null, schemaName, tableName, check.getTable());
        assertEquals(check.getName(), checkName2);
        assertEquals(check.getClause(), checkClause2);

        columns = check.getColumns();
        assertEquals(columns.size(), 1);
    }
}
