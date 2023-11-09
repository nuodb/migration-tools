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
import com.nuodb.migrator.jdbc.metadata.MetaDataUtils;
import com.nuodb.migrator.jdbc.metadata.Table;
import org.testng.annotations.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;

import static com.google.common.collect.Iterables.get;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.CHECK;
import static org.mockito.BDDMockito.given;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class MSSQLServerCheckInspectorTest extends InspectorTestBase {

    public MSSQLServerCheckInspectorTest() {
        super(MSSQLServerCheckInspector.class);
    }

    @Test
    public void testInspect() throws Exception {
        PreparedStatement query = mock(PreparedStatement.class);
        given(getConnection().prepareStatement(anyString(), anyInt(), anyInt())).willReturn(query);

        ResultSet resultSet = mock(ResultSet.class);
        given(query.executeQuery()).willReturn(resultSet);

        String catalogName = "catalog";
        String schemaName = "schema";
        String tableName = "table";
        String checkName = "check";
        String checkClause = "clause";

        given(resultSet.next()).willReturn(true, false);
        given(resultSet.getString("TABLE_CATALOG")).willReturn(catalogName);
        given(resultSet.getString("TABLE_SCHEMA")).willReturn(schemaName);
        given(resultSet.getString("TABLE_NAME")).willReturn(tableName);
        given(resultSet.getString("CONSTRAINT_NAME")).willReturn(checkName);
        given(resultSet.getString("CHECK_CLAUSE")).willReturn(checkClause);

        TableInspectionScope inspectionScope = new TableInspectionScope(catalogName, schemaName, tableName);
        InspectionResults inspectionResults = getInspectionManager().inspect(getConnection(), inspectionScope, CHECK);
        verifyInspectScope(getInspector(), inspectionScope);

        Collection<Check> checks = inspectionResults.getObjects(CHECK);
        assertNotNull(checks);
        assertEquals(checks.size(), 1);

        Table table = MetaDataUtils.createTable(catalogName, schemaName, tableName);
        Check check = table.addCheck(new Check(checkName, checkClause));
        assertEquals(get(checks, 0), check);
    }
}
