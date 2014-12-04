/**
 * Copyright (c) 2014, NuoDB, Inc.
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

import static com.google.common.collect.Iterables.get;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.USER_DEFINED;
import static com.nuodb.migrator.jdbc.metadata.MetaDataUtils.createSchema;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.Collection;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Schema;
import com.nuodb.migrator.jdbc.metadata.UserDefined;

/**
 * @author Mukund
 */

public class OracleUserDefineInspectorTest extends InspectorTestBase {

    String catalogName = null, schemaName = "schema", tableName = "table" ;

    public OracleUserDefineInspectorTest() {
        super(OracleUserDefinedInspector.class);
    }

    @Override
    @BeforeMethod
    public void setUp() throws Exception {
        super.setUp();
    }

    @DataProvider(name = "getUserDefinedData")
    public Object[][] createGetTypeNameData() throws Exception{
        UserDefined userDefined1 = new UserDefined("MTYPE");
        userDefined1.setUserDefinedName("MTYPE");
        userDefined1.setTypeCode("ARRAY");

        UserDefined userDefined2 = new UserDefined("DTYPE");
        userDefined2.setUserDefinedName("DTYPE");
        userDefined2.setTypeCode("STRUCT");

        return new Object[][] {
                { userDefined1 },
                { userDefined2 } 
        };
    }

    @Test(dataProvider = "getUserDefinedData")
    public void testUserDefinedType(UserDefined userDefined) throws Exception {
        configureUserDefinedResultSet(userDefined.getTypeCode(), userDefined.getUserDefinedName());

        TableInspectionScope tableInspectionScope = new TableInspectionScope(catalogName,schemaName, tableName);
        InspectionResults inspectionResults = getInspectionManager().inspect(getConnection(), tableInspectionScope,USER_DEFINED);
        verifyInspectScope(getInspector(), tableInspectionScope);

        Collection<UserDefined> userDefineds = inspectionResults.getObjects(USER_DEFINED);
        assertNotNull(userDefineds);
        assertEquals(userDefineds.size(), 1);
        assertEquals(get(userDefineds, 0).getUserDefinedName(), userDefined.getUserDefinedName());
    }

    private ResultSet configureUserDefinedResultSet(String typeCode, String typeName) throws Exception{
        PreparedStatement query = mock(PreparedStatement.class);
        given(getConnection().prepareStatement(anyString(), anyInt(), anyInt())).willReturn(query);
        given(getConnection().prepareStatement(anyString())).willReturn(query);

        ResultSet udResultSet = mock(ResultSet.class);
        given(udResultSet.next()).willReturn(true, false);
        given(udResultSet.getString("OWNER")).willReturn("ROOT");
        given(udResultSet.getString("TYPE_NAME")).willReturn(typeName);
        given(udResultSet.getString("TYPE_NAME")).willReturn(typeName);
        given(udResultSet.getString("TYPECODE")).willReturn(typeCode);
        given(query.executeQuery()).willReturn(udResultSet);

        return udResultSet;
    }
}
