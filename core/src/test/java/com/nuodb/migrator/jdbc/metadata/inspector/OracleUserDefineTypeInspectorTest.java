/**
 * Copyright (c) 2015, NuoDB, Inc.
 * All rights reserved.
 * <p/>
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * <p/>
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of NuoDB, Inc. nor the names of its contributors may
 * be used to endorse or promote products derived from this software
 * without specific prior written permission.
 * <p/>
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

import com.nuodb.migrator.jdbc.metadata.UserDefinedType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;

import static com.google.common.collect.Iterables.get;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.USER_DEFINED_TYPE;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * @author Mukund
 */
public class OracleUserDefineTypeInspectorTest extends InspectorTestBase {

    private String schemaName = "schema";
    private String tableName = "table";

    public OracleUserDefineTypeInspectorTest() {
        super(OracleUserDefinedTypeTypeInspector.class);
    }

    @DataProvider(name = "inspect")
    public Object[][] createInspectData() throws Exception {
        UserDefinedType userDefinedType1 = new UserDefinedType();
        userDefinedType1.setName("MTYPE");
        userDefinedType1.setCode("ARRAY");

        UserDefinedType userDefinedType2 = new UserDefinedType();
        userDefinedType2.setName("DTYPE");
        userDefinedType2.setCode("STRUCT");

        return new Object[][] { { userDefinedType1 }, { userDefinedType2 } };
    }

    @Test(dataProvider = "inspect")
    public void testInspect(UserDefinedType userDefinedType) throws Exception {
        createResultSet(userDefinedType.getName(), userDefinedType.getCode());

        TableInspectionScope tableInspectionScope = new TableInspectionScope(null, schemaName, tableName);
        InspectionResults inspectionResults = getInspectionManager().inspect(getConnection(), tableInspectionScope,
                USER_DEFINED_TYPE);
        verifyInspectScope(getInspector(), tableInspectionScope);

        Collection<UserDefinedType> userDefinedTypes = inspectionResults.getObjects(USER_DEFINED_TYPE);
        assertNotNull(userDefinedTypes);
        assertEquals(userDefinedTypes.size(), 1);
        assertEquals(get(userDefinedTypes, 0).getName(), userDefinedType.getName());
    }

    private ResultSet createResultSet(String name, String code) throws Exception {
        PreparedStatement query = mock(PreparedStatement.class);
        given(getConnection().prepareStatement(anyString(), anyInt(), anyInt())).willReturn(query);
        given(getConnection().prepareStatement(anyString())).willReturn(query);

        ResultSet resultSet = mock(ResultSet.class);
        given(resultSet.next()).willReturn(true, false);
        given(resultSet.getString("OWNER")).willReturn("ROOT");
        given(resultSet.getString("TYPE_NAME")).willReturn(name);
        given(resultSet.getString("TYPE_NAME")).willReturn(name);
        given(resultSet.getString("TYPECODE")).willReturn(code);
        given(query.executeQuery()).willReturn(resultSet);

        return resultSet;
    }
}
