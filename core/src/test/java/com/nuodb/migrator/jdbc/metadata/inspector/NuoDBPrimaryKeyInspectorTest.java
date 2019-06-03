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

import com.nuodb.migrator.jdbc.metadata.PrimaryKey;
import com.nuodb.migrator.jdbc.metadata.Table;
import org.testng.annotations.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;

import static com.google.common.collect.Iterables.get;
import static com.nuodb.migrator.jdbc.metadata.Identifier.valueOf;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.PRIMARY_KEY;
import static com.nuodb.migrator.jdbc.metadata.MetaDataUtils.createTable;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class NuoDBPrimaryKeyInspectorTest extends InspectorTestBase {

    public NuoDBPrimaryKeyInspectorTest() {
        super(NuoDBPrimaryKeyInspector.class);
    }

    @Test
    public void testInspect() throws Exception {
        PreparedStatement query = mock(PreparedStatement.class);
        given(getConnection().prepareStatement(anyString(), anyInt(), anyInt())).willReturn(query);

        String catalogName = null;
        String schemaName = "schema";
        String tableName = "table";
        String columnName = "column";
        String indexName = "pk";
        int position = 0;

        ResultSet resultSet = mock(ResultSet.class);
        given(query.executeQuery()).willReturn(resultSet);
        given(resultSet.next()).willReturn(true, false);
        given(resultSet.getString("SCHEMA")).willReturn(schemaName);
        given(resultSet.getString("TABLENAME")).willReturn(tableName);
        given(resultSet.getString("FIELD")).willReturn(columnName);
        given(resultSet.getString("INDEXNAME")).willReturn(indexName);
        given(resultSet.getInt("INDEXTYPE")).willReturn(NuoDBIndex.PRIMARY_KEY);
        given(resultSet.getInt("POSITION")).willReturn(position);

        TableInspectionScope inspectionScope = new TableInspectionScope(catalogName, schemaName, tableName);
        InspectionResults inspectionResults = getInspectionManager().inspect(getConnection(), inspectionScope,
                PRIMARY_KEY);
        verifyInspectScope(getInspector(), inspectionScope);

        Collection<PrimaryKey> primaryKeys = inspectionResults.getObjects(PRIMARY_KEY);
        assertEquals(primaryKeys.size(), 1);

        Table table = createTable(catalogName, schemaName, tableName);
        PrimaryKey primaryKey = new PrimaryKey(valueOf(indexName));
        table.setPrimaryKey(primaryKey);
        primaryKey.addColumn(table.addColumn(columnName), position);

        assertEquals(get(primaryKeys, 0), primaryKey);
    }
}
