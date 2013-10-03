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

import com.nuodb.migrator.jdbc.metadata.*;
import org.testng.annotations.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;

import static com.google.common.collect.Iterables.get;
import static com.nuodb.migrator.jdbc.metadata.Identifier.EMPTY;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.FOREIGN_KEY;
import static com.nuodb.migrator.jdbc.metadata.MetaDataUtils.createTable;
import static java.sql.DatabaseMetaData.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class NuoDBForeignKeyInspectorTest extends InspectorTestBase {

    private static DeferrabilityMap DEFERRABILITY_MAP = DeferrabilityMap.getInstance();
    private static ReferenceActionMap REFERENCE_ACTION_MAP = ReferenceActionMap.getInstance();

    public NuoDBForeignKeyInspectorTest() {
        super(NuoDBForeignKeyInspector.class);
    }

    @Test
    public void testInspect() throws Exception {
        PreparedStatement query = mock(PreparedStatement.class);
        given(getConnection().prepareStatement(anyString(), anyInt(), anyInt())).willReturn(query);

        ResultSet resultSet = mock(ResultSet.class);
        given(query.executeQuery()).willReturn(resultSet);

        String catalogName = null;
        String pkSchemaName = "pk schema";
        String pkTableName = "pk table";
        String pkColumnName = "pk column";
        String fkSchemaName = "fk schema";
        String fkTableName = "fk table";
        String fkColumnName = "fk column";

        int position = 1;
        int updateRule = importedKeySetNull;
        int deleteRule = importedKeySetDefault;
        int deferrability = importedKeyNotDeferrable;

        given(resultSet.next()).willReturn(true, false);
        given(resultSet.getString("FKTABLE_SCHEM")).willReturn(pkSchemaName);
        given(resultSet.getString("FKTABLE_NAME")).willReturn(pkTableName);
        given(resultSet.getString("FKCOLUMN_NAME")).willReturn(pkColumnName);
        given(resultSet.getString("PKTABLE_SCHEM")).willReturn(fkSchemaName);
        given(resultSet.getString("PKTABLE_NAME")).willReturn(fkTableName);
        given(resultSet.getString("PKCOLUMN_NAME")).willReturn(fkColumnName);
        given(resultSet.getInt("KEY_SEQ")).willReturn(position);
        given(resultSet.getInt("UPDATE_RULE")).willReturn(updateRule);
        given(resultSet.getInt("DELETE_RULE")).willReturn(deleteRule);
        given(resultSet.getInt("DEFERRABILITY")).willReturn(deferrability);

        TableInspectionScope inspectionScope = new TableInspectionScope(catalogName, pkSchemaName, pkTableName);
        InspectionResults inspectionResults = getInspectionManager().inspect(getConnection(), inspectionScope, FOREIGN_KEY);
        verifyInspectScope(getInspector(), inspectionScope);

        Collection<ForeignKey> foreignKeys = inspectionResults.getObjects(FOREIGN_KEY);
        assertEquals(foreignKeys.size(), 1);

        Table pkTable = createTable(catalogName, pkSchemaName, pkTableName);
        Column pkColumn = pkTable.addColumn(pkColumnName);
        Table fkTable = createTable(catalogName, fkSchemaName, fkTableName);
        Column fkColumn = fkTable.addColumn(fkColumnName);

        ForeignKey foreignKey = new ForeignKey(EMPTY);
        foreignKey.setPrimaryTable(pkTable);
        foreignKey.setForeignTable(fkTable);
        foreignKey.addReference(pkColumn, fkColumn, position);
        foreignKey.setDeferrability(DEFERRABILITY_MAP.get(deferrability));
        foreignKey.setUpdateAction(REFERENCE_ACTION_MAP.get(updateRule));
        foreignKey.setDeleteAction(REFERENCE_ACTION_MAP.get(deleteRule));
        pkTable.addForeignKey(foreignKey);

        assertNotNull(foreignKey);
        assertEquals(get(foreignKeys, 0), foreignKey);
    }
}
