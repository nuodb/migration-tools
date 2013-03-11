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
import com.nuodb.migrator.jdbc.metadata.DeferrabilityMap;
import com.nuodb.migrator.jdbc.metadata.ForeignKey;
import com.nuodb.migrator.jdbc.metadata.ReferenceActionMap;
import org.testng.annotations.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.nuodb.migrator.jdbc.metadata.MetaDataType.FOREIGN_KEY;
import static com.nuodb.migrator.jdbc.metadata.inspector.AssertUtils.assertTable;
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

    public NuoDBForeignKeyInspectorTest() {
        super(NuoDBForeignKeyInspector.class);
    }

    @Test
    public void testInspect() throws SQLException {
        PreparedStatement query = mock(PreparedStatement.class);
        given(getConnection().prepareStatement(anyString(), anyInt(), anyInt())).willReturn(query);

        ResultSet resultSet = mock(ResultSet.class);
        given(query.executeQuery()).willReturn(resultSet);

        String pkCatalogName = null;
        String pkSchemaName = "schema";
        String pkTableName = "pk table";
        String pkColumnName = "pk column";
        String fkCatalogName = null;
        String fkSchemaName = "schema";
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

        TableInspectionScope inspectionScope = new TableInspectionScope(pkCatalogName, pkSchemaName, pkTableName);
        InspectionResults inspectionResults = getInspectionManager().inspect(inspectionScope, FOREIGN_KEY);

        Collection<ForeignKey> foreignKeys = inspectionResults.getObjects(FOREIGN_KEY);
        assertEquals(foreignKeys.size(), 1);

        ForeignKey foreignKey = Iterables.get(foreignKeys, 0);
        assertNotNull(foreignKey);
        ReferenceActionMap referenceActionMap = ReferenceActionMap.getInstance();
        assertEquals(foreignKey.getUpdateAction(), referenceActionMap.get(updateRule));
        assertEquals(foreignKey.getDeleteAction(), referenceActionMap.get(deleteRule));

        DeferrabilityMap deferrabilityMap = DeferrabilityMap.getInstance();
        assertEquals(foreignKey.getDeferrability(), deferrabilityMap.get(deferrability));
        assertTable(pkCatalogName, pkSchemaName, pkTableName, foreignKey.getPrimaryTable());
        assertTable(fkCatalogName, fkSchemaName, fkTableName, foreignKey.getForeignTable());
    }
}
