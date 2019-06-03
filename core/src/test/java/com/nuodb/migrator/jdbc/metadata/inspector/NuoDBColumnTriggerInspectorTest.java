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

import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.ColumnTrigger;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.metadata.TriggerEvent;
import com.nuodb.migrator.jdbc.metadata.TriggerTime;

import org.testng.annotations.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;

import static com.google.common.collect.Iterables.get;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.COLUMN_TRIGGER;
import static com.nuodb.migrator.jdbc.metadata.MetaDataUtils.createTable;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

/**
 * @author Mukund
 */
@SuppressWarnings("unchecked")
public class NuoDBColumnTriggerInspectorTest extends InspectorTestBase {

    public NuoDBColumnTriggerInspectorTest() {
        super(NuoDBColumnTriggerInspector.class);
    }

    @Test
    public void testColumnTrigger() throws Exception {
        PreparedStatement query = mock(PreparedStatement.class);
        given(getConnection().prepareStatement(anyString(), anyInt(), anyInt())).willReturn(query);

        String catalogName = null;
        String schemaName = "schema";
        String tableName = "table";
        String type = "TABLE";

        Table table = createTable(catalogName, schemaName, tableName);
        table.addColumn("f1");
        InspectionResults inspectionResults = new SimpleInspectionResults();
        ResultSet resultSet = mock(ResultSet.class);
        given(query.executeQuery()).willReturn(resultSet);
        given(resultSet.next()).willReturn(true, false);
        given(resultSet.getInt("TRIGGER_TYPE")).willReturn(1);
        given(resultSet.getInt("TYPE_MASK")).willReturn(1);
        given(resultSet.getInt("ACTIVE")).willReturn(1);
        given(resultSet.getString("TABLENAME")).willReturn(tableName);
        given(resultSet.getString("TRIGGER_TEXT")).willReturn("NEW.`f1` = 'NOW'; END_TRIGGER;");
        inspectionResults.addObject(table);

        TableInspectionScope inspectionScope = new TableInspectionScope(catalogName, schemaName, tableName);
        getInspectionManager().inspect(getConnection(), inspectionResults, inspectionScope, COLUMN_TRIGGER);
        verifyInspectScope(getInspector(), inspectionScope);

        Collection<ColumnTrigger> cTriggers = inspectionResults.getObjects(COLUMN_TRIGGER);
        assertEquals(cTriggers.size(), 1);

        ColumnTrigger columnTrigger = new ColumnTrigger();
        Column column = new Column("f1");
        columnTrigger.setColumn(column);
        columnTrigger.setTriggerEvent(TriggerEvent.valueOf("INSERT"));
        columnTrigger.setTriggerTime(TriggerTime.valueOf("BEFORE"));
        columnTrigger.setTriggerBody("NEW.`f1` = 'NOW'; END_TRIGGER");

        Table table1 = createTable(catalogName, schemaName, tableName);
        table1.addColumn(column);
        table1.setType(type);
        table1.addTrigger(columnTrigger);
        assertEquals(get(cTriggers, 0).getTriggerBody() + " END_TRIGGER",
                table1.getTriggers().iterator().next().getTriggerBody());
    }
}