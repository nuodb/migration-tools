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

import com.nuodb.migrator.jdbc.metadata.Sequence;
import org.testng.annotations.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;

import static com.google.common.collect.Iterables.get;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.SEQUENCE;
import static com.nuodb.migrator.jdbc.metadata.MetaDataUtils.createSequence;
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
public class MSSQLServerSequenceInspectorTest extends InspectorTestBase {

    public MSSQLServerSequenceInspectorTest() {
        super(MSSQLServerSequenceInspector.class);
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
        String columnName = "column";
        long startWith = 1L;
        long lastValue = 100L;
        long incrementBy = 1L;

        given(resultSet.next()).willReturn(true, false);
        given(resultSet.getString("table_catalog")).willReturn(catalogName);
        given(resultSet.getString("table_schema")).willReturn(schemaName);
        given(resultSet.getString("table_name")).willReturn(tableName);
        given(resultSet.getLong("start_with")).willReturn(startWith);
        given(resultSet.getLong("last_value")).willReturn(lastValue);
        given(resultSet.getLong("increment_by")).willReturn(incrementBy);
        given(resultSet.getString("column_name")).willReturn(columnName);

        TableInspectionScope inspectionScope = new TableInspectionScope(catalogName, schemaName, tableName);
        InspectionResults inspectionResults = getInspectionManager().inspect(getConnection(), inspectionScope,
                SEQUENCE);

        verifyInspectScope(getInspector(), inspectionScope);
        Collection<Sequence> sequences = inspectionResults.getObjects(SEQUENCE);

        assertNotNull(sequences);
        assertEquals(sequences.size(), 1);

        Sequence sequence = createSequence(catalogName, schemaName, tableName, columnName);
        sequence.setStartWith(startWith);
        sequence.setLastValue(lastValue);
        sequence.setIncrementBy(incrementBy);
        assertEquals(get(sequences, 0), sequence);
    }
}
