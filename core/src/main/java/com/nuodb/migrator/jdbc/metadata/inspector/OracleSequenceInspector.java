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

import com.nuodb.migrator.jdbc.metadata.Schema;
import com.nuodb.migrator.jdbc.metadata.Sequence;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.ParameterizedQuery;
import com.nuodb.migrator.jdbc.query.Query;
import com.nuodb.migrator.jdbc.query.SelectQuery;
import com.nuodb.migrator.utils.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.SEQUENCE;
import static com.nuodb.migrator.jdbc.metadata.inspector.InspectionResultsUtils.addSchema;
import static com.nuodb.migrator.utils.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.containsAny;

/**
 * @author Sergey Bushik
 */
public class OracleSequenceInspector extends TableInspectorBase<Table, TableInspectionScope> {

    public OracleSequenceInspector() {
        super(SEQUENCE, TableInspectionScope.class);
    }

    @Override
    protected Query createQuery(InspectionContext inspectionContext, TableInspectionScope tableInspectionScope) {
        SelectQuery query = new SelectQuery();
        Collection<String> parameters = newArrayList();
        query.columns("SEQUENCE_OWNER", "SEQUENCE_NAME", "MIN_VALUE", "MAX_VALUE", "INCREMENT_BY", "CYCLE_FLAG",
                "ORDER_FLAG", "CACHE_SIZE", "LAST_NUMBER");
        query.from("ALL_SEQUENCES");
        String schema = tableInspectionScope.getSchema();
        if (!isEmpty(schema)) {
            query.where(containsAny(schema, "%") ? "SEQUENCE_OWNER LIKE ? ESCAPE '/'" : "SEQUENCE_OWNER=?");
            parameters.add(schema);
        }
        return new ParameterizedQuery(query, parameters);
    }

    @Override
    protected void processResultSet(InspectionContext inspectionContext, ResultSet sequences) throws SQLException {
        InspectionResults inspectionResults = inspectionContext.getInspectionResults();
        while (sequences.next()) {
            Schema schema = addSchema(inspectionResults, null, sequences.getString("SEQUENCE_OWNER"));
            Sequence sequence = new Sequence(sequences.getString("SEQUENCE_NAME"));
            sequence.setMinValue(sequences.getBigDecimal("MIN_VALUE"));
            sequence.setMaxValue(sequences.getBigDecimal("MAX_VALUE"));
            sequence.setIncrementBy(sequences.getBigDecimal("INCREMENT_BY"));
            sequence.setLastValue(sequences.getBigDecimal("LAST_NUMBER"));
            sequence.setCache(sequences.getBigDecimal("CACHE_SIZE"));
            sequence.setCycle(StringUtils.equals(sequences.getString("CYCLE_FLAG"), "Y"));
            sequence.setOrder(StringUtils.equals(sequences.getString("ORDER_FLAG"), "Y"));
            schema.addSequence(sequence);
            inspectionResults.addObject(sequence);
        }
    }
}
