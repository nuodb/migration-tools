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
import com.nuodb.migrator.jdbc.metadata.Sequence;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.ParameterizedQuery;
import com.nuodb.migrator.jdbc.query.Query;
import com.nuodb.migrator.jdbc.query.SelectQuery;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.SEQUENCE;
import static com.nuodb.migrator.jdbc.metadata.inspector.InspectionResultsUtils.addTable;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.trim;

/**
 * @author Sergey Bushik
 */
public class DB2SequenceInspector extends TableInspectorBase<Table, TableInspectionScope> {

    public DB2SequenceInspector() {
        super(SEQUENCE, TableInspectionScope.class);
    }

    @Override
    protected Query createQuery(InspectionContext inspectionContext, TableInspectionScope tableInspectionScope) {
        SelectQuery query = new SelectQuery();
        query.columns("T.TABSCHEMA", "T.TABNAME", "C.COLNAME", "S.SEQNAME", "S.INCREMENT", "S.START", "S.MINVALUE",
                "S.MAXVALUE", "S.CYCLE", "S.CACHE", "S.ORDER");
        query.from("SYSCAT.SEQUENCES S");
        query.innerJoin("SYSCAT.TABLES T", "S.SEQSCHEMA=T.TABSCHEMA AND S.CREATE_TIME=T.CREATE_TIME");
        query.innerJoin("SYSCAT.COLUMNS C", "T.TABSCHEMA=C.TABSCHEMA AND T.TABNAME=C.TABNAME");
        query.where("S.SEQNAME LIKE 'SQL%'");
        query.where("C.IDENTITY='Y'");
        Collection<Object> parameters = newArrayList();
        String schema = tableInspectionScope.getSchema();
        if (!isEmpty(schema)) {
            query.where("T.TABSCHEMA=?");
            parameters.add(tableInspectionScope.getSchema());
        }
        String table = tableInspectionScope.getTable();
        if (!isEmpty(table)) {
            query.where("T.TABNAME=?");
            parameters.add(table);
        }
        return new ParameterizedQuery(query, parameters);
    }

    @Override
    protected void processResultSet(InspectionContext inspectionContext, ResultSet sequences) throws SQLException {
        InspectionResults inspectionResults = inspectionContext.getInspectionResults();
        while (sequences.next()) {
            Table table = addTable(inspectionResults, null, trim(sequences.getString("TABSCHEMA")),
                    trim(sequences.getString("TABNAME")));
            Column column = table.addColumn(sequences.getString("COLNAME"));

            Sequence sequence = new Sequence(sequences.getString("SEQNAME"));
            sequence.setStartWith(sequences.getLong("START"));
            sequence.setIncrementBy(sequences.getLong("INCREMENT"));
            sequence.setMinValue(sequences.getLong("MINVALUE"));
            sequence.setMaxValue(sequences.getLong("MAXVALUE"));
            sequence.setCache(sequences.getInt("CACHE"));
            sequence.setOrder(StringUtils.equals("Y", sequences.getString("ORDER")));
            column.setSequence(sequence);
            column.getTable().getSchema().addSequence(sequence);
            inspectionResults.addObject(sequence);
        }
    }
}
