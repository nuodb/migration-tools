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

import com.nuodb.migrator.jdbc.metadata.Identifier;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.metadata.PrimaryKey;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.ParameterizedQuery;
import com.nuodb.migrator.jdbc.query.Query;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.metadata.inspector.InspectionResultsUtils.addTable;
import static com.nuodb.migrator.jdbc.metadata.inspector.NuoDBIndex.PRIMARY_KEY;
import static com.nuodb.migrator.jdbc.query.Queries.newQuery;

/**
 * @author Sergey Bushik
 */
public class NuoDBPrimaryKeyInspector extends TableInspectorBase<Table, TableInspectionScope> {

    private static final String QUERY = NuoDBIndex.createQuery(PRIMARY_KEY);

    public NuoDBPrimaryKeyInspector() {
        super(MetaDataType.PRIMARY_KEY, TableInspectionScope.class);
    }

    @Override
    protected Query createQuery(InspectionContext inspectionContext, TableInspectionScope tableInspectionScope) {
        Collection<Object> parameters = newArrayList();
        parameters.add(tableInspectionScope.getSchema());
        parameters.add(tableInspectionScope.getTable());
        return new ParameterizedQuery(newQuery(QUERY), parameters);
    }

    @Override
    protected void processResultSet(InspectionContext inspectionContext, ResultSet primaryKeys) throws SQLException {
        InspectionResults inspectionResults = inspectionContext.getInspectionResults();
        while (primaryKeys.next()) {
            Table table = addTable(inspectionResults, null, primaryKeys.getString("SCHEMA"),
                    primaryKeys.getString("TABLENAME"));

            final Identifier identifier = Identifier.valueOf(primaryKeys.getString("INDEXNAME"));
            PrimaryKey primaryKey = table.getPrimaryKey();
            if (primaryKey == null) {
                table.setPrimaryKey(primaryKey = new PrimaryKey(identifier));
            }
            primaryKey.addColumn(table.addColumn(primaryKeys.getString("FIELD")), primaryKeys.getInt("POSITION"));
            inspectionResults.addObject(primaryKey);
        }
    }

    @Override
    protected boolean supportsScope(TableInspectionScope tableInspectionScope) {
        return tableInspectionScope.getSchema() != null && tableInspectionScope.getTable() != null;
    }
}
