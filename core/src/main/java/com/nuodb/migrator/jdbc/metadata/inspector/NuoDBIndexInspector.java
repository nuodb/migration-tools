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

import com.nuodb.migrator.jdbc.metadata.Identifier;
import com.nuodb.migrator.jdbc.metadata.Index;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.Query;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.nuodb.migrator.jdbc.metadata.Identifier.valueOf;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.INDEX;
import static com.nuodb.migrator.jdbc.metadata.inspector.InspectionResultsUtils.addTable;
import static com.nuodb.migrator.jdbc.metadata.inspector.NuoDBIndex.KEY;
import static com.nuodb.migrator.jdbc.metadata.inspector.NuoDBIndex.UNIQUE;
import static com.nuodb.migrator.jdbc.metadata.inspector.NuoDBIndex.UNIQUECONSTRAINT;

/**
 * @author Sergey Bushik
 */
public class NuoDBIndexInspector extends TableInspectorBase<Table, TableInspectionScope> {

    public NuoDBIndexInspector() {
        super(INDEX, TableInspectionScope.class);
    }

    @Override
    protected Query createQuery(InspectionContext inspectionContext, TableInspectionScope tableInspectionScope) {
        return NuoDBIndex.createQuery(tableInspectionScope, UNIQUE, KEY, UNIQUECONSTRAINT);
    }

    @Override
    protected void processResultSet(InspectionContext inspectionContext, ResultSet indexes) throws SQLException {
        InspectionResults inspectionResults = inspectionContext.getInspectionResults();
        while (indexes.next()) {
            Table table = addTable(inspectionResults, null, indexes.getString("SCHEMA"),
                    indexes.getString("TABLENAME"));
            Identifier identifier = valueOf(indexes.getString("INDEXNAME"));
            Index index = table.getIndex(identifier);
            if (index == null) {
                table.addIndex(index = new Index(identifier));
                index.setUnique(
                        indexes.getInt("INDEXTYPE") == UNIQUE || indexes.getInt("INDEXTYPE") == UNIQUECONSTRAINT);
                index.setUniqueConstraint(indexes.getInt("INDEXTYPE") == UNIQUECONSTRAINT);
            }
            index.addColumn(table.addColumn(indexes.getString("FIELD")), indexes.getInt("POSITION"));
            inspectionResults.addObject(index);
        }
    }

    @Override
    protected boolean supportsScope(TableInspectionScope tableInspectionScope) {
        return tableInspectionScope.getSchema() != null && tableInspectionScope.getTable() != null;
    }
}
