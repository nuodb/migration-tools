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
import com.nuodb.migrator.jdbc.metadata.ForeignKey;
import com.nuodb.migrator.jdbc.metadata.Identifier;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.ParameterizedQuery;
import com.nuodb.migrator.jdbc.query.Query;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.metadata.inspector.InspectionResultsUtils.addTable;
import static com.nuodb.migrator.jdbc.query.Queries.newQuery;
import static com.nuodb.migrator.utils.StringUtils.equalsIgnoreCase;

/**
 * @author Sergey Bushik
 */
public class NuoDBForeignKeyInspector extends ForeignKeyInspectorBase {

    private static final String QUERY = "SELECT PRIMARYTABLE.SCHEMA AS PKTABLE_SCHEM, PRIMARYTABLE.TABLENAME AS PKTABLE_NAME,\n"
            + "       PRIMARYFIELD.FIELD AS PKCOLUMN_NAME, FOREIGNTABLE.SCHEMA AS FKTABLE_SCHEM,\n"
            + "       FOREIGNTABLE.TABLENAME AS FKTABLE_NAME, FOREIGNFIELD.FIELD AS FKCOLUMN_NAME,\n"
            + "       FOREIGNKEYS.FOREIGNKEYNAME AS FK_NAME,\n"
            + "       FOREIGNKEYS.POSITION+1 AS KEY_SEQ, FOREIGNKEYS.UPDATERULE AS UPDATE_RULE,\n"
            + "       FOREIGNKEYS.DELETERULE AS DELETE_RULE, FOREIGNKEYS.DEFERRABILITY AS DEFERRABILITY\n"
            + "FROM SYSTEM.FOREIGNKEYS\n"
            + "INNER JOIN SYSTEM.TABLES PRIMARYTABLE ON PRIMARYTABLEID=PRIMARYTABLE.TABLEID\n"
            + "INNER JOIN SYSTEM.FIELDS PRIMARYFIELD ON PRIMARYTABLE.SCHEMA=PRIMARYFIELD.SCHEMA\n"
            + "AND PRIMARYTABLE.TABLENAME=PRIMARYFIELD.TABLENAME\n"
            + "AND FOREIGNKEYS.PRIMARYFIELDID=PRIMARYFIELD.FIELDID\n"
            + "INNER JOIN SYSTEM.TABLES FOREIGNTABLE ON FOREIGNTABLEID=FOREIGNTABLE.TABLEID\n"
            + "INNER JOIN SYSTEM.FIELDS FOREIGNFIELD ON FOREIGNTABLE.SCHEMA=FOREIGNFIELD.SCHEMA\n"
            + "AND FOREIGNTABLE.TABLENAME=FOREIGNFIELD.TABLENAME\n"
            + "AND FOREIGNKEYS.FOREIGNFIELDID=FOREIGNFIELD.FIELDID\n"
            + "WHERE FOREIGNTABLE.SCHEMA=? AND FOREIGNTABLE.TABLENAME=? ORDER BY PKTABLE_SCHEM, PKTABLE_NAME, "
            + "KEY_SEQ ASC";

    @Override
    protected Query createQuery(InspectionContext inspectionContext, TableInspectionScope tableInspectionScope) {
        Collection<Object> parameters = newArrayList();
        parameters.add(tableInspectionScope.getSchema());
        parameters.add(tableInspectionScope.getTable());
        return new ParameterizedQuery(newQuery(QUERY), parameters);
    }

    @Override
    protected void processResultSet(InspectionContext inspectionContext, TableInspectionScope tableInspectionScope,
            ResultSet foreignKeys) throws SQLException {
        InspectionResults inspectionResults = inspectionContext.getInspectionResults();
        ForeignKey foreignKey = null;
        while (foreignKeys.next()) {
            String primarySchemaName = foreignKeys.getString("PKTABLE_SCHEM");
            boolean addObject = (tableInspectionScope.getSchema() == null
                    || equalsIgnoreCase(tableInspectionScope.getSchema(), primarySchemaName));

            String primaryTableName = foreignKeys.getString("PKTABLE_NAME");
            final Table primaryTable = addTable(inspectionResults, null, primarySchemaName, primaryTableName,
                    addObject);

            final Column primaryColumn = primaryTable.addColumn(foreignKeys.getString("PKCOLUMN_NAME"));
            int position = foreignKeys.getInt("KEY_SEQ");

            Table foreignTable = addTable(inspectionResults, null, foreignKeys.getString("FKTABLE_SCHEM"),
                    foreignKeys.getString("FKTABLE_NAME"));
            final Column foreignColumn = foreignTable.addColumn(foreignKeys.getString("FKCOLUMN_NAME"));

            if (position == 1) {
                foreignTable.addForeignKey(foreignKey = new ForeignKey(foreignKeys.getString("FK_NAME")));
                foreignKey.setPrimaryTable(primaryTable);
                foreignKey.setForeignTable(foreignTable);
                foreignKey.setUpdateAction(getReferentialAction(foreignKeys.getInt("UPDATE_RULE")));
                foreignKey.setDeleteAction(getReferentialAction(foreignKeys.getInt("DELETE_RULE")));
                foreignKey.setDeferrability(getDeferrability(foreignKeys.getInt("DEFERRABILITY")));
                inspectionResults.addObject(foreignKey);
            }
            if (foreignKey != null) {
                foreignKey.addReference(primaryColumn, foreignColumn, position);
            }
        }
    }

    @Override
    protected boolean supportsScope(TableInspectionScope tableInspectionScope) {
        return tableInspectionScope.getSchema() != null && tableInspectionScope.getTable() != null;
    }
}
