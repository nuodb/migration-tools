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
package com.nuodb.migration.jdbc.metadata.inspector;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.nuodb.migration.jdbc.metadata.*;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.nuodb.migration.jdbc.JdbcUtils.close;
import static com.nuodb.migration.jdbc.metadata.Identifier.valueOf;
import static com.nuodb.migration.jdbc.metadata.inspector.InspectionResultsUtils.addTable;

/**
 * @author Sergey Bushik
 */
public class SimpleForeignKeyInspector extends ForeignKeyInspectorBase {

    @Override
    protected Collection<? extends TableInspectionScope> createInspectionScopes(Collection<? extends Table> tables) {
        return createTableInspectionScopes(tables);
    }

    @Override
    protected void inspectScopes(final InspectionContext inspectionContext,
                                 final Collection<? extends TableInspectionScope> inspectionScopes) throws SQLException {
        InspectionResults inspectionResults = inspectionContext.getInspectionResults();
        DatabaseMetaData databaseMetaData = inspectionContext.getConnection().getMetaData();
        for (TableInspectionScope inspectionScope : inspectionScopes) {
            ResultSet foreignKeys = databaseMetaData.getImportedKeys(
                    inspectionScope.getCatalog(), inspectionScope.getSchema(), inspectionScope.getTable());
            try {
                boolean fixPosition = false;
                while (foreignKeys.next()) {
                    Table primaryTable = addTable(inspectionResults, foreignKeys.getString("FKTABLE_CAT"),
                            foreignKeys.getString("FKTABLE_SCHEM"), foreignKeys.getString("FKTABLE_NAME"));

                    final Column primaryColumn = primaryTable.createColumn(foreignKeys.getString("FKCOLUMN_NAME"));

                    final Table foreignTable = addTable(inspectionResults, foreignKeys.getString("PKTABLE_CAT"),
                            foreignKeys.getString("PKTABLE_SCHEM"), foreignKeys.getString("PKTABLE_NAME"));

                    final Column foreignColumn = foreignTable.createColumn(foreignKeys.getString("PKCOLUMN_NAME"));

                    int position = foreignKeys.getInt("KEY_SEQ");
                    if (fixPosition || position == 0) {
                        fixPosition = true;
                    }
                    if (fixPosition) {
                        position += 1;
                    }
                    final Identifier identifier = valueOf(foreignKeys.getString("FK_NAME"));
                    Optional<ForeignKey> optional = Iterables.tryFind(primaryTable.getForeignKeys(),
                            new Predicate<ForeignKey>() {
                                @Override
                                public boolean apply(ForeignKey foreignKey) {
                                    if (identifier != null) {
                                        return identifier.equals(foreignKey.getIdentifier());
                                    }
                                    for (ForeignKeyReference foreignKeyReference : foreignKey.getReferences()) {
                                        if (foreignTable.equals(foreignKeyReference.getForeignTable())) {
                                            return true;
                                        }
                                    }
                                    return false;
                                }
                            });
                    ForeignKey foreignKey;
                    if (optional.isPresent()) {
                        foreignKey = optional.get();
                    } else {
                        primaryTable.addForeignKey(foreignKey = new ForeignKey(identifier));
                        foreignKey.setPrimaryTable(primaryTable);
                        foreignKey.setForeignTable(foreignTable);
                        foreignKey.setUpdateAction(getReferentialAction(foreignKeys.getInt("UPDATE_RULE")));
                        foreignKey.setDeleteAction(getReferentialAction(foreignKeys.getInt("DELETE_RULE")));
                        foreignKey.setDeferrability(getDeferrability(foreignKeys.getInt("DEFERRABILITY")));

                        inspectionResults.addObject(foreignKey);
                    }
                    foreignKey.addReference(primaryColumn, foreignColumn, position);
                }
            } finally {
                close(foreignKeys);
            }
        }
    }

    @Override
    protected boolean supports(TableInspectionScope inspectionScope) {
        return inspectionScope.getTable() != null;
    }
}
