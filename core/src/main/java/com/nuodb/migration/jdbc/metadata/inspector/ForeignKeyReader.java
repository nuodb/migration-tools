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

import static com.nuodb.migration.jdbc.JdbcUtils.close;
import static com.nuodb.migration.jdbc.metadata.Identifier.valueOf;

/**
 * @author Sergey Bushik
 */
public class ForeignKeyReader extends MetaDataReaderBase {

    protected ReferentialActionMap referentialActionMap;
    protected DeferrabilityMap deferrabilityMap;

    public ForeignKeyReader() {
        super(MetaDataType.FOREIGN_KEY);
    }

    @Override
    public void read(DatabaseInspector inspector, Database database,
                     DatabaseMetaData databaseMetaData) throws SQLException {
        for (Table table : database.listTables()) {
            readForeignKeys(database, databaseMetaData, table);
        }
    }

    protected void readForeignKeys(Database database, DatabaseMetaData metaData, Table table) throws SQLException {
        ResultSet foreignKeys = metaData.getImportedKeys(
                table.getCatalog().getName(), table.getSchema().getName(), table.getName());
        try {
            boolean fixPosition = false;
            while (foreignKeys.next()) {
                Table sourceTable = database.createCatalog(foreignKeys.getString("FKTABLE_CAT")).createSchema(
                        foreignKeys.getString("FKTABLE_SCHEM")).createTable(foreignKeys.getString("FKTABLE_NAME"));
                final Column sourceColumn = sourceTable.createColumn(foreignKeys.getString("FKCOLUMN_NAME"));

                final Table targetTable = database.createCatalog(foreignKeys.getString("PKTABLE_CAT")).createSchema(
                        foreignKeys.getString("PKTABLE_SCHEM")).createTable(foreignKeys.getString("PKTABLE_NAME"));
                final Column targetColumn = targetTable.createColumn(foreignKeys.getString("PKCOLUMN_NAME"));

                int position = foreignKeys.getInt("KEY_SEQ");
                if (fixPosition || position == 0) {
                    fixPosition = true;
                }
                if (fixPosition) {
                    position += 1;
                }
                final Identifier identifier = valueOf(foreignKeys.getString("FK_NAME"));
                Optional<ForeignKey> foreignKeyOptional = Iterables.tryFind(sourceTable.getForeignKeys(),
                        new Predicate<ForeignKey>() {
                            @Override
                            public boolean apply(ForeignKey input) {
                                if (identifier != null) {
                                    return identifier.equals(input.getIdentifier());
                                }
                                for (ForeignKeyReference reference : input.getReferences()) {
                                    if (targetTable.equals(reference.getTargetTable())) {
                                        return true;
                                    }
                                }
                                return false;
                            }
                        });
                ForeignKey foreignKey;
                if (foreignKeyOptional.isPresent()) {
                    foreignKey = foreignKeyOptional.get();
                } else {
                    sourceTable.addForeignKey(foreignKey = new ForeignKey(identifier));
                    foreignKey.setSourceTable(sourceTable);
                    foreignKey.setTargetTable(targetTable);
                    foreignKey.setUpdateAction(getReferentialAction(foreignKeys.getInt("UPDATE_RULE")));
                    foreignKey.setDeleteAction(getReferentialAction(foreignKeys.getInt("DELETE_RULE")));
                    foreignKey.setDeferrability(getDeferrability(foreignKeys.getInt("DEFERRABILITY")));
                }
                foreignKey.addReference(sourceColumn, targetColumn, position);
            }
        } finally {
            close(foreignKeys);
        }
    }

    protected ReferenceAction getReferentialAction(int value) {
        ReferentialActionMap referentialActionMap = getReferentialActionMap() != null ?
                getReferentialActionMap() : ReferentialActionMap.getInstance();
        return referentialActionMap.get(value);
    }

    protected Deferrability getDeferrability(int value) {
        DeferrabilityMap deferrabilityMap = getDeferrabilityMap() != null ?
                getDeferrabilityMap() : DeferrabilityMap.getInstance();
        return deferrabilityMap.getDeferrability(value);
    }

    public ReferentialActionMap getReferentialActionMap() {
        return referentialActionMap;
    }

    public void setReferentialActionMap(ReferentialActionMap referentialActionMap) {
        this.referentialActionMap = referentialActionMap;
    }

    public DeferrabilityMap getDeferrabilityMap() {
        return deferrabilityMap;
    }

    public void setDeferrabilityMap(DeferrabilityMap deferrabilityMap) {
        this.deferrabilityMap = deferrabilityMap;
    }
}
