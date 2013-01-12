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

import com.nuodb.migration.jdbc.metadata.*;
import com.nuodb.migration.jdbc.query.StatementCallback;
import com.nuodb.migration.jdbc.query.StatementCreator;
import com.nuodb.migration.jdbc.query.StatementTemplate;

import java.sql.*;

import static com.nuodb.migration.jdbc.JdbcUtils.close;
import static com.nuodb.migration.jdbc.metadata.Identifier.EMPTY_IDENTIFIER;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;

/**
 * @author Sergey Bushik
 */
public class NuoDBForeignKeyReader extends NuoDBMetaDataReaderBase {

    private DeferrabilityMap deferrabilityMap;
    private ReferentialActionMap referentialActionMap;

    public static final String QUERY =
            "SELECT PRIMARYTABLE.SCHEMA AS PKTABLE_SCHEM,\n" +
            "       PRIMARYTABLE.TABLENAME AS PKTABLE_NAME,\n" +
            "       PRIMARYFIELD.FIELD AS PKCOLUMN_NAME,\n" +
            "       FOREIGNTABLE.SCHEMA AS FKTABLE_SCHEM,\n" +
            "       FOREIGNTABLE.TABLENAME AS FKTABLE_NAME,\n" +
            "       FOREIGNFIELD.FIELD AS FKCOLUMN_NAME,\n" +
            "       FOREIGNKEYS.POSITION+1 AS KEY_SEQ,\n" +
            "       FOREIGNKEYS.UPDATERULE AS UPDATE_RULE,\n" +
            "       FOREIGNKEYS.DELETERULE AS DELETE_RULE,\n" +
            "       FOREIGNKEYS.DEFERRABILITY AS DEFERRABILITY\n" +
            "FROM SYSTEM.FOREIGNKEYS\n" +
            "INNER JOIN SYSTEM.TABLES PRIMARYTABLE ON PRIMARYTABLEID=PRIMARYTABLE.TABLEID\n" +
            "INNER JOIN SYSTEM.FIELDS PRIMARYFIELD ON PRIMARYTABLE.SCHEMA=PRIMARYFIELD.SCHEMA\n" +
            "AND PRIMARYTABLE.TABLENAME=PRIMARYFIELD.TABLENAME\n" +
            "AND FOREIGNKEYS.PRIMARYFIELDID=PRIMARYFIELD.FIELDID\n" +
            "INNER JOIN SYSTEM.TABLES FOREIGNTABLE ON FOREIGNTABLEID=FOREIGNTABLE.TABLEID\n" +
            "INNER JOIN SYSTEM.FIELDS FOREIGNFIELD ON FOREIGNTABLE.SCHEMA=FOREIGNFIELD.SCHEMA\n" +
            "AND FOREIGNTABLE.TABLENAME=FOREIGNFIELD.TABLENAME\n" +
            "AND FOREIGNKEYS.FOREIGNFIELDID=FOREIGNFIELD.FIELDID\n" +
            "WHERE PRIMARYTABLE.SCHEMA=?\n" +
            "  AND PRIMARYTABLE.TABLENAME=?\n" +
            "ORDER BY PKTABLE_SCHEM,\n" +
            "         PKTABLE_NAME,\n" +
            "         KEY_SEQ ASC;";

    public NuoDBForeignKeyReader() {
        super(MetaDataType.FOREIGN_KEY);
    }

    @Override
    protected void doRead(DatabaseInspector inspector, final Database database,
                          DatabaseMetaData databaseMetaData) throws SQLException {
        final StatementTemplate template = new StatementTemplate(databaseMetaData.getConnection());
        template.execute(
                new StatementCreator<PreparedStatement>() {
                    @Override
                    public PreparedStatement create(Connection connection) throws SQLException {
                        return connection.prepareStatement(QUERY,
                                TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
                    }
                },
                new StatementCallback<PreparedStatement>() {
                    @Override
                    public void execute(PreparedStatement statement) throws SQLException {
                        for (Table table : database.listTables()) {
                            statement.setString(1, table.getSchema().getName());
                            statement.setString(2, table.getName());
                            ResultSet foreignKeys = statement.executeQuery();
                            try {
                                readForeignKeys(table, foreignKeys);
                            } finally {
                                close(foreignKeys);
                            }
                        }
                    }
                }
        );
    }

    protected void readForeignKeys(Table table, ResultSet foreignKeys) throws SQLException {
        Catalog catalog = table.getDatabase().createCatalog(EMPTY_IDENTIFIER);
        ForeignKey foreignKey = null;
        while (foreignKeys.next()) {
            Table primaryTable = catalog.createSchema(foreignKeys.getString("FKTABLE_SCHEM")).
                    createTable(foreignKeys.getString("FKTABLE_NAME"));
            final Column primaryColumn = primaryTable.createColumn(foreignKeys.getString("FKCOLUMN_NAME"));

            final Table foreignTable = catalog.createSchema(foreignKeys.getString("PKTABLE_SCHEM")).
                    createTable(foreignKeys.getString("PKTABLE_NAME"));
            final Column foreignColumn = foreignTable.createColumn(foreignKeys.getString("PKCOLUMN_NAME"));
            int position = foreignKeys.getInt("KEY_SEQ");

            if (position == 1) {
                primaryTable.addForeignKey(foreignKey = new ForeignKey(Identifier.EMPTY_IDENTIFIER));
                foreignKey.setPrimaryTable(primaryTable);
                foreignKey.setForeignTable(foreignTable);
                foreignKey.setUpdateAction(getReferentialAction(foreignKeys.getInt("UPDATE_RULE")));
                foreignKey.setDeleteAction(getReferentialAction(foreignKeys.getInt("DELETE_RULE")));
                foreignKey.setDeferrability(getDeferrability(foreignKeys.getInt("DEFERRABILITY")));
            }
            foreignKey.addReference(primaryColumn, foreignColumn, position);
        }
    }

    protected Deferrability getDeferrability(int value) {
        DeferrabilityMap deferrabilityMap = getDeferrabilityMap() != null ?
                getDeferrabilityMap() : DeferrabilityMap.getInstance();
        return deferrabilityMap.getDeferrability(value);
    }

    protected ReferenceAction getReferentialAction(int value) {
        ReferentialActionMap referentialActionMap = getReferentialActionMap() != null ?
                getReferentialActionMap() : ReferentialActionMap.getInstance();
        return referentialActionMap.get(value);
    }

    public DeferrabilityMap getDeferrabilityMap() {
        return deferrabilityMap;
    }

    public void setDeferrabilityMap(DeferrabilityMap deferrabilityMap) {
        this.deferrabilityMap = deferrabilityMap;
    }

    public ReferentialActionMap getReferentialActionMap() {
        return referentialActionMap;
    }

    public void setReferentialActionMap(ReferentialActionMap referentialActionMap) {
        this.referentialActionMap = referentialActionMap;
    }
}
