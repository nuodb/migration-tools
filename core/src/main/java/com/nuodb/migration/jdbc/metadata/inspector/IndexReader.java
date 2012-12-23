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

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.nuodb.migration.jdbc.JdbcUtils.close;
import static com.nuodb.migration.jdbc.metadata.Identifier.valueOf;
import static java.sql.DatabaseMetaData.tableIndexStatistic;

/**
 * @author Sergey Bushik
 */
public class IndexReader extends MetaDataReaderBase {

    public IndexReader() {
        super(MetaDataType.INDEX);
    }

    @Override
    public void read(DatabaseInspector inspector, Database database,
                     DatabaseMetaData metaData) throws SQLException {
        for (Table table : database.listTables()) {
            readIndexes(database, metaData, table);
        }
    }

    protected void readIndexes(Database database, DatabaseMetaData metaData, Table table) throws SQLException {
        ResultSet indexes = metaData.getIndexInfo(
                table.getCatalog().getName(), table.getSchema().getName(), table.getName(), false, true);
        try {
            while (indexes.next()) {
                if (indexes.getShort("TYPE") == tableIndexStatistic) {
                    continue;
                }
                table = database.createCatalog(indexes.getString("TABLE_CAT")).createSchema(
                        indexes.getString("TABLE_SCHEM")).getTable(indexes.getString("TABLE_NAME"));
                Identifier identifier = valueOf(indexes.getString("INDEX_NAME"));
                Index index = table.getIndex(identifier);
                if (index == null) {
                    table.addIndex(index = new Index(identifier));
                    index.setUnique(!indexes.getBoolean("NON_UNIQUE"));
                    index.setFilterCondition(indexes.getString("FILTER_CONDITION"));
                    index.setSortOrder(getIndexSortOrder(indexes.getString("ASC_OR_DESC")));
                }
                index.addColumn(table.createColumn(indexes.getString("COLUMN_NAME")),
                        indexes.getInt("ORDINAL_POSITION"));
            }
        } finally {
            close(indexes);
        }
    }

    private static SortOrder getIndexSortOrder(String ascOrDesc) {
        if (ascOrDesc != null) {
            return ascOrDesc.equals("A") ? SortOrder.ASC : SortOrder.DESC;
        } else {
            return null;
        }
    }
}
