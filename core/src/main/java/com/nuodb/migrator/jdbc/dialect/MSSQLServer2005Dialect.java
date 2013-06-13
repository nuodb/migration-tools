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
package com.nuodb.migrator.jdbc.dialect;

import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.SelectQuery;
import com.nuodb.migrator.jdbc.resolve.DatabaseInfo;

import static com.nuodb.migrator.jdbc.dialect.RowCountType.APPROX;

/**
 * @author Sergey Bushik
 */
public class MSSQLServer2005Dialect extends MSSQLServerDialect {

    public static final DatabaseInfo DATABASE_INFO = new DatabaseInfo("Microsoft SQL Server", null, 9);

    public MSSQLServer2005Dialect(DatabaseInfo databaseInfo) {
        super(databaseInfo);
    }

    /**
     * Row counts using SYS.DM_DB_PARTITION_STATS dynamic management view.
     * http://www.sqlservercentral.com/articles/T-SQL/67624/
     *
     * @param table to estimate row count for.
     * @return query used to estimate row count number.
     */
    @Override
    protected RowCountQuery createRowCountApproxQuery(Table table) {
        String catalog = table.getCatalog().getName() + ".";

        SelectQuery selectQuery = new SelectQuery();
        selectQuery.column("DDPS.ROW_COUNT");
        selectQuery.from(catalog + "SYS.INDEXES AS I");
        selectQuery.innerJoin(catalog + "SYS.TABLES AS T", "I.OBJECT_ID = t.OBJECT_ID");
        selectQuery.innerJoin(catalog + "SYS.SCHEMAS AS S", "T.SCHEMA_ID=S.SCHEMA_ID");
        selectQuery.innerJoin(catalog + "SYS.DM_DB_PARTITION_STATS AS DDPS",
                "I.OBJECT_ID = DDPS.OBJECT_ID AND I.INDEX_ID = DDPS.INDEX_ID");
        selectQuery.where("I.INDEX_ID < 2");
        selectQuery.where("T.IS_MS_SHIPPED=0");
        selectQuery.where("S.NAME='" + table.getSchema().getName() + "'");
        selectQuery.where("T.NAME='" + table.getName() + "'");

        RowCountQuery rowCountQuery = new RowCountQuery();
        rowCountQuery.setTable(table);
        rowCountQuery.setRowCountType(APPROX);
        rowCountQuery.setQuery(selectQuery);
        return rowCountQuery;
    }
}
