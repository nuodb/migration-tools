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
package com.nuodb.migrator.jdbc.dialect;

import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.SelectQuery;

import static com.nuodb.migrator.jdbc.dialect.RowCountType.APPROX;

/**
 * @author Sergey Bushik
 */
public class MSSQLServerTableRowCountHandler extends SimpleTableRowCountHandler {

    public MSSQLServerTableRowCountHandler(Dialect dialect, Table table, Column column, String filter,
            RowCountType rowCountType) {
        super(dialect, table, column, filter, rowCountType);
    }

    @Override
    protected TableRowCountQuery createApproxRowCountQuery() {
        if (getColumn() != null) {
            throw new DialectException("Approx row count query with column is not supported");
        }
        if (getFilter() != null) {
            throw new DialectException("Approx row count query with filter is not supported");
        }
        String catalog = getTable().getCatalog().getName() + ".";

        SelectQuery query = new SelectQuery();
        query.column("I.ROWCNT");
        query.from(catalog + "SYS.SYSINDEXES AS I");
        query.innerJoin(catalog + "SYS.TABLES AS T", "I.ID=T.OBJECT_ID");
        query.innerJoin(catalog + "SYS.SCHEMAS AS S", "T.SCHEMA_ID=S.SCHEMA_ID");
        query.where("I.INDID < 2");
        query.where("T.IS_MS_SHIPPED=0");
        query.where("S.NAME='" + getTable().getSchema().getName() + "'");
        query.where("T.NAME='" + getTable().getName() + "'");

        return new TableRowCountQuery(getTable(), getColumn(), getFilter(), query, APPROX);
    }
}
