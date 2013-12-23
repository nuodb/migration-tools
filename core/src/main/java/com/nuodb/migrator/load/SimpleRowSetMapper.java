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
package com.nuodb.migrator.load;

import com.nuodb.migrator.backup.catalog.QueryRowSet;
import com.nuodb.migrator.backup.catalog.RowSet;
import com.nuodb.migrator.backup.catalog.TableRowSet;
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.spec.ConnectionSpec;
import org.slf4j.Logger;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.metadata.IdentifiableBase.getQualifiedName;
import static com.nuodb.migrator.utils.CollectionUtils.addIgnoreNull;
import static java.lang.Math.min;
import static java.lang.String.format;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Sergey Bushik
 */
public class SimpleRowSetMapper implements RowSetMapper {

    private final transient Logger logger = getLogger(getClass());

    @Override
    public Table map(RowSet rowSet, Database database) {
        Table table = null;
        if (rowSet instanceof TableRowSet) {
            table = map((TableRowSet) rowSet, database);
        } else if (rowSet instanceof QueryRowSet) {
            table = map((QueryRowSet) rowSet, database);
        }
        return table;
    }

    private Table map(TableRowSet tableRowSet, Database database) {
        Dialect dialect = database.getDialect();
        List<String> allQualifiers = newArrayList();
        int maximum = 0;
        if (dialect.supportsCatalogs()) {
            maximum++;
        }
        if (dialect.supportsSchemas()) {
            maximum++;
        }
        ConnectionSpec connectionSpec = database.getConnectionSpec();
        if (connectionSpec.getCatalog() != null || connectionSpec.getSchema() != null) {
            addIgnoreNull(allQualifiers, connectionSpec.getCatalog());
            addIgnoreNull(allQualifiers, connectionSpec.getSchema());
        } else {
            addIgnoreNull(allQualifiers, tableRowSet.getCatalogName());
            addIgnoreNull(allQualifiers, tableRowSet.getSchemaName());
        }
        int actual = min(allQualifiers.size(), maximum);
        List<String> qualifiers = allQualifiers.subList(allQualifiers.size() - actual, allQualifiers.size());
        final String sourceTable = getQualifiedName(null,
                tableRowSet.getCatalogName(), tableRowSet.getSchemaName(), tableRowSet.getTableName(), null);
        final String targetTable = getQualifiedName(null,
                qualifiers, tableRowSet.getTableName(), null);
        if (logger.isDebugEnabled()) {
            logger.debug(format("Mapping source %s row set to %s table", sourceTable, targetTable));
        }
        return database.findTable(targetTable);
    }

    private Table map(QueryRowSet queryRowSet, Database database) {
        if (logger.isWarnEnabled()) {
            logger.warn(format("Can't map %s query row set %s to a table, explicit mapping is required",
                    queryRowSet.getName(), queryRowSet.getQuery()));
        }
        return null;
    }
}
