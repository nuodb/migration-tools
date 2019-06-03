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
package com.nuodb.migrator.backup.loader;

import com.nuodb.migrator.backup.QueryRowSet;
import com.nuodb.migrator.backup.RowSet;
import com.nuodb.migrator.backup.TableRowSet;
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.spec.ConnectionSpec;
import org.slf4j.Logger;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.metadata.IdentifiableBase.getQualifiedName;
import static com.nuodb.migrator.utils.Collections.addIgnoreNull;
import static java.lang.Math.min;
import static java.lang.String.format;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Sergey Bushik
 */
public class SimpleRowSetMapper implements RowSetMapper {

    private final transient Logger logger = getLogger(getClass());

    @Override
    public Table mapRowSet(RowSet rowSet, BackupLoaderContext backupLoaderContext) {
        Table table = null;
        if (rowSet instanceof TableRowSet) {
            table = mapRowSet((TableRowSet) rowSet, backupLoaderContext);
        } else if (rowSet instanceof QueryRowSet) {
            table = mapRowSet((QueryRowSet) rowSet, backupLoaderContext);
        }
        return table;
    }

    protected Table mapRowSet(TableRowSet rowSet, BackupLoaderContext backupLoaderContext) {
        Database database = backupLoaderContext.getDatabase();
        Dialect dialect = database.getDialect();
        List<String> qualifiers = newArrayList();
        int maximum = 0;
        if (dialect.supportsCatalogs()) {
            maximum++;
        }
        if (dialect.supportsSchemas()) {
            maximum++;
        }
        ConnectionSpec targetSpec = backupLoaderContext.getTargetSpec();
        if (targetSpec.getCatalog() != null || targetSpec.getSchema() != null) {
            addIgnoreNull(qualifiers, targetSpec.getCatalog());
            addIgnoreNull(qualifiers, targetSpec.getSchema());
        } else {
            addIgnoreNull(qualifiers, rowSet.getCatalog());
            addIgnoreNull(qualifiers, rowSet.getSchema());
        }
        int actual = min(qualifiers.size(), maximum);
        qualifiers = qualifiers.subList(qualifiers.size() - actual, qualifiers.size());
        final String source = getQualifiedName(null, rowSet.getCatalog(), rowSet.getSchema(), rowSet.getTable(), null);
        final String target = getQualifiedName(null, qualifiers, rowSet.getTable(), null);
        if (logger.isDebugEnabled()) {
            logger.debug(format("Mapping %s row set to %s table", source, target));
        }
        return database.findTable(target);
    }

    protected Table mapRowSet(QueryRowSet rowSet, BackupLoaderContext backupLoaderContext) {
        if (logger.isWarnEnabled()) {
            logger.warn(format("Can't map %s query row set %s to a table, explicit mapping is required",
                    rowSet.getName(), rowSet.getQuery()));
        }
        return null;
    }
}
