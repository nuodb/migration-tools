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
package com.nuodb.migration.jdbc.metadata.generator;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.nuodb.migration.jdbc.dialect.Dialect;
import com.nuodb.migration.jdbc.metadata.*;
import com.nuodb.migration.jdbc.type.JdbcTypeNameMap;

import java.util.Collection;
import java.util.Iterator;

import static com.nuodb.migration.jdbc.metadata.MetaDataType.*;

/**
 * @author Sergey Bushik
 */
public class TableGenerator implements ScriptGenerator<Table> {

    @Override
    public Class<Table> getRelationalType() {
        return Table.class;
    }

    @Override
    public String[] getCreateSql(Table table, ScriptGeneratorContext context) {
        Dialect dialect = context.getDialect();
        Collection<MetaDataType> metaDataTypes = context.getMetaDataTypes();
        String catalog = context.getCatalog();
        String schema = context.getSchema();

        StringBuilder buffer = new StringBuilder("CREATE TABLE");
        buffer.append(' ').append(table.getQualifiedName(dialect, catalog, schema)).append(" (");
        JdbcTypeNameMap jdbcTypeNameMap = dialect.getJdbcTypeRegistry().getJdbcTypeNameMap();
        Collection<Column> columns = table.getColumns();
        Collection<Index> indexes = Lists.newArrayList(table.getIndexes());
        for (Iterator<Column> iterator = columns.iterator(); iterator.hasNext(); ) {
            final Column column = iterator.next();
            buffer.append(column.getQuotedName(dialect));
            buffer.append(' ');
            buffer.append(jdbcTypeNameMap.getTypeName(column.getTypeCode(), column.getSize(), column.getPrecision(),
                    column.getScale()));
            if (column.isIdentity()) {
                buffer.append(' ');
                buffer.append(dialect.getIdentityColumnString());
            }
            String defaultValue = column.getDefaultValue();
            if (defaultValue != null) {
                buffer.append(" DEFAULT ").append(dialect.getDefaultValue(column.getTypeCode(), defaultValue));
            }
            if (column.isNullable()) {
                buffer.append(dialect.getNullColumnString());
            } else {
                buffer.append(" NOT NULL");
            }
            if (metaDataTypes.contains(INDEX)) {
                Optional<Index> index = Iterables.tryFind(indexes, new Predicate<Index>() {
                    @Override
                    public boolean apply(Index index) {
                        Collection<Column> columns = index.getColumns();
                        return columns.size() == 1 && columns.contains(column) &&
                                index.getSortOrder() == null && index.getFilterCondition() == null;
                    }
                });
                boolean unique = index.isPresent() && (!column.isNullable() || dialect.supportsNotNullUnique());
                if (unique) {
                    if (dialect.supportsUnique()) {
                        buffer.append(" UNIQUE");
                        indexes.remove(index.get());
                    }
                }
            }
            if (metaDataTypes.contains(COLUMN_CHECK)) {
                String check = column.getCheck();
                if (check != null && dialect.supportsColumnCheck()) {
                    buffer.append(" CHECK ");
                    buffer.append(dialect.getColumnCheck(check));
                }
            }
            String comment = column.getComment();
            if (comment != null) {
                buffer.append(dialect.getColumnComment(comment));
            }
            if (iterator.hasNext()) {
                buffer.append(", ");
            }
        }
        PrimaryKey primaryKey = table.getPrimaryKey();
        if (primaryKey != null) {
            ConstraintGenerator<PrimaryKey> generator = (ConstraintGenerator<PrimaryKey>)
                    context.getScriptGenerator(primaryKey);
            buffer.append(", ").append(generator.getConstraintSql(primaryKey, context));
        }
        if (metaDataTypes.contains(INDEX) && dialect.supportsIndexInCreateTable()) {
            for (Index index : indexes) {
                ConstraintGenerator<Index> generator = (ConstraintGenerator<Index>)
                        context.getScriptGenerator(index);
                String constraint = generator.getConstraintSql(index, context);
                if (constraint != null) {
                    buffer.append(", ").append(constraint);
                }

            }
        }
        if (metaDataTypes.contains(FOREIGN_KEY)) {
            for (ForeignKey foreignKey : table.getForeignKeys()) {
                ConstraintGenerator<ForeignKey> generator = (ConstraintGenerator<ForeignKey>)
                        context.getScriptGenerator(foreignKey);
                String constraint = generator.getConstraintSql(foreignKey, context);
                if (constraint != null) {
                    buffer.append(", ").append(constraint);
                }
            }
        }
        if (metaDataTypes.contains(TABLE_CHECK)) {
            if (dialect.supportsTableCheck()) {
                for (String check : table.getChecks()) {
                    buffer.append(", CHECK ");
                    buffer.append(dialect.getTableCheck(check));
                }
            }
        }
        buffer.append(')');
        String comment = table.getComment();
        if (comment != null) {
            buffer.append(dialect.getTableComment(comment));
        }
        return new String[]{buffer.toString()};
    }

    @Override
    public String[] getDropSql(Table table, ScriptGeneratorContext context) {
        Dialect dialect = context.getDialect();
        StringBuilder buffer = new StringBuilder("DROP TABLE");
        buffer.append(' ');
        boolean ifExistsBeforeTable;
        if (ifExistsBeforeTable = dialect.supportsIfExistsBeforeTable()) {
            buffer.append("IF EXISTS");
            buffer.append(' ');
        }
        buffer.append(table.getQualifiedName(dialect));
        String cascadeConstraints = dialect.getCascadeConstraintsString();
        if (cascadeConstraints != null) {
            buffer.append(' ');
            buffer.append(cascadeConstraints);
        }
        if (!ifExistsBeforeTable && dialect.supportsIfExistsAfterTable()) {
            buffer.append(' ');
            buffer.append("IF EXISTS");
        }
        return new String[]{buffer.toString()};
    }
}
