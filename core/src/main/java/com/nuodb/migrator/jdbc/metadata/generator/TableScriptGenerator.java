/**
 * Copyright (c) 2014, NuoDB, Inc.
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
package com.nuodb.migrator.jdbc.metadata.generator;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.metadata.Check;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.DatabaseInfo;
import com.nuodb.migrator.jdbc.metadata.ForeignKey;
import com.nuodb.migrator.jdbc.metadata.Index;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.metadata.PrimaryKey;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.type.JdbcType;

import java.util.Collection;
import java.util.Iterator;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.*;
import static java.lang.String.format;
import static java.util.Collections.singleton;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.join;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class TableScriptGenerator extends ScriptGeneratorBase<Table> {

    public TableScriptGenerator() {
        super(Table.class);
    }

    @Override
    public Collection<String> getCreateScripts(Table table, ScriptGeneratorManager scriptGeneratorManager) {
        Dialect dialect = scriptGeneratorManager.getTargetDialect();
        StringBuilder buffer = new StringBuilder("CREATE TABLE");
        buffer.append(' ').append(scriptGeneratorManager.getName(table)).append(" (");
        Collection<Column> columns = table.getColumns();
        Collection<Index> indexes = table.getIndexes();
        Collection<MetaDataType> objectTypes = scriptGeneratorManager.getObjectTypes();
        for (Iterator<Column> iterator = columns.iterator(); iterator.hasNext(); ) {
            final Column column = iterator.next();
            buffer.append(scriptGeneratorManager.getName(column));
            buffer.append(' ');
            buffer.append(getTypeName(column, scriptGeneratorManager));
            if (column.isIdentity() && objectTypes.contains(SEQUENCE)) {
                buffer.append(' ');
                buffer.append(dialect.getIdentityColumn(
                        column.getSequence() != null ?
                                scriptGeneratorManager.getName(column.getSequence()) : null));
            }
            if (column.isNullable()) {
                buffer.append(dialect.getNullColumnString());
            } else {
                buffer.append(' ');
                buffer.append("NOT NULL");
            }
            String defaultValue = dialect.getDefaultValue(scriptGeneratorManager.getSourceSession(), column);
            if (defaultValue != null) {
                buffer.append(" DEFAULT ").append(defaultValue);
            }
            if (objectTypes.contains(INDEX)) {
                Optional<Index> index = Iterables.tryFind(indexes, new Predicate<Index>() {
                    @Override
                    public boolean apply(Index index) {
                        Collection<Column> columns = index.getColumns();
                        return columns.size() == 1 && columns.contains(column) &&
                                !index.isPrimary() && index.isUnique();
                    }
                });
                boolean unique = index.isPresent() && (!column.isNullable() || dialect.supportsNotNullUnique());
                if (unique) {
                    if (dialect.supportsUniqueInCreateTable()) {
                        buffer.append(' ');
                        buffer.append("UNIQUE");
                        indexes.remove(index.get());
                    }
                }
            }
            if (objectTypes.contains(CHECK) && dialect.supportsColumnCheck()) {
                for (Check check : column.getChecks()) {
                    buffer.append(", CHECK ");
                    buffer.append(dialect.getCheckClause(check.getText()));
                }
            }
            String comment = column.getComment();
            if (!isEmpty(comment)) {
                buffer.append(dialect.getColumnComment(comment));
            }
            if (iterator.hasNext()) {
                buffer.append(", ");
            }
        }
        if (objectTypes.contains(PRIMARY_KEY)) {
            PrimaryKey primaryKey = table.getPrimaryKey();
            if (primaryKey != null) {
                ConstraintScriptGenerator<PrimaryKey> generator = (ConstraintScriptGenerator<PrimaryKey>)
                        scriptGeneratorManager.getScriptGenerator(primaryKey);
                buffer.append(", ").append(generator.getConstraintScript(primaryKey, scriptGeneratorManager));
            }
        }
        if (objectTypes.contains(INDEX) && dialect.supportsIndexInCreateTable()) {
            boolean primary = false;
            for (Index index : indexes) {
                if (!primary && index.isPrimary()) {
                    primary = true;
                    continue;
                }
                ConstraintScriptGenerator<Index> generator = (ConstraintScriptGenerator<Index>)
                        scriptGeneratorManager.getScriptGenerator(index);
                String constraint = generator.getConstraintScript(index, scriptGeneratorManager);
                if (constraint != null) {
                    buffer.append(", ").append(constraint);
                }

            }
        }
        if (objectTypes.contains(FOREIGN_KEY)) {
            for (ForeignKey foreignKey : table.getForeignKeys()) {
                ConstraintScriptGenerator<ForeignKey> generator = (ConstraintScriptGenerator<ForeignKey>)
                        scriptGeneratorManager.getScriptGenerator(foreignKey);
                String constraint = generator.getConstraintScript(foreignKey, scriptGeneratorManager);
                if (constraint != null) {
                    buffer.append(", ").append(constraint);
                }
            }
        }
        if (objectTypes.contains(CHECK) && dialect.supportsTableCheck()) {
            for (Check check : table.getChecks()) {
                buffer.append(", CHECK ");
                buffer.append(dialect.getCheckClause(check.getText()));
            }
        }
        buffer.append(')');
        String comment = table.getComment();
        if (!isEmpty(comment)) {
            buffer.append(dialect.getTableComment(comment));
        }
        return singleton(buffer.toString());
    }

    @Override
    public Collection<String> getDropScripts(Table table, ScriptGeneratorManager scriptGeneratorManager) {
        Dialect dialect = scriptGeneratorManager.getTargetDialect();
        StringBuilder buffer = new StringBuilder("DROP TABLE");
        buffer.append(' ');
        boolean ifExistsBeforeTable;
        if (ifExistsBeforeTable = dialect.supportsIfExistsBeforeDropTable()) {
            buffer.append("IF EXISTS");
            buffer.append(' ');
        }
        buffer.append(scriptGeneratorManager.getName(table));
        String cascadeConstraints = dialect.getCascadeConstraints();
        if (cascadeConstraints != null) {
            buffer.append(' ');
            buffer.append(cascadeConstraints);
        }
        if (!ifExistsBeforeTable && dialect.supportsIfExistsAfterDropTable()) {
            buffer.append(' ');
            buffer.append("IF EXISTS");
        }
        return singleton(buffer.toString());
    }

    protected String getTypeName(Column column, ScriptGeneratorManager scriptGeneratorManager) {
        Dialect dialect = scriptGeneratorManager.getTargetDialect();
        int scale = column.getScale();
        JdbcType jdbcType = column.getJdbcType();
        if (scale < 0 && !dialect.supportsNegativeScale()) {
            jdbcType = jdbcType.withScale(0);
        }
        DatabaseInfo databaseInfo = scriptGeneratorManager.getSourceSession().getDialect().getDatabaseInfo();
        String typeName = dialect.getTypeName(databaseInfo, jdbcType);
        if (typeName == null) {
            String tableName = scriptGeneratorManager.getQualifiedName(column.getTable(),
                    column.getTable().getSchema().getName(), column.getTable().getCatalog().getName(), false);
            String columnName = scriptGeneratorManager.getName(column, false);
            Collection<String> typeInfo = newArrayList();
            typeInfo.add(format("type name %s", column.getTypeName()));
            typeInfo.add(format("type code %s", column.getTypeCode()));
            if (column.getSize() != null) {
                typeInfo.add(format("length %d", column.getSize()));
            }
            if (column.getPrecision() != null) {
                typeInfo.add(format("precision %d", column.getPrecision()));
            }
            if (column.getScale() != null) {
                typeInfo.add(format("scale %d", column.getScale()));
            }
            throw new GeneratorException(format("Unsupported type on table %s column %s: %s",
                            tableName, columnName, join(typeInfo, ", ")));
        }
        return typeName;
    }
}
