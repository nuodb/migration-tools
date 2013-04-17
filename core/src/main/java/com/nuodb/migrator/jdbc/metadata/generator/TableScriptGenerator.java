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
package com.nuodb.migrator.jdbc.metadata.generator;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.metadata.*;
import com.nuodb.migrator.jdbc.type.JdbcTypeDesc;
import com.nuodb.migrator.jdbc.type.JdbcTypeSpecifiers;

import java.util.Collection;
import java.util.Iterator;

import static com.nuodb.migrator.jdbc.metadata.MetaDataType.*;
import static com.nuodb.migrator.jdbc.type.JdbcTypeSpecifiers.newSpecifiers;
import static java.lang.String.format;
import static java.util.Collections.singleton;
import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * @author Sergey Bushik
 */
public class TableScriptGenerator extends ScriptGeneratorBase<Table> {

    public TableScriptGenerator() {
        super(Table.class);
    }

    @Override
    public Collection<String> getCreateScripts(Table table, ScriptGeneratorContext scriptGeneratorContext) {
        Dialect dialect = scriptGeneratorContext.getDialect();
        StringBuilder buffer = new StringBuilder("CREATE TABLE");
        buffer.append(' ').append(scriptGeneratorContext.getQualifiedName(table)).append(" (");
        Collection<Column> columns = table.getColumns();
        final Collection<Index> indexes = table.getIndexes();
        Collection<MetaDataType> metaDataTypes = scriptGeneratorContext.getObjectTypes();
        Dialect sourceDialect = table.getDatabase().getDialect();
        for (Iterator<Column> iterator = columns.iterator(); iterator.hasNext(); ) {
            final Column column = iterator.next();
            buffer.append(scriptGeneratorContext.getName(column));
            buffer.append(' ');
            buffer.append(getTypeName(column, scriptGeneratorContext));
            if (column.isIdentity() && metaDataTypes.contains(AUTO_INCREMENT)) {
                buffer.append(' ');
                buffer.append(dialect.getIdentityColumn(
                        column.getSequence() != null ?
                                scriptGeneratorContext.getName(column.getSequence()) : null));
            }
            if (column.isNullable()) {
                buffer.append(dialect.getNullColumnString());
            } else {
                buffer.append(' ');
                buffer.append("NOT NULL");
            }
            String defaultValue = null;
            if (column.getDefaultValue() != null) {
                defaultValue = dialect.getDefaultValue(
                        column.getTypeCode(), column.getDefaultValue().getValue(), sourceDialect);
            }
            if (defaultValue != null) {
                buffer.append(" DEFAULT ").append(defaultValue);
            }
            if (metaDataTypes.contains(INDEX)) {
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
            if (metaDataTypes.contains(CHECK) && dialect.supportsColumnCheck()) {
                for (Check check : column.getChecks()) {
                    buffer.append(", CHECK ");
                    buffer.append(dialect.getCheckClause(check.getClause()));
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
        if (metaDataTypes.contains(PRIMARY_KEY)) {
            PrimaryKey primaryKey = table.getPrimaryKey();
            if (primaryKey != null) {
                ConstraintScriptGenerator<PrimaryKey> generator = (ConstraintScriptGenerator<PrimaryKey>)
                        scriptGeneratorContext.getScriptGenerator(primaryKey);
                buffer.append(", ").append(generator.getConstraintScript(primaryKey, scriptGeneratorContext));
            }
        }
        if (metaDataTypes.contains(INDEX) && dialect.supportsIndexInCreateTable()) {
            boolean primary = false;
            for (Index index : indexes) {
                if (!primary && index.isPrimary()) {
                    primary = true;
                    continue;
                }
                ConstraintScriptGenerator<Index> generator = (ConstraintScriptGenerator<Index>)
                        scriptGeneratorContext.getScriptGenerator(index);
                String constraint = generator.getConstraintScript(index, scriptGeneratorContext);
                if (constraint != null) {
                    buffer.append(", ").append(constraint);
                }

            }
        }
        if (metaDataTypes.contains(FOREIGN_KEY)) {
            for (ForeignKey foreignKey : table.getForeignKeys()) {
                ConstraintScriptGenerator<ForeignKey> generator = (ConstraintScriptGenerator<ForeignKey>)
                        scriptGeneratorContext.getScriptGenerator(foreignKey);
                String constraint = generator.getConstraintScript(foreignKey, scriptGeneratorContext);
                if (constraint != null) {
                    buffer.append(", ").append(constraint);
                }
            }
        }
        if (metaDataTypes.contains(CHECK) && dialect.supportsTableCheck()) {
            for (Check check : table.getChecks()) {
                buffer.append(", CHECK ");
                buffer.append(dialect.getCheckClause(check.getClause()));
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
    public Collection<String> getDropScripts(Table table, ScriptGeneratorContext scriptGeneratorContext) {
        Dialect dialect = scriptGeneratorContext.getDialect();
        StringBuilder buffer = new StringBuilder("DROP TABLE");
        buffer.append(' ');
        boolean ifExistsBeforeTable;
        if (ifExistsBeforeTable = dialect.supportsIfExistsBeforeDropTable()) {
            buffer.append("IF EXISTS");
            buffer.append(' ');
        }
        buffer.append(scriptGeneratorContext.getQualifiedName(table));
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

    protected String getTypeName(Column column, ScriptGeneratorContext context) {
        Dialect dialect = context.getDialect();
        int scale = column.getScale();
        if (scale < 0 && !dialect.supportsNegativeScale()) {
            scale = 0;
        }
        JdbcTypeSpecifiers typeSpecifiers = newSpecifiers(column.getSize(), column.getPrecision(), scale);
        String typeName = dialect.getJdbcTypeNameMap().getTypeName(
                new JdbcTypeDesc(column.getTypeCode(), column.getTypeName()), typeSpecifiers);
        if (typeName == null) {
            typeName = dialect.getJdbcTypeNameMap().getTypeName(
                    new JdbcTypeDesc(column.getTypeCode()), typeSpecifiers);
        }
        if (typeName == null) {
            throw new GeneratorException(
                    format("Unsupported type %s, type code %d, length %d on table %s column %s",
                            column.getTypeName(), column.getTypeCode(), column.getSize(),
                            context.getQualifiedName(column.getTable(), false),
                            context.getName(column, false)));
        }
        return typeName;
    }
}
