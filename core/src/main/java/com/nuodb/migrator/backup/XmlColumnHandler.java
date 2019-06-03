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
package com.nuodb.migrator.backup;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.nuodb.migrator.jdbc.metadata.Check;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.ColumnTrigger;
import com.nuodb.migrator.jdbc.metadata.DefaultValue;
import com.nuodb.migrator.jdbc.metadata.Schema;
import com.nuodb.migrator.jdbc.metadata.Sequence;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.type.JdbcEnumType;
import com.nuodb.migrator.jdbc.type.JdbcSetType;
import com.nuodb.migrator.jdbc.type.JdbcType;
import com.nuodb.migrator.utils.xml.XmlReadContext;
import com.nuodb.migrator.utils.xml.XmlWriteContext;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.indexOf;
import static com.nuodb.migrator.jdbc.metadata.DefaultValue.valueOf;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.SEQUENCE;
import static com.nuodb.migrator.utils.Predicates.is;
import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
public class XmlColumnHandler extends XmlIdentifiableHandlerBase<Column> {

    private static final String NULLABLE_ATTRIBUTE = "nullable";
    private static final String AUTO_INCREMENT_ATTRIBUTE = "auto-increment";
    private static final String COMMENT_ELEMENT = "comment";
    private static final String TYPE_ELEMENT = "type";
    private static final String SET_ELEMENT = "set";
    private static final String ENUM_ELEMENT = "enum";
    private static final String DEFAULT_VALUE_ATTRIBUTE = "default-value";
    private static final String TRIGGER_ELEMENT = "trigger";
    private static final String CHECK_ELEMENT = "check";
    private static final String SEQUENCE_ELEMENT = "sequence";
    private static final String REF_INDEX_ATTRIBUTE = "ref-index";

    public XmlColumnHandler() {
        super(Column.class);
    }

    @Override
    protected Column createTarget(InputNode input, Class<? extends Column> type, XmlReadContext context) {
        Table table = getParent(context, 0);
        String name = context.readAttribute(input, NAME_ATTRIBUTE, String.class);
        return table != null ? (table.hasColumn(name) ? table.getColumn(name) : table.addColumn(name))
                : new Column(name);
    }

    @Override
    protected void readAttributes(InputNode input, Column column, XmlReadContext context) throws Exception {
        column.setNullable(context.readAttribute(input, NULLABLE_ATTRIBUTE, Boolean.class, false));
        column.setAutoIncrement(context.readAttribute(input, AUTO_INCREMENT_ATTRIBUTE, Boolean.class, false));
        column.setDefaultValue(valueOf(context.readAttribute(input, DEFAULT_VALUE_ATTRIBUTE, String.class)));
    }

    @Override
    protected void readElement(InputNode input, Column column, XmlReadContext context) throws Exception {
        final String element = input.getName();
        if (COMMENT_ELEMENT.equals(element)) {
            column.setComment(context.read(input, String.class));
        } else if (TYPE_ELEMENT.equals(element)) {
            column.setJdbcType(context.read(input, JdbcType.class));
        } else if (ENUM_ELEMENT.equals(element)) {
            column.setJdbcType(context.read(input, JdbcEnumType.class));
        } else if (SET_ELEMENT.equals(element)) {
            column.setJdbcType(context.read(input, JdbcSetType.class));
        } else if (TRIGGER_ELEMENT.equals(element)) {
            ColumnTrigger columnTrigger = context.read(input, ColumnTrigger.class);
            columnTrigger.setColumn(column);
            column.getTable().addTrigger(columnTrigger);
        } else if (CHECK_ELEMENT.equals(element)) {
            column.addCheck(context.read(input, Check.class));
        } else if (SEQUENCE_ELEMENT.equals(element)) {
            Integer index = context.readAttribute(input, REF_INDEX_ATTRIBUTE, Integer.class);
            Sequence sequence = index != null ? Iterables.get(((Schema) getParent(context, 2)).getSequences(), index)
                    : context.read(input, Sequence.class);
            column.setSequence(sequence);
        }
    }

    @Override
    protected void writeAttributes(Column column, OutputNode output, XmlWriteContext context) throws Exception {
        super.writeAttributes(column, output, context);
        boolean nullable = column.isNullable();
        if (nullable) {
            context.writeAttribute(output, NULLABLE_ATTRIBUTE, nullable);
        }
        boolean autoIncrement = column.isAutoIncrement();
        if (autoIncrement) {
            if (!skip(SEQUENCE, context)) {
                context.writeAttribute(output, AUTO_INCREMENT_ATTRIBUTE, autoIncrement);
            } else if (logger.isWarnEnabled()) {
                logger.warn(format("Attribute %s on table %s column %s is ignored, since sequences are disabled",
                        AUTO_INCREMENT_ATTRIBUTE, column.getTable().getQualifiedName(), column.getName()));
            }
        }
        DefaultValue defaultValue = column.getDefaultValue();
        if (defaultValue != null && defaultValue.getScript() != null) {
            context.writeAttribute(output, DEFAULT_VALUE_ATTRIBUTE, defaultValue.getScript());
        }
    }

    @Override
    protected void writeElements(Column column, OutputNode output, final XmlWriteContext context) throws Exception {
        JdbcType jdbcType = column.getJdbcType();
        Class<? extends JdbcType> typeClass = jdbcType.getClass();
        if (typeClass.equals(JdbcEnumType.class)) {
            context.writeElement(output, ENUM_ELEMENT, jdbcType);
        } else if (typeClass.equals(JdbcSetType.class)) {
            context.writeElement(output, SET_ELEMENT, jdbcType);
        } else {
            context.writeElement(output, TYPE_ELEMENT, jdbcType);
        }
        if (column.getComment() != null) {
            context.writeElement(output, COMMENT_ELEMENT, column.getComment());
        }
        if (column.getTrigger() != null) {
            context.writeElement(output, TRIGGER_ELEMENT, column.getTrigger());
        }
        for (Check check : column.getChecks()) {
            context.writeElement(output, CHECK_ELEMENT, check);
        }
        final Sequence sequence = column.getSequence();
        if (sequence != null && sequence.getSchema() != null && !skip(SEQUENCE, context)) {
            OutputNode element = output.getChild(SEQUENCE_ELEMENT);
            context.writeAttribute(element, REF_INDEX_ATTRIBUTE,
                    indexOf(filter(sequence.getSchema().getSequences(), new Predicate<Sequence>() {
                        @Override
                        public boolean apply(Sequence sequence) {
                            return !context.skip(null, sequence);
                        }
                    }), is(sequence)));
        }
    }
}
