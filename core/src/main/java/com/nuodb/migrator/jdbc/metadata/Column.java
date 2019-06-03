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
package com.nuodb.migrator.jdbc.metadata;

import com.nuodb.migrator.jdbc.model.Field;
import com.nuodb.migrator.jdbc.type.JdbcType;
import com.nuodb.migrator.jdbc.type.JdbcTypeDesc;
import com.nuodb.migrator.jdbc.type.JdbcTypeOptions;
import com.nuodb.migrator.utils.ObjectUtils;

import java.util.Set;

import static com.google.common.collect.Sets.newLinkedHashSet;
import static com.nuodb.migrator.jdbc.metadata.Identifier.valueOf;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.COLUMN;
import static com.nuodb.migrator.jdbc.type.JdbcTypeOptions.newOptions;
import static java.lang.String.format;

@SuppressWarnings("unchecked")
public class Column extends IdentifiableBase implements Field {

    public static final long DEFAULT_SIZE = 0;
    public static final int DEFAULT_PRECISION = 38;
    public static final int DEFAULT_SCALE = 0;

    private Table table;

    private JdbcType jdbcType = new JdbcType(newOptions(DEFAULT_SIZE, DEFAULT_PRECISION, DEFAULT_SCALE));

    /**
     * Ordinal position of value in table, starting at 1.
     */
    private int position;
    /**
     * Contains value remarks, may be null.
     */
    private String comment;
    /**
     * Determines the nullability for a value.
     */
    private boolean nullable;
    /**
     * Indicates whether this value is auto incremented.
     */
    private boolean autoIncrement;
    /**
     * Associated identity sequence
     */
    private Sequence sequence;
    /**
     * Check constraints
     */
    private Set<Check> checks = newLinkedHashSet();
    /**
     * Column trigger
     */
    private Trigger trigger;

    private DefaultValue defaultValue;

    public Column() {
        super(COLUMN);
    }

    public Column(String name) {
        this(valueOf(name));
    }

    public Column(Identifier identifier) {
        super(COLUMN, identifier);
    }

    public static String getDefaultValue(Column column) {
        DefaultValue defaultValue = column.getDefaultValue();
        return defaultValue != null ? defaultValue.getScript() : null;
    }

    @Override
    public int getTypeCode() {
        return jdbcType.getTypeCode();
    }

    @Override
    public void setTypeCode(int typeCode) {
        jdbcType.setTypeCode(typeCode);
    }

    @Override
    public String getTypeName() {
        return jdbcType.getTypeName();
    }

    @Override
    public void setTypeName(String typeName) {
        jdbcType.setTypeName(typeName);
    }

    @Override
    public Long getSize() {
        return jdbcType.getSize();
    }

    @Override
    public void setSize(Long size) {
        jdbcType.setSize(size);
    }

    @Override
    public Integer getPrecision() {
        return jdbcType.getPrecision();
    }

    @Override
    public void setPrecision(Integer precision) {
        jdbcType.setPrecision(precision);
    }

    @Override
    public Integer getScale() {
        return jdbcType.getScale();
    }

    @Override
    public void setScale(Integer scale) {
        jdbcType.setScale(scale);
    }

    @Override
    public JdbcType getJdbcType() {
        return jdbcType;
    }

    @Override
    public void setJdbcType(JdbcType jdbcType) {
        this.jdbcType = jdbcType;
    }

    @Override
    public JdbcTypeDesc getJdbcTypeDesc() {
        return jdbcType.getJdbcTypeDesc();
    }

    @Override
    public void setJdbcTypeDesc(JdbcTypeDesc jdbcTypeDesc) {
        jdbcType.setJdbcTypeDesc(jdbcTypeDesc);
    }

    @Override
    public JdbcTypeOptions getJdbcTypeOptions() {
        return jdbcType.getJdbcTypeOptions();
    }

    @Override
    public void setJdbcTypeOptions(JdbcTypeOptions jdbcTypeOptions) {
        jdbcType.setJdbcTypeOptions(jdbcTypeOptions);
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        if (this.table != null) {
            this.table.getColumns().remove(this);
        }
        this.table = table;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }

    public void setAutoIncrement(boolean autoIncrement) {
        this.autoIncrement = autoIncrement;
    }

    public boolean isIdentity() {
        return isAutoIncrement();
    }

    public Sequence getSequence() {
        return sequence;
    }

    public void setSequence(Sequence sequence) {
        if (!ObjectUtils.equals(this.sequence, sequence)) {
            if (sequence != null) {
                sequence.addColumn(this);
            }
            this.sequence = sequence;
            this.autoIncrement = sequence != null;
        }
    }

    public DefaultValue getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(DefaultValue defaultValue) {
        this.defaultValue = defaultValue;
    }

    public Check addCheck(Check check) {
        check.setTable(table);
        check.getColumns().add(this);
        checks.add(check);
        return check;
    }

    public Set<Check> getChecks() {
        return checks;
    }

    public void setChecks(Set<Check> checks) {
        this.checks = checks;
    }

    public Trigger getTrigger() {
        return trigger;
    }

    public void setTrigger(Trigger trigger) {
        this.trigger = trigger;
    }

    @Override
    public Field toField() {
        return this;
    }

    @Override
    public void fromField(Field field) {
        setName(field.getName());
        setTypeName(field.getTypeName());
        setTypeCode(field.getTypeCode());
        setPrecision(field.getPrecision());
        setScale(field.getScale());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;

        Column column = (Column) o;

        if (table != null ? !table.equals(column.table) : column.table != null)
            return false;
        if (autoIncrement != column.autoIncrement)
            return false;
        if (nullable != column.nullable)
            return false;
        if (comment != null ? !comment.equals(column.comment) : column.comment != null)
            return false;
        if (jdbcType != null ? !jdbcType.equals(column.jdbcType) : column.jdbcType != null)
            return false;
        if (defaultValue != null ? !defaultValue.equals(column.defaultValue) : column.defaultValue != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (table != null ? table.hashCode() : 0);
        result = 31 * result + (jdbcType != null ? jdbcType.hashCode() : 0);
        result = 31 * result + (comment != null ? comment.hashCode() : 0);
        result = 31 * result + (nullable ? 1 : 0);
        result = 31 * result + (autoIncrement ? 1 : 0);
        result = 31 * result + (defaultValue != null ? defaultValue.hashCode() : 0);
        return result;
    }

    @Override
    public void output(int indent, StringBuilder buffer) {
        super.output(indent, buffer);

        buffer.append(' ');
        buffer.append(format("type=%s", getJdbcType()));
        if (!isNullable()) {
            buffer.append(", not null");
        }
        if (isAutoIncrement()) {
            Number lastValue = sequence != null ? sequence.getLastValue() : null;
            buffer.append(format(", auto increment=%s", lastValue != null ? lastValue : 0));
        }
        buffer.append(format(", position=%d", position));

        Trigger trigger = getTrigger();
        if (trigger != null) {
            buffer.append(format(", trigger=%s", trigger));
        }
    }
}
