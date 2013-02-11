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
package com.nuodb.migration.jdbc.metadata;

import com.google.common.collect.Sets;
import com.nuodb.migration.jdbc.model.ValueModel;

import java.util.Set;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.upperCase;

public class Column extends IdentifiableBase implements ValueModel {

    /**
     * Default precision is maximum value
     */
    public static final int DEFAULT_PRECISION = 38;
    public static final int DEFAULT_SCALE = 0;
    public static final int DEFAULT_RADIX = 10;

    private Table table;
    /**
     * SQL type from java.sql.Types
     */
    private int typeCode;
    /**
     * Data source dependent type name
     */
    private String typeName;
    /**
     * Holds column size.
     */
    private int size;
    /**
     * The maximum total number of decimal digits that can be stored, both to the left and to the right of the decimal
     * point. The precision is in the range of 1 through the maximum precision of 38.
     */
    private int precision = DEFAULT_PRECISION;
    /**
     * The number of fractional digits for numeric data types.
     */
    private int scale = DEFAULT_SCALE;
    /**
     * Contains column remarks, may be null.
     */
    private String comment;
    /**
     * Radix for numbers, typically 2 or 10.
     */
    private int radix = DEFAULT_RADIX;
    /**
     * Ordinal position of column in table, starting at 1.
     */
    private int position;
    /**
     * Determines the nullability for a column.
     */
    private boolean nullable;
    /**
     * Indicates whether this column is auto incremented.
     */
    private boolean autoIncrement;
    /**
     * Associated auto increment sequence
     */
    private Sequence sequence;
    /**
     * Check constraint.
     */
    private Set<Check> checks = Sets.newLinkedHashSet();

    private String defaultValue;

    public Column(Table table, String name) {
        this(table, Identifier.valueOf(name));
    }

    public Column(Table table, Identifier identifier) {
        super(MetaDataType.COLUMN, identifier);
        this.table = table;
    }

    @Override
    public int getTypeCode() {
        return typeCode;
    }

    @Override
    public void setTypeCode(int typeCode) {
        this.typeCode = typeCode;
    }

    @Override
    public String getTypeName() {
        return typeName;
    }

    @Override
    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    @Override
    public int getPrecision() {
        return precision;
    }

    @Override
    public void setPrecision(int precision) {
        this.precision = precision;
    }

    @Override
    public int getScale() {
        return scale;
    }

    @Override
    public void setScale(int scale) {
        this.scale = scale;
    }

    public Table getTable() {
        return table;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public int getRadix() {
        return radix;
    }

    public void setRadix(int radix) {
        this.radix = radix;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
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

    public Sequence getSequence() {
        return sequence;
    }

    public void setSequence(Sequence sequence) {
        if (sequence != null) {
            sequence.setColumn(this);
        }
        this.sequence = sequence;
    }

    public boolean isIdentity() {
        return isAutoIncrement();
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public void addCheck(Check check) {
        check.setTable(table);
        check.getColumns().add(this);
        checks.add(check);
    }

    public Set<Check> getChecks() {
        return checks;
    }

    public void setChecks(Set<Check> checks) {
        this.checks = checks;
    }

    @Override
    public ValueModel asValueModel() {
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        Column column = (Column) o;
        if (table != null ? !table.equals(column.table) : column.table != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (table != null ? table.hashCode() : 0);
        return result;
    }

    @Override
    public void output(int indent, StringBuilder buffer) {
        super.output(indent, buffer);

        buffer.append(' ');
        buffer.append(format("type=%d, %s", getTypeCode(), upperCase(getTypeName())));
        if (!isNullable()) {
            buffer.append(", not null");
        }
        if (isAutoIncrement()) {
            Long lastValue = sequence != null ? sequence.getLastValue() : null;
            buffer.append(format(", auto increment=%d", lastValue != null ? lastValue : 0));
        }
        buffer.append(format(", size=%d", size));
        buffer.append(format(", precision=%d", precision));
        buffer.append(format(", scale=%d", scale));
    }
}

