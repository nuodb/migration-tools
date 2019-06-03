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

import com.google.common.collect.Maps;
import com.nuodb.migrator.utils.StringUtils;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.INDEX;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isEmpty;

public class Index extends ConstraintBase {

    private boolean primary;
    private boolean unique;
    private boolean uniqueConstraint;
    private String filterCondition;
    private SortOrder sortOrder;
    private Map<Integer, Column> columns = Maps.newTreeMap();
    private String expression;
    private String type;
    public static final String BTREE = "BTREE";

    public Index() {
        super(INDEX);
    }

    public Index(String identifier) {
        super(INDEX, identifier);
    }

    public Index(Identifier identifier) {
        super(INDEX, identifier);
    }

    public boolean isUnique() {
        return unique;
    }

    public void setUnique(boolean unique) {
        this.unique = unique;
    }

    public boolean isUniqueConstraint() {
        return uniqueConstraint;
    }

    public void setUniqueConstraint(boolean uniqueConstraint) {
        this.uniqueConstraint = uniqueConstraint;
    }

    public void addColumn(Column column, int position) {
        columns.put(position, column);
    }

    public boolean isPrimary() {
        PrimaryKey primaryKey = getTable().getPrimaryKey();
        return primary || primaryKey != null && getColumns().equals(primaryKey.getColumns());
    }

    public void setPrimary(boolean primary) {
        this.primary = primary;
    }

    @Override
    public Collection<Column> getColumns() {
        return newArrayList(columns.values());
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public String getFilterCondition() {
        return filterCondition;
    }

    public void setFilterCondition(String filterCondition) {
        this.filterCondition = filterCondition;
    }

    public SortOrder getSortOrder() {
        return sortOrder;
    }

    public void setSortOrder(SortOrder sortOrder) {
        this.sortOrder = sortOrder;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean isBtree() {
        return StringUtils.equalsIgnoreCase(type, BTREE);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;

        Index index = (Index) o;

        if (unique != index.unique)
            return false;
        if (columns != null ? !columns.equals(index.columns) : index.columns != null)
            return false;
        if (filterCondition != null ? !filterCondition.equals(index.filterCondition) : index.filterCondition != null)
            return false;
        if (expression != null ? !expression.equals(index.expression) : index.expression != null)
            return false;
        if (sortOrder != index.sortOrder)
            return false;
        if (type != null ? !type.equals(index.type) : index.type != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (unique ? 1 : 0);
        result = 31 * result + (filterCondition != null ? filterCondition.hashCode() : 0);
        result = 31 * result + (expression != null ? expression.hashCode() : 0);
        result = 31 * result + (sortOrder != null ? sortOrder.hashCode() : 0);
        result = 31 * result + (columns != null ? columns.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        return result;
    }

    @Override
    public void output(int indent, StringBuilder buffer) {
        super.output(indent, buffer);

        buffer.append(' ');
        buffer.append(unique ? "unique" : "non-unique");

        if (!isEmpty(filterCondition)) {
            buffer.append(' ');
            buffer.append(format("filter %s", filterCondition));
        }

        buffer.append(' ');
        String expression = getExpression();
        if (expression != null) {
            buffer.append(expression);
        } else {
            Collection<Column> columns = getColumns();
            buffer.append('(');
            for (Iterator<Column> iterator = columns.iterator(); iterator.hasNext();) {
                Column column = iterator.next();
                buffer.append(column.getName());
                if (iterator.hasNext()) {
                    buffer.append(", ");
                }
            }
            buffer.append(')');
        }
    }
}
