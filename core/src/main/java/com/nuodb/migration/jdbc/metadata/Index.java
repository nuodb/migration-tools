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

import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.Map;

import static java.lang.String.format;

public class Index extends ConstraintBase {

    private boolean unique;
    private String filterCondition;
    private IndexSortOrder sortOrder;
    private Map<Integer, Column> columns = Maps.newTreeMap();

    public Index(Identifier identifier) {
        super(identifier);
    }

    public boolean isUnique() {
        return unique;
    }

    public void setUnique(boolean unique) {
        this.unique = unique;
    }

    public void addColumn(Column column, int position) {
        columns.put(position, column);
    }

    @Override
    public Collection<Column> getColumns() {
        return columns.values();
    }

    public String getFilterCondition() {
        return filterCondition;
    }

    public void setFilterCondition(String filterCondition) {
        this.filterCondition = filterCondition;
    }

    public IndexSortOrder getSortOrder() {
        return sortOrder;
    }

    public void setSortOrder(IndexSortOrder sortOrder) {
        this.sortOrder = sortOrder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        Index index = (Index) o;

        if (unique != index.unique) return false;
        if (columns != null ? !columns.equals(index.columns) : index.columns != null) return false;
        if (filterCondition != null ? !filterCondition.equals(index.filterCondition) : index.filterCondition != null)
            return false;
        if (sortOrder != index.sortOrder) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (unique ? 1 : 0);
        result = 31 * result + (filterCondition != null ? filterCondition.hashCode() : 0);
        result = 31 * result + (sortOrder != null ? sortOrder.hashCode() : 0);
        result = 31 * result + (columns != null ? columns.hashCode() : 0);
        return result;
    }

    @Override
    public void output(int indent, StringBuilder buffer) {
        super.output(indent, buffer);
        buffer.append(' ');
        buffer.append(unique ? "unique" : "non-unique");
        if (filterCondition != null) {
            buffer.append(' ');
            buffer.append(format("filter %s", filterCondition));
        }
        Collection<Column> columns = getColumns();
        buffer.append(' ');
        buffer.append(format("%d column(s)", columns.size()));
        if (!columns.isEmpty()) {
            buffer.append(' ');
            output(indent, buffer, columns);
        }
    }
}
