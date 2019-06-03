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

import java.math.BigDecimal;
import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newLinkedHashSet;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.SEQUENCE;
import static java.lang.String.format;
import static java.math.BigDecimal.valueOf;
import static java.util.Collections.singleton;
import static org.apache.commons.lang3.StringUtils.join;

/**
 * @author Sergey Bushik
 */
public class Sequence extends IdentifiableBase {

    private Schema schema;
    private Collection<Column> columns = newLinkedHashSet();
    private BigDecimal startWith;
    private BigDecimal lastValue;
    private BigDecimal incrementBy;
    private BigDecimal minValue;
    private BigDecimal maxValue;
    private BigDecimal cache;
    private boolean cycle;
    private boolean order;
    private boolean temporary;

    public Sequence() {
        super(SEQUENCE, true);
    }

    public Sequence(String name) {
        super(SEQUENCE, name, true);
    }

    public Sequence(Identifier identifier) {
        super(SEQUENCE, identifier, true);
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public void addColumn(Column column) {
        columns.add(column);
    }

    public void removeColumn(Column column) {
        columns.remove(column);
    }

    public void setColumn(Column column) {
        setColumns(singleton(column));
    }

    public Collection<Column> getColumns() {
        return columns;
    }

    public void setColumns(Collection<Column> columns) {
        this.columns = columns;
    }

    public BigDecimal getStartWith() {
        return startWith;
    }

    public void setStartWith(long startWith) {
        setStartWith(valueOf(startWith));
    }

    public void setStartWith(BigDecimal startWith) {
        this.startWith = startWith;
    }

    public BigDecimal getLastValue() {
        return lastValue;
    }

    public void setLastValue(long lastValue) {
        setLastValue(valueOf(lastValue));
    }

    public void setLastValue(BigDecimal lastValue) {
        this.lastValue = lastValue;
    }

    public BigDecimal getIncrementBy() {
        return incrementBy;
    }

    public void setIncrementBy(long incrementBy) {
        setIncrementBy(valueOf(incrementBy));
    }

    public void setIncrementBy(BigDecimal incrementBy) {
        this.incrementBy = incrementBy;
    }

    public BigDecimal getMinValue() {
        return minValue;
    }

    public void setMinValue(long minValue) {
        setMinValue(valueOf(minValue));
    }

    public void setMinValue(BigDecimal minValue) {
        this.minValue = minValue;
    }

    public BigDecimal getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(long maxValue) {
        setMaxValue(valueOf(maxValue));
    }

    public void setMaxValue(BigDecimal maxValue) {
        this.maxValue = maxValue;
    }

    public boolean isCycle() {
        return cycle;
    }

    public void setCycle(boolean cycle) {
        this.cycle = cycle;
    }

    public boolean isOrder() {
        return order;
    }

    public void setOrder(boolean order) {
        this.order = order;
    }

    public boolean isTemporary() {
        return temporary;
    }

    public void setTemporary(boolean temporary) {
        this.temporary = temporary;
    }

    public BigDecimal getCache() {
        return cache;
    }

    public void setCache(long cache) {
        setCache(valueOf(cache));
    }

    public void setCache(BigDecimal cache) {
        this.cache = cache;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;

        Sequence sequence = (Sequence) o;

        if (cycle != sequence.cycle)
            return false;
        if (order != sequence.order)
            return false;
        if (temporary != sequence.temporary)
            return false;
        if (cache != null ? !cache.equals(sequence.cache) : sequence.cache != null)
            return false;
        if (lastValue != null ? !lastValue.equals(sequence.lastValue) : sequence.lastValue != null)
            return false;
        if (incrementBy != null ? !incrementBy.equals(sequence.incrementBy) : sequence.incrementBy != null)
            return false;
        if (maxValue != null ? !maxValue.equals(sequence.maxValue) : sequence.maxValue != null)
            return false;
        if (minValue != null ? !minValue.equals(sequence.minValue) : sequence.minValue != null)
            return false;
        if (startWith != null ? !startWith.equals(sequence.startWith) : sequence.startWith != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (startWith != null ? startWith.hashCode() : 0);
        result = 31 * result + (lastValue != null ? lastValue.hashCode() : 0);
        result = 31 * result + (incrementBy != null ? incrementBy.hashCode() : 0);
        result = 31 * result + (minValue != null ? minValue.hashCode() : 0);
        result = 31 * result + (maxValue != null ? maxValue.hashCode() : 0);
        result = 31 * result + (cycle ? 1 : 0);
        result = 31 * result + (order ? 1 : 0);
        result = 31 * result + (temporary ? 1 : 0);
        result = 31 * result + (cache != null ? cache.hashCode() : 0);
        return result;
    }

    @Override
    public void output(int indent, StringBuilder buffer) {
        super.output(indent, buffer);

        buffer.append(' ');
        Collection<String> attributes = newArrayList();
        if (startWith != null) {
            attributes.add(format("start value=%s", startWith));
        }
        if (lastValue != null) {
            attributes.add(format("last value=%s", lastValue));
        }
        if (incrementBy != null) {
            attributes.add(format("increment by=%s", incrementBy));
        }
        if (minValue != null) {
            attributes.add(format("min value=%s", minValue));
        }
        if (maxValue != null) {
            attributes.add(format("max value=%s", maxValue));
        }
        attributes.add(format("cycle=%b", cycle));
        attributes.add(format("order=%s", order ? "DESC" : "ASC"));
        attributes.add(format("temporary=%b", temporary));
        if (cache != null) {
            attributes.add(format("cache=%s", cache));
        }
        buffer.append(join(attributes, ", "));
    }
}
