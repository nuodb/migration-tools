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
package com.nuodb.migrator.backup;

import com.nuodb.migrator.jdbc.metadata.Sequence;
import com.nuodb.migrator.utils.xml.XmlReadContext;
import com.nuodb.migrator.utils.xml.XmlWriteContext;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;

/**
 * @author Sergey Bushik
 */
public class XmlSequenceHandler extends XmlIdentifiableHandlerBase<Sequence> {

    private static final String START_WITH_ATTRIBUTE = "start-with";
    private static final String LAST_VALUE_ATTRIBUTE = "last-value";
    private static final String INCREMENT_BY_ATTRIBUTE = "increment-by";
    private static final String MIN_VALUE_ATTRIBUTE = "min-value";
    private static final String MAX_VALUE_ATTRIBUTE = "max-value";
    private static final String CYCLE_ATTRIBUTE = "cycle";
    private static final String ORDER_ATTRIBUTE = "order";
    private static final String TEMPORARY_ATTRIBUTE = "temporary";
    private static final String CACHE_ATTRIBUTE = "cache";

    public XmlSequenceHandler() {
        super(Sequence.class);
    }

    @Override
    protected void readAttributes(InputNode input, Sequence sequence, XmlReadContext context) throws Exception {
        sequence.setName(context.readAttribute(input, NAME_ATTRIBUTE, String.class));
        sequence.setStartWith(context.readAttribute(input, START_WITH_ATTRIBUTE, Long.class));
        sequence.setLastValue(context.readAttribute(input, LAST_VALUE_ATTRIBUTE, Long.class));
        sequence.setIncrementBy(context.readAttribute(input, INCREMENT_BY_ATTRIBUTE, Long.class));
        sequence.setMinValue(context.readAttribute(input, MIN_VALUE_ATTRIBUTE, Long.class));
        sequence.setMaxValue(context.readAttribute(input, MAX_VALUE_ATTRIBUTE, Long.class));
        sequence.setCycle(context.readAttribute(input, CYCLE_ATTRIBUTE, Boolean.class, false));
        sequence.setOrder(context.readAttribute(input, ORDER_ATTRIBUTE, Boolean.class, false));
        sequence.setTemporary(context.readAttribute(input, TEMPORARY_ATTRIBUTE, Boolean.class, false));
        sequence.setCache(context.readAttribute(input, CACHE_ATTRIBUTE, Integer.class));
    }

    @Override
    protected void writeAttributes(OutputNode output, Sequence sequence, XmlWriteContext context) throws Exception {
        if (sequence.getName() != null) {
            context.writeAttribute(output, NAME_ATTRIBUTE, sequence.getName());
        }
        Long startWith = sequence.getStartWith();
        if (startWith != null) {
            context.writeAttribute(output, START_WITH_ATTRIBUTE, startWith);
        }
        Long lastValue = sequence.getLastValue();
        if (lastValue != null) {
            context.writeAttribute(output, LAST_VALUE_ATTRIBUTE, lastValue);
        }
        Long incrementBy = sequence.getIncrementBy();
        if (incrementBy != null) {
            context.writeAttribute(output, INCREMENT_BY_ATTRIBUTE, lastValue);
        }
        Long minValue = sequence.getMinValue();
        if (minValue != null) {
            context.writeAttribute(output, MIN_VALUE_ATTRIBUTE, minValue);
        }
        Long maxValue = sequence.getMaxValue();
        if (maxValue != null) {
            context.writeAttribute(output, MAX_VALUE_ATTRIBUTE, maxValue);
        }
        boolean cycle = sequence.isCycle();
        if (cycle) {
            context.writeAttribute(output, CYCLE_ATTRIBUTE, cycle);
        }
        boolean order = sequence.isOrder();
        if (order) {
            context.writeAttribute(output, ORDER_ATTRIBUTE, order);
        }
        boolean temporary = sequence.isTemporary();
        if (temporary) {
            context.writeAttribute(output, TEMPORARY_ATTRIBUTE, temporary);
        }
        Integer cache = sequence.getCache();
        if (cache != null) {
            context.writeAttribute(output, CACHE_ATTRIBUTE, cache);
        }
    }
}
