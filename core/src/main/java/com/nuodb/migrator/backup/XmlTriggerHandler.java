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

import com.nuodb.migrator.jdbc.metadata.Trigger;
import com.nuodb.migrator.jdbc.metadata.TriggerEvent;
import com.nuodb.migrator.jdbc.metadata.TriggerTime;
import com.nuodb.migrator.utils.xml.XmlReadContext;
import com.nuodb.migrator.utils.xml.XmlWriteContext;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;

import static com.nuodb.migrator.utils.StringUtils.lowerCase;
import static com.nuodb.migrator.utils.StringUtils.upperCase;

/**
 * @author Sergey Bushik
 */
public class XmlTriggerHandler<T extends Trigger> extends XmlIdentifiableHandlerBase<T> {

    private static final String ACTIVE_ATTRIBUTE = "active";
    private static final String TIME_ATTRIBUTE = "time";
    private static final String EVENT_ATTRIBUTE = "event";

    public XmlTriggerHandler() {
        super(Trigger.class);
    }

    public XmlTriggerHandler(Class<? extends T> type) {
        super(type);
    }

    @Override
    protected void readAttributes(InputNode input, T target, XmlReadContext context) throws Exception {
        super.readAttributes(input, target, context);
        target.setActive(context.readAttribute(input, ACTIVE_ATTRIBUTE, boolean.class));
        String triggerTime = context.readAttribute(input, TIME_ATTRIBUTE, String.class);
        if (triggerTime != null) {
            target.setTriggerTime(TriggerTime.valueOf(upperCase(triggerTime)));
        }
        String triggerEvent = context.readAttribute(input, EVENT_ATTRIBUTE, String.class);
        if (triggerEvent != null) {
            target.setTriggerEvent(TriggerEvent.valueOf(upperCase(triggerEvent)));
        }
    }

    @Override
    protected void writeAttributes(T trigger, OutputNode output, XmlWriteContext context) throws Exception {
        super.writeAttributes(trigger, output, context);
        context.writeAttribute(output, ACTIVE_ATTRIBUTE, trigger.isActive());
        TriggerTime triggerTime = trigger.getTriggerTime();
        if (triggerTime != null) {
            context.writeAttribute(output, TIME_ATTRIBUTE, lowerCase(triggerTime.toString()));
        }
        TriggerEvent triggerEvent = trigger.getTriggerEvent();
        if (triggerEvent != null) {
            context.writeAttribute(output, EVENT_ATTRIBUTE, lowerCase(triggerEvent.toString()));
        }
    }

    @Override
    protected void readElements(InputNode input, T trigger, XmlReadContext context) throws Exception {
        trigger.setTriggerBody(context.read(input, String.class));
    }

    @Override
    protected void writeElements(T trigger, OutputNode output, XmlWriteContext context) throws Exception {
        context.write(output, trigger.getTriggerBody());
    }
}
