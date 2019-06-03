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

import java.util.Collection;
import java.util.Collections;

import static com.nuodb.migrator.jdbc.metadata.MetaDataType.TRIGGER;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class Trigger extends ConstraintBase {

    public static final boolean ACTIVE = true;
    private boolean active = ACTIVE;
    private String triggerBody;
    private TriggerTime triggerTime;
    private TriggerEvent triggerEvent;

    public Trigger() {
        super(TRIGGER, true);
    }

    public Trigger(String name) {
        super(TRIGGER, name, true);
    }

    public Trigger(Identifier identifier) {
        super(TRIGGER, identifier, true);
    }

    protected Trigger(MetaDataType objectType, boolean qualified) {
        super(objectType, qualified);
    }

    protected Trigger(MetaDataType objectType, Identifier identifier, boolean qualified) {
        super(objectType, identifier, qualified);
    }

    protected Trigger(MetaDataType objectType, String name, boolean qualified) {
        super(objectType, name, qualified);
    }

    @Override
    public Collection<Column> getColumns() {
        return Collections.EMPTY_SET;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public String getTriggerBody() {
        return triggerBody;
    }

    public void setTriggerBody(String triggerBody) {
        this.triggerBody = triggerBody;
    }

    public TriggerTime getTriggerTime() {
        return triggerTime;
    }

    public void setTriggerTime(TriggerTime triggerTime) {
        this.triggerTime = triggerTime;
    }

    public TriggerEvent getTriggerEvent() {
        return triggerEvent;
    }

    public void setTriggerEvent(TriggerEvent triggerEvent) {
        this.triggerEvent = triggerEvent;
    }

    @Override
    public void output(int indent, StringBuilder buffer) {
        super.output(indent, buffer);
        TriggerTime triggerTime = getTriggerTime();
        if (triggerTime != null) {
            buffer.append(triggerTime);
            buffer.append(' ');
        }
        TriggerEvent triggerEvent = getTriggerEvent();
        if (triggerEvent != null) {
            buffer.append(triggerEvent);
        }
        String body = getTriggerBody();
        if (body != null) {
            buffer.append(' ');
            buffer.append(body);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;

        Trigger trigger = (Trigger) o;

        if (active != trigger.active)
            return false;
        if (triggerBody != null ? !triggerBody.equals(trigger.triggerBody) : trigger.triggerBody != null)
            return false;
        if (triggerEvent != trigger.triggerEvent)
            return false;
        if (triggerTime != null ? !triggerTime.equals(trigger.triggerTime) : trigger.triggerTime != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (active ? 1 : 0);
        result = 31 * result + (triggerBody != null ? triggerBody.hashCode() : 0);
        result = 31 * result + (triggerTime != null ? triggerTime.hashCode() : 0);
        result = 31 * result + (triggerEvent != null ? triggerEvent.hashCode() : 0);
        return result;
    }
}
