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

import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.metadata.Trigger;

import java.util.Collection;

import static java.util.Collections.singleton;

/**
 * @author Sergey Bushik
 */
public class TriggerScriptGenerator<H extends Trigger> extends ScriptGeneratorBase<H> {

    public TriggerScriptGenerator() {
        super(Trigger.class);
    }

    @Override
    protected Collection<String> getCreateScripts(H trigger, ScriptGeneratorContext scriptGeneratorContext) {
        StringBuilder buffer = new StringBuilder();
        buffer.append("CREATE TRIGGER ");
        buffer.append(scriptGeneratorContext.getQualifiedName(trigger));
        getCreateOn(trigger, scriptGeneratorContext, buffer);
        getCreateAttributes(trigger, scriptGeneratorContext, buffer);
        getCreateBody(trigger, scriptGeneratorContext, buffer);
        return singleton(buffer.toString());
    }

    protected void getCreateOn(H trigger, ScriptGeneratorContext scriptGeneratorContext, StringBuilder buffer) {
        Dialect dialect = scriptGeneratorContext.getTargetDialect();
        buffer.append(' ');
        buffer.append(dialect.getTriggerOn(trigger.getTable()));
        buffer.append(' ');
        buffer.append(scriptGeneratorContext.getQualifiedName(trigger.getTable()));
    }

    protected void getCreateAttributes(H trigger, ScriptGeneratorContext context, StringBuilder buffer) {
        Dialect dialect = context.getTargetDialect();
        String active = dialect.getTriggerActive(trigger.isActive());
        if (active != null) {
            buffer.append(' ');
            buffer.append(active);
        }
        buffer.append(' ');
        buffer.append(dialect.getTriggerTime(trigger.getTriggerTime()));
        buffer.append(' ');
        buffer.append(dialect.getTriggerEvent(trigger.getTriggerEvent()));
    }

    protected void getCreateBody(H trigger, ScriptGeneratorContext scriptGeneratorContext, StringBuilder buffer) {
        buffer.append(' ');
        Dialect dialect = scriptGeneratorContext.getTargetDialect();
        buffer.append(dialect.getTriggerBegin(trigger));
        buffer.append(' ');
        buffer.append(dialect.getTriggerBody(trigger, scriptGeneratorContext.getSourceSession()));
        buffer.append(' ');
        buffer.append(dialect.getTriggerEnd(trigger));
    }

    @Override
    protected Collection<String> getDropScripts(H trigger, ScriptGeneratorContext scriptGeneratorContext) {
        Dialect dialect = scriptGeneratorContext.getTargetDialect();
        StringBuilder buffer = new StringBuilder();
        buffer.append("DROP TRIGGER ");
        buffer.append(scriptGeneratorContext.getQualifiedName(trigger));
        if (dialect.supportsDropTriggerIfExists()) {
            buffer.append(' ');
            buffer.append("IF EXISTS");
        }
        return singleton(buffer.toString());
    }
}
