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

import static java.util.Collections.singleton;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.Collection;

import org.slf4j.Logger;

import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Trigger;

/**
 * @author Jayesh J
 */
public class TriggerScriptGenerator extends ScriptGeneratorBase<Trigger> {

    protected final Logger logger = getLogger(getClass());
    
    public TriggerScriptGenerator() {
        super(Trigger.class);
    }


    @Override
    public Collection<String> getCreateScripts(Trigger trigger,
            ScriptGeneratorContext scriptGeneratorContext) {
        System.out.println("Starting trigger generation");
        if (logger.isDebugEnabled()) {
            logger.debug("Generating Trigger");
        }
        StringBuilder buffer = new StringBuilder();
        buffer.append("CREATE TRIGGER TR_");
        buffer.append(trigger.getName());
        buffer.append(" ACTIVE BEFORE UPDATE ");
        buffer.append(" AS ");
        for (Column column : trigger.getColumns()) {
            buffer.append("NEW." + column.getName() + "= 'NOW';");
        }
        buffer.append("END TRIGGER");
        System.out.println(">>>>:" + buffer.toString());
       
        return singleton(buffer.toString());
    }

    @Override
    public Collection<String> getDropScripts(Trigger trigger,
            ScriptGeneratorContext scriptGeneratorContext) {
        StringBuilder buffer = new StringBuilder();
        buffer.append("DROP TRIGGER ");
        buffer.append(scriptGeneratorContext.getName(trigger));
        buffer.append(";");
        return singleton(buffer.toString());
    }
}
