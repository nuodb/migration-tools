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
package com.nuodb.migrator.jdbc.metadata.generator;

import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.metadata.Sequence;

import java.util.Collection;

import static java.util.Collections.singleton;

/**
 * @author Sergey Bushik
 */
public class SequenceScriptGenerator extends ScriptGeneratorBase<Sequence> {

    public SequenceScriptGenerator() {
        super(Sequence.class);
    }

    @Override
    public Collection<Script> getCreateScripts(Sequence sequence, ScriptGeneratorManager scriptGeneratorManager) {
        StringBuilder buffer = new StringBuilder();
        buffer.append("CREATE");
        buffer.append(' ');
        if (sequence.isTemporary()) {
            buffer.append("TEMPORARY");
            buffer.append(' ');
        }
        buffer.append("SEQUENCE");
        buffer.append(' ');
        buffer.append(scriptGeneratorManager.getName(sequence));
        Dialect dialect = scriptGeneratorManager.getTargetDialect();

        String currentValue = dialect.getSequenceStartWith(sequence.getLastValue());
        if (currentValue != null) {
            buffer.append(' ');
            buffer.append(currentValue);
        }

        String incrementBy = dialect.getSequenceIncrementBy(sequence.getIncrementBy());
        if (incrementBy != null) {
            buffer.append(' ');
            buffer.append(incrementBy);
        }

        String minValue = dialect.getSequenceMinValue(sequence.getMinValue());
        if (minValue != null) {
            buffer.append(' ');
            buffer.append(minValue);
        }

        String maxValue = dialect.getSequenceMaxValue(sequence.getMaxValue());
        if (maxValue != null) {
            buffer.append(' ');
            buffer.append(maxValue);
        }

        String cycle = dialect.getSequenceCycle(sequence.isCycle());
        if (cycle != null) {
            buffer.append(' ');
            buffer.append(cycle);
        }

        String cache = dialect.getSequenceCache(sequence.getCache());
        if (cache != null) {
            buffer.append(' ');
            buffer.append(cache);
        }

        String order = dialect.getSequenceOrder(sequence.isOrder());
        if (cache != null) {
            buffer.append(' ');
            buffer.append(order);
        }
        return singleton(new Script(buffer.toString()));
    }

    @Override
    public Collection<Script> getDropScripts(Sequence sequence, ScriptGeneratorManager scriptGeneratorManager) {
        Dialect dialect = scriptGeneratorManager.getTargetDialect();
        StringBuilder buffer = new StringBuilder();
        buffer.append("DROP SEQUENCE");
        if (dialect.supportsDropSequenceIfExists()) {
            buffer.append(' ');
            buffer.append("IF EXISTS");
        }
        buffer.append(' ');
        buffer.append(scriptGeneratorManager.getName(sequence));
        return singleton(new Script(buffer.toString()));
    }
}
