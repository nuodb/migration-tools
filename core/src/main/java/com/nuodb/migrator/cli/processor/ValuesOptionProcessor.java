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
package com.nuodb.migrator.cli.processor;

import com.google.common.base.Supplier;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.nuodb.migrator.cli.parse.CommandLine;
import com.nuodb.migrator.cli.parse.OptionProcessor;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Multimaps.newListMultimap;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Remembers values for the option between invocations and returns set of delta
 * values
 *
 * @author Sergey Bushik
 */
public abstract class ValuesOptionProcessor implements OptionProcessor {

    protected transient final Logger logger = getLogger(getClass());

    private ListMultimap<String, Object> lastOptionValues = newListMultimap(
            Maps.<String, Collection<Object>>newLinkedHashMap(), new Supplier<List<Object>>() {
                @Override
                public List<Object> get() {
                    return newArrayList();
                }
            });

    protected Collection<String> getValues(CommandLine commandLine, String trigger) {
        List<String> optionValues = newArrayList(commandLine.<String>getValues(trigger));
        List<Object> values = lastOptionValues.replaceValues(trigger, optionValues);
        return optionValues.subList(values.size(), optionValues.size());
    }
}
