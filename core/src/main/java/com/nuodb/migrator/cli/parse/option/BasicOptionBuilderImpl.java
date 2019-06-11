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
package com.nuodb.migrator.cli.parse.option;

import com.google.common.collect.Maps;
import com.nuodb.migrator.cli.parse.Argument;
import com.nuodb.migrator.cli.parse.BasicOption;
import com.nuodb.migrator.cli.parse.Group;
import com.nuodb.migrator.cli.parse.OptionProcessor;
import com.nuodb.migrator.cli.parse.OptionValidator;
import com.nuodb.migrator.cli.parse.Trigger;

import java.util.Map;

/**
 * @author Sergey Bushik
 */
public class BasicOptionBuilderImpl<O extends BasicOption> extends AugmentOptionBuilderBase<O>
        implements BasicOptionBuilder<O> {

    private Map<String, OptionFormat> aliases = Maps.newHashMap();

    public BasicOptionBuilderImpl(OptionFormat optionFormat) {
        super((Class<? extends O>) BasicOptionImpl.class, optionFormat);
    }

    @Override
    public BasicOptionBuilder withId(int id) {
        return (BasicOptionBuilder) super.withId(id);
    }

    @Override
    public BasicOptionBuilder withName(String name) {
        return (BasicOptionBuilder) super.withName(name);
    }

    @Override
    public BasicOptionBuilder withDescription(String description) {
        return (BasicOptionBuilder) super.withDescription(description);
    }

    @Override
    public BasicOptionBuilder withRequired(boolean required) {
        return (BasicOptionBuilder) super.withRequired(required);
    }

    @Override
    public BasicOptionBuilder withTrigger(Trigger trigger) {
        return (BasicOptionBuilder) super.withTrigger(trigger);
    }

    @Override
    public BasicOptionBuilder withOptionFormat(OptionFormat optionFormat) {
        return (BasicOptionBuilder) super.withOptionFormat(optionFormat);
    }

    @Override
    public BasicOptionBuilder withOptionProcessor(OptionProcessor optionProcessor) {
        return (BasicOptionBuilder) super.withOptionProcessor(optionProcessor);
    }

    @Override
    public BasicOptionBuilder withOptionValidator(OptionValidator optionValidator) {
        return (BasicOptionBuilder) super.withOptionValidator(optionValidator);
    }

    @Override
    public BasicOptionBuilder withGroup(Group group) {
        return (BasicOptionBuilder) super.withGroup(group);
    }

    @Override
    public BasicOptionBuilder withArgument(Argument argument) {
        return (BasicOptionBuilder) super.withArgument(argument);
    }

    @Override
    public BasicOptionBuilder withAlias(String alias) {
        return withAlias(alias, optionFormat);
    }

    @Override
    public BasicOptionBuilder withAlias(String alias, OptionFormat optionFormat) {
        this.aliases.put(alias, optionFormat);
        return this;
    }

    @Override
    public O build() {
        O option = super.build();
        option.setAliases(aliases);
        return option;
    }
}
