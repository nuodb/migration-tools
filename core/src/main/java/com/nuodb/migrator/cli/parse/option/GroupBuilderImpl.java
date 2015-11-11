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

import com.google.common.collect.Lists;
import com.nuodb.migrator.cli.parse.Group;
import com.nuodb.migrator.cli.parse.Option;
import com.nuodb.migrator.cli.parse.OptionProcessor;
import com.nuodb.migrator.cli.parse.OptionValidator;
import com.nuodb.migrator.cli.parse.Trigger;

import java.util.List;

/**
 * @author Sergey Bushik
 */
public class GroupBuilderImpl<O extends Group> extends OptionBuilderBase<O> implements GroupBuilder<O> {

    private int minimum = 0;
    private int maximum = 0;
    private List<Option> options = Lists.newArrayList();

    public GroupBuilderImpl(OptionFormat optionFormat) {
        super((Class<? extends O>) GroupImpl.class, optionFormat);
    }

    @Override
    public GroupBuilder withId(int id) {
        return (GroupBuilder) super.withId(id);
    }

    @Override
    public GroupBuilder withName(String name) {
        return (GroupBuilder) super.withName(name);
    }

    @Override
    public GroupBuilder withDescription(String description) {
        return (GroupBuilder) super.withDescription(description);
    }

    @Override
    public GroupBuilder withRequired(boolean required) {
        return (GroupBuilder) super.withRequired(required);
    }

    @Override
    public GroupBuilder withTrigger(Trigger trigger) {
        return (GroupBuilder) super.withTrigger(trigger);
    }

    @Override
    public GroupBuilder withOptionFormat(OptionFormat optionFormat) {
        return (GroupBuilder) super.withOptionFormat(optionFormat);
    }

    @Override
    public GroupBuilder withOptionProcessor(OptionProcessor optionProcessor) {
        return (GroupBuilder) super.withOptionProcessor(optionProcessor);
    }

    @Override
    public GroupBuilder withOptionValidator(OptionValidator optionValidator) {
        return (GroupBuilder) super.withOptionValidator(optionValidator);
    }

    @Override
    public GroupBuilder withMinimum(int minimum) {
        this.minimum = minimum;
        return this;
    }

    @Override
    public GroupBuilder withMaximum(int maximum) {
        this.maximum = maximum;
        return this;
    }

    @Override
    public GroupBuilder withOption(Option option) {
        this.options.add(option);
        return this;
    }

    @Override
    public O build() {
        O option = super.build();
        option.setMinimum(minimum);
        option.setMaximum(maximum);
        option.addOptions(options);
        return option;
    }
}
