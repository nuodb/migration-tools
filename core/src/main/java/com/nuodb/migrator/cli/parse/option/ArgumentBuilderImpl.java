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
import com.nuodb.migrator.cli.parse.Argument;
import com.nuodb.migrator.cli.parse.OptionProcessor;
import com.nuodb.migrator.cli.parse.OptionValidator;
import com.nuodb.migrator.cli.parse.Trigger;

import java.util.Collection;
import java.util.List;

import static java.lang.Integer.MAX_VALUE;

/**
 * Default argument builder implementation.
 *
 * @author Sergey Bushik
 */
public class ArgumentBuilderImpl<O extends Argument> extends OptionBuilderBase<O> implements ArgumentBuilder<O> {

    private int minimum = 0;
    private int maximum = 1;
    private int minimumValue = 0;
    private int maximumValue = MAX_VALUE;

    private List<Object> defaultValues = Lists.newArrayList();
    private Collection<String> helpValues;

    public ArgumentBuilderImpl(OptionFormat optionFormat) {
        super((Class<? extends O>) ArgumentImpl.class, optionFormat);
    }

    @Override
    public ArgumentBuilder withId(int id) {
        return (ArgumentBuilder) super.withId(id);
    }

    @Override
    public ArgumentBuilder withName(String name) {
        return (ArgumentBuilder) super.withName(name);
    }

    @Override
    public ArgumentBuilder withDescription(String description) {
        return (ArgumentBuilder) super.withDescription(description);
    }

    @Override
    public ArgumentBuilder withRequired(boolean required) {
        return (ArgumentBuilder) super.withRequired(required);
    }

    @Override
    public ArgumentBuilder withTrigger(Trigger trigger) {
        return (ArgumentBuilder) super.withTrigger(trigger);
    }

    @Override
    public ArgumentBuilder withOptionFormat(OptionFormat optionFormat) {
        return (ArgumentBuilder) super.withOptionFormat(optionFormat);
    }

    @Override
    public ArgumentBuilder withOptionProcessor(OptionProcessor optionProcessor) {
        return (ArgumentBuilder) super.withOptionProcessor(optionProcessor);
    }

    @Override
    public ArgumentBuilder withOptionValidator(OptionValidator optionValidator) {
        return (ArgumentBuilder) super.withOptionValidator(optionValidator);
    }

    @Override
    public ArgumentBuilder withMinimum(int minimum) {
        this.minimum = minimum;
        return this;
    }

    @Override
    public ArgumentBuilder withMaximum(int maximum) {
        this.maximum = maximum;
        return this;
    }

    @Override
    public ArgumentBuilder withValueMinimum(int minimum) {
        this.minimumValue = minimum;
        return this;
    }

    @Override
    public ArgumentBuilder withValueMaximum(int maximum) {
        this.maximumValue = maximum;
        return this;
    }

    @Override
    public ArgumentBuilder withDefaultValue(Object defaultValue) {
        this.defaultValues.add(defaultValue);
        return this;
    }

    @Override
    public ArgumentBuilder withHelpValues(Collection<String> helpValues) {
        this.helpValues = helpValues;
        return this;
    }

    @Override
    public O build() {
        O option = super.build();
        option.setMinimum(minimum);
        option.setMaximum(maximum);
        option.setMinimumValue(minimumValue);
        option.setMaximumValue(maximumValue);
        option.setDefaultValues(defaultValues);
        option.setHelpValues(helpValues);
        return option;
    }
}
