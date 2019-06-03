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

import com.nuodb.migrator.cli.parse.Argument;
import com.nuodb.migrator.cli.parse.AugmentOption;
import com.nuodb.migrator.cli.parse.Group;
import com.nuodb.migrator.cli.parse.OptionProcessor;
import com.nuodb.migrator.cli.parse.OptionValidator;
import com.nuodb.migrator.cli.parse.Trigger;

/**
 * @author Sergey Bushik
 */
public abstract class AugmentOptionBuilderBase<O extends AugmentOption> extends OptionBuilderBase<O>
        implements AugmentOptionBuilder<O> {

    private Argument argument;
    private Group group;

    public AugmentOptionBuilderBase(Class<? extends O> optionClass, OptionFormat optionFormat) {
        super(optionClass, optionFormat);
    }

    @Override
    public AugmentOptionBuilder withId(int id) {
        return (AugmentOptionBuilder) super.withId(id);
    }

    @Override
    public AugmentOptionBuilder withName(String name) {
        return (AugmentOptionBuilder) super.withName(name);
    }

    @Override
    public AugmentOptionBuilder withDescription(String description) {
        return (AugmentOptionBuilder) super.withDescription(description);
    }

    @Override
    public AugmentOptionBuilder withRequired(boolean required) {
        return (AugmentOptionBuilder) super.withRequired(required);
    }

    @Override
    public AugmentOptionBuilder withTrigger(Trigger trigger) {
        return (AugmentOptionBuilder) super.withTrigger(trigger);
    }

    @Override
    public AugmentOptionBuilder withOptionFormat(OptionFormat optionFormat) {
        return (AugmentOptionBuilder) super.withOptionFormat(optionFormat);
    }

    @Override
    public AugmentOptionBuilder withOptionProcessor(OptionProcessor optionProcessor) {
        return (AugmentOptionBuilder) super.withOptionProcessor(optionProcessor);
    }

    @Override
    public AugmentOptionBuilder withOptionValidator(OptionValidator optionValidator) {
        return (AugmentOptionBuilder) super.withOptionValidator(optionValidator);
    }

    @Override
    public AugmentOptionBuilder withGroup(Group group) {
        this.group = group;
        return this;
    }

    @Override
    public AugmentOptionBuilder withArgument(Argument argument) {
        this.argument = argument;
        return this;
    }

    @Override
    public O build() {
        O option = super.build();
        option.setArgument(argument);
        option.setGroup(group);
        return option;
    }
}
