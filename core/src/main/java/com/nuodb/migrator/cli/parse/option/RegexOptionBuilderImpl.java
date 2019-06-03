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
import com.nuodb.migrator.cli.parse.Group;
import com.nuodb.migrator.cli.parse.OptionProcessor;
import com.nuodb.migrator.cli.parse.OptionValidator;
import com.nuodb.migrator.cli.parse.RegexOption;
import com.nuodb.migrator.cli.parse.Trigger;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

/**
 * @author Sergey Bushik
 */
public class RegexOptionBuilderImpl<O extends RegexOption> extends AugmentOptionBuilderBase<O>
        implements RegexOptionBuilder<O> {

    private List<RegexGroupPriority> regexGroupPriorityList = newArrayList();

    public RegexOptionBuilderImpl(OptionFormat optionFormat) {
        super((Class<? extends O>) RegexOptionImpl.class, optionFormat);
    }

    @Override
    public RegexOptionBuilder withId(int id) {
        return (RegexOptionBuilder) super.withId(id);
    }

    @Override
    public RegexOptionBuilder withName(String name) {
        return (RegexOptionBuilder) super.withName(name);
    }

    @Override
    public RegexOptionBuilder withDescription(String description) {
        return (RegexOptionBuilder) super.withDescription(description);
    }

    @Override
    public RegexOptionBuilder withRequired(boolean required) {
        return (RegexOptionBuilder) super.withRequired(required);
    }

    @Override
    public RegexOptionBuilder withTrigger(Trigger trigger) {
        return (RegexOptionBuilder) super.withTrigger(trigger);
    }

    @Override
    public RegexOptionBuilder withOptionFormat(OptionFormat optionFormat) {
        return (RegexOptionBuilder) super.withOptionFormat(optionFormat);
    }

    @Override
    public RegexOptionBuilder withOptionProcessor(OptionProcessor optionProcessor) {
        return (RegexOptionBuilder) super.withOptionProcessor(optionProcessor);
    }

    @Override
    public RegexOptionBuilder withOptionValidator(OptionValidator optionValidator) {
        return (RegexOptionBuilder) super.withOptionValidator(optionValidator);
    }

    @Override
    public RegexOptionBuilder withGroup(Group group) {
        return (RegexOptionBuilder) super.withGroup(group);
    }

    @Override
    public RegexOptionBuilder withArgument(Argument argument) {
        return (RegexOptionBuilder) super.withArgument(argument);
    }

    @Override
    public RegexOptionBuilder withRegex(String regex, int group, int priority) {
        regexGroupPriorityList.add(new RegexGroupPriority(regex, group, priority));
        return this;
    }

    @Override
    public O build() {
        O option = super.build();
        for (RegexGroupPriority regexGroupPriority : regexGroupPriorityList) {
            option.addRegex(regexGroupPriority.getRegex(), regexGroupPriority.getGroup(),
                    regexGroupPriority.getPriority());
        }
        return option;
    }

    class RegexGroupPriority {

        private String regex;
        private int group;
        private int priority;

        public RegexGroupPriority(String regex, int group, int priority) {
            this.regex = regex;
            this.group = group;
            this.priority = priority;
        }

        public String getRegex() {
            return regex;
        }

        public int getGroup() {
            return group;
        }

        public int getPriority() {
            return priority;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            RegexGroupPriority that = (RegexGroupPriority) o;

            if (group != that.group)
                return false;
            if (priority != that.priority)
                return false;
            if (regex != null ? !regex.equals(that.regex) : that.regex != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = regex != null ? regex.hashCode() : 0;
            result = 31 * result + group;
            result = 31 * result + priority;
            return result;
        }
    }
}
