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
package com.nuodb.tools.migration.cli.parse.option;

/**
 * @author Sergey Bushik
 */

import com.nuodb.tools.migration.cli.parse.Argument;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Sergey Bushik
 */
public class ArgumentBuilderImpl implements ArgumentBuilder {

    private int id;
    private String name;
    private String description;
    private boolean required;
    private int minimum = 0;
    private int maximum = 1;
    private List<Object> defaultValues = new ArrayList<Object>();
    private String valuesSeparator;

    public ArgumentBuilderImpl(OptionFormat optionFormat) {
        this.valuesSeparator = optionFormat.getArgumentValuesSeparator();
    }

    @Override
    public ArgumentBuilder withId(int id) {
        this.id = id;
        return this;
    }

    @Override
    public ArgumentBuilder withName(String name) {
        this.name = name;
        return this;
    }

    @Override
    public ArgumentBuilder withDescription(String description) {
        this.description = description;
        return this;
    }

    @Override
    public ArgumentBuilder withRequired(boolean required) {
        this.required = required;
        return this;
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
    public ArgumentBuilder withDefaultValue(Object defaultValue) {
        this.defaultValues.add(defaultValue);
        return this;
    }

    @Override
    public ArgumentBuilder withValuesSeparator(String valuesSeparator) {
        this.valuesSeparator = valuesSeparator;
        return this;
    }

    @Override
    public Argument build() {
        ArgumentImpl argument = new ArgumentImpl();
        argument.setId(id);
        argument.setName(name);
        argument.setDescription(description);
        argument.setRequired(required);
        argument.setMinimum(minimum);
        argument.setMaximum(maximum);
        argument.setDefaultValues(defaultValues);
        argument.setValuesSeparator(valuesSeparator);
        return argument;
    }
}

