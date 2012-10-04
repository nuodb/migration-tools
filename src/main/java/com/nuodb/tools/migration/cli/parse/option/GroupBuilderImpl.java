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

import com.nuodb.tools.migration.cli.parse.Group;
import com.nuodb.tools.migration.cli.parse.Option;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Sergey Bushik
 */
public class GroupBuilderImpl implements GroupBuilder {

    private int id;
    private String name;
    private String description;
    private int minimum = 0;
    private int maximum = 0;
    private boolean required;
    private List<Option> options = new ArrayList<Option>();

    @Override
    public GroupBuilder withId(int id) {
        this.id = id;
        return this;
    }

    @Override
    public GroupBuilder withName(String name) {
        this.name = name;
        return this;
    }

    @Override
    public GroupBuilder withDescription(String description) {
        this.description = description;
        return this;
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
    public GroupBuilder withRequired(boolean required) {
        this.required = required;
        return this;
    }

    @Override
    public Group build() {
        return new GroupImpl(id, name, description, required, minimum, maximum, options);
    }
}
