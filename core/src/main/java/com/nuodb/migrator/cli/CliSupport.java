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
package com.nuodb.migrator.cli;

import com.nuodb.migrator.cli.parse.option.ArgumentBuilder;
import com.nuodb.migrator.cli.parse.option.BasicOptionBuilder;
import com.nuodb.migrator.cli.parse.option.GroupBuilder;
import com.nuodb.migrator.cli.parse.option.OptionFormat;
import com.nuodb.migrator.cli.parse.option.OptionToolkit;
import com.nuodb.migrator.cli.parse.option.RegexOptionBuilder;

/**
 * @author Sergey Bushik
 */
public class CliSupport implements CliMessages, CliOptions {

    private OptionToolkit optionToolkit;

    public CliSupport() {
        this(OptionToolkit.getInstance());
    }

    public CliSupport(OptionToolkit optionToolkit) {
        this.optionToolkit = optionToolkit;
    }

    public ArgumentBuilder newArgumentBuilder() {
        return getOptionToolkit().newArgumentBuilder();
    }

    public BasicOptionBuilder newBasicOptionBuilder() {
        return getOptionToolkit().newBasicOptionBuilder();
    }

    public GroupBuilder newGroupBuilder() {
        return getOptionToolkit().newGroupBuilder();
    }

    public RegexOptionBuilder newRegexOptionBuilder() {
        return getOptionToolkit().newRegexOptionBuilder();
    }

    public OptionFormat getOptionFormat() {
        return getOptionToolkit().getOptionFormat();
    }

    public OptionToolkit getOptionToolkit() {
        return optionToolkit;
    }
}
