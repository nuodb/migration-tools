/**
 * Copyright (c) 2020, NuoDB, Inc.
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

import com.nuodb.migrator.cli.parse.CommandLine;
import com.nuodb.migrator.cli.parse.Option;
import com.nuodb.migrator.cli.parse.OptionException;
import com.nuodb.migrator.utils.ConfigUtils;

import java.io.Console;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.trimToNull;

public class PasswordOptionProcessor extends ConfigOptionProcessor {
    private String optionName;
    private String password;

    private String getPasswordFromPrompt(String prompt) {
        Console console = System.console();
        if (console == null) {
            return null;
        }
        System.out.print(prompt);
        return trimToNull(new String(console.readPassword()));
    }

    public PasswordOptionProcessor(String optionName) {
        this.optionName = optionName;
        password = null;
    }

    public String getPassword() {
        return password;
    }

    @Override
    public void postProcess(CommandLine commandLine, Option option) {
        password = (String)commandLine.getValue(optionName);
        if (isEmpty(password)) {
            // preProcess and process will *only* be called if the option is provided.
            // Only postProcess is called whether or not the option is provided.
            // We prompt if the option is provided but empty, or if it is not provided at all.
            // So we must prompt from postProcess.
            password = getPasswordFromPrompt("Enter " + optionName + ": ");
            if (isEmpty(password)) {
                throw new OptionException(
                    format("Missing required option %s. The user's password should be provided", optionName),
                    option);
            }
        }
    }
}
