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
package com.nuodb.migrator.cli.validator;

import com.nuodb.migrator.cli.parse.CommandLine;
import com.nuodb.migrator.cli.parse.Option;
import com.nuodb.migrator.cli.parse.OptionValidator;

/**
 * @author Sergey Bushik
 */
public abstract class ConnectionGroupValidator implements OptionValidator {

    private ConnectionGroupInfo connectionGroupInfo;

    public ConnectionGroupValidator(ConnectionGroupInfo connectionGroupInfo) {
        this.connectionGroupInfo = connectionGroupInfo;
    }

    protected String getDriver(CommandLine commandLine) {
        return getValue(commandLine, connectionGroupInfo.getDriverOption());
    }

    protected Option getDriverOption(CommandLine commandLine) {
        return getOption(commandLine, connectionGroupInfo.getDriverOption());
    }

    protected String getUrl(CommandLine commandLine) {
        return getValue(commandLine, connectionGroupInfo.getUrlOption());
    }

    protected Option getUrlOption(CommandLine commandLine) {
        return getOption(commandLine, connectionGroupInfo.getUrlOption());
    }

    protected String getUsername(CommandLine commandLine) {
        return getValue(commandLine, connectionGroupInfo.getUsernameOption());
    }

    protected Option getUsernameOption(CommandLine commandLine) {
        return getOption(commandLine, connectionGroupInfo.getUsernameOption());
    }

    protected String getPassword(CommandLine commandLine) {
        return getValue(commandLine, connectionGroupInfo.getPasswordOption());
    }

    protected Option getPasswordOption(CommandLine commandLine) {
        return getOption(commandLine, connectionGroupInfo.getPasswordOption());
    }

    protected String getCatalog(CommandLine commandLine) {
        return getValue(commandLine, connectionGroupInfo.getCatalogOption());
    }

    protected Option getCatalogOption(CommandLine commandLine) {
        return getOption(commandLine, connectionGroupInfo.getCatalogOption());
    }

    protected String getSchema(CommandLine commandLine) {
        return getValue(commandLine, connectionGroupInfo.getSchemaOption());
    }

    protected Option getSchemaOption(CommandLine commandLine) {
        return getOption(commandLine, connectionGroupInfo.getSchemaOption());
    }

    protected String getProperties(CommandLine commandLine) {
        return getValue(commandLine, connectionGroupInfo.getPropertiesOption());
    }

    protected Option getPropertiesOption(CommandLine commandLine) {
        return getOption(commandLine, connectionGroupInfo.getPropertiesOption());
    }

    protected String getValue(CommandLine commandLine, String option) {
        return option != null ? (String) commandLine.getValue(option) : null;
    }

    protected Option getOption(CommandLine commandLine, String option) {
        return option != null ? commandLine.getOption(option) : null;
    }
}

