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
package com.nuodb.migrator.cli.validation;

import com.nuodb.migrator.cli.parse.CommandLine;
import com.nuodb.migrator.cli.parse.OptionValidator;

/**
 * @author Sergey Bushik
 */
public abstract class ConnectionGroupValidator implements OptionValidator {

    private ConnectionGroupInfo connectionGroupInfo;

    public ConnectionGroupValidator(ConnectionGroupInfo connectionGroupInfo) {
        this.connectionGroupInfo = connectionGroupInfo;
    }

    public String getDriverValue(CommandLine commandLine) {
        return getOptionValue(commandLine, connectionGroupInfo.getDriverOption());
    }

    public String getUrlValue(CommandLine commandLine) {
        return getOptionValue(commandLine, connectionGroupInfo.getUrlOption());
    }

    public String getUsernameValue(CommandLine commandLine) {
        return getOptionValue(commandLine, connectionGroupInfo.getUsernameOption());
    }

    public String getPasswordValue(CommandLine commandLine) {
        return getOptionValue(commandLine, connectionGroupInfo.getPasswordOption());
    }

    public String getCatalogValue(CommandLine commandLine) {
        return getOptionValue(commandLine, connectionGroupInfo.getCatalogOption());
    }

    public String getSchemaValue(CommandLine commandLine) {
        return getOptionValue(commandLine, connectionGroupInfo.getSchemaOption());
    }

    public String getPropertiesValue(CommandLine commandLine) {
        return getOptionValue(commandLine, connectionGroupInfo.getPropertiesOption());
    }

    public String getOptionValue(CommandLine commandLine, String option) {
        return option != null ? (String) commandLine.getValue(option) : null;
    }

    public String getDriverOption() {
        return connectionGroupInfo.getDriverOption();
    }

    public String getUrlOption() {
        return connectionGroupInfo.getUrlOption();
    }

    public String getCatalogOption() {
        return connectionGroupInfo.getCatalogOption();
    }

    public String getSchemaOption() {
        return connectionGroupInfo.getSchemaOption();
    }

    public String getUsernameOption() {
        return connectionGroupInfo.getUsernameOption();
    }

    public String getPropertiesOption() {
        return connectionGroupInfo.getPropertiesOption();
    }

    public String getPasswordOption() {
        return connectionGroupInfo.getPasswordOption();
    }
}

