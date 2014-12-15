/**
 * Copyright (c) 2014, NuoDB, Inc.
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

import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import com.nuodb.migrator.cli.parse.CommandLine;
import com.nuodb.migrator.cli.parse.Option;
import com.nuodb.migrator.cli.parse.OptionException;
import com.nuodb.migrator.cli.parse.OptionValidator;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import static com.nuodb.migrator.cli.run.CliOptionValues.INSTANCE;
import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("all")
public abstract class ConnectionGroupValidator implements OptionValidator {

    private ConnectionGroupInfo connectionGroupInfo;

    public ConnectionGroupValidator(ConnectionGroupInfo connectionGroupInfo) {
        this.connectionGroupInfo = connectionGroupInfo;
    }

    @Override
    public void validate(CommandLine commandLine, Option option) {
        validateCatalog(commandLine, option, getCatalogValue(commandLine));
        validateSchema(commandLine, option, getSchemaValue(commandLine));
        validateUsername(commandLine, option, getUsernameValue(commandLine));
        validatePassword(commandLine, option, getPasswordValue(commandLine));
        validateTransactionIsolation(commandLine, option, getTransactionIsolationValue(commandLine));
    }

    protected void validateCatalog(CommandLine commandLine, Option option, String value) {
    }

    protected void validateSchema(CommandLine commandLine, Option option, String value) {
    }

    protected void validateUsername(CommandLine commandLine, Option option, String value) {
    }

    protected void validatePassword(CommandLine commandLine, Option option, String value) {
    }

    protected void validateTransactionIsolation(CommandLine commandLine, Option option, String value) {
        Integer transactionIsolation = INSTANCE.getTransactionIsolation(option, value);
        Map<String, Integer> transactionIsolations = getTransactionIsolations();
        if (transactionIsolation != null && transactionIsolations != null &&
                !transactionIsolations.containsValue(transactionIsolation)) {
            StringBuilder result = new StringBuilder();
            for (Iterator<Map.Entry<String, Integer>> iterator = transactionIsolations.entrySet().iterator(); iterator
                    .hasNext(); ) {
                Map.Entry<String, Integer> entry = iterator.next();
                result.append(format("%s or %d", entry.getKey(), entry.getValue()));
                if (iterator.hasNext()) {
                    result.append(", ");
                }
            }
            throw new OptionException(
                    format("Unexpected option value %s. The database supports the following transaction isolations %s",
                            getTransactionIsolationOption(), result.toString()), option
            );
        }
    }

    /**
     * Returns collection of supported transaction isolations
     *
     * @return collection of supported transaction isolations
     */
    protected Map<String, Integer> getTransactionIsolations() {
        return null;
    }

    public String getDriverValue(CommandLine commandLine) {
        return getOptionValue(commandLine, getDriverOption());
    }

    public String getUrlValue(CommandLine commandLine) {
        return getOptionValue(commandLine, getUrlOption());
    }

    public String getUsernameValue(CommandLine commandLine) {
        return getOptionValue(commandLine, getUsernameOption());
    }

    public String getPasswordValue(CommandLine commandLine) {
        return getOptionValue(commandLine, getPasswordOption());
    }

    public String getCatalogValue(CommandLine commandLine) {
        return getOptionValue(commandLine, getCatalogOption());
    }

    public String getSchemaValue(CommandLine commandLine) {
        return getOptionValue(commandLine, getSchemaOption());
    }

    public String getPropertiesValue(CommandLine commandLine) {
        return getOptionValue(commandLine, getPropertiesOption());
    }

    public String getTransactionIsolationValue(CommandLine commandLine) {
        return getOptionValue(commandLine, getTransactionIsolationOption());
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

    public String getTransactionIsolationOption() {
        return connectionGroupInfo.getTransactionIsolationOption();
    }
}

