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
package com.nuodb.migrator.cli.validation;

import com.nuodb.migrator.cli.parse.CommandLine;
import com.nuodb.migrator.cli.parse.Option;
import com.nuodb.migrator.cli.parse.OptionException;
import com.nuodb.migrator.cli.parse.OptionValidator;
import com.nuodb.migrator.jdbc.url.JdbcUrl;
import org.apache.commons.lang3.StringUtils;

import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;

import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.nuodb.migrator.cli.run.CliOptionValues.*;
import static java.lang.String.format;
import static java.sql.Connection.*;
import static com.nuodb.migrator.jdbc.url.JdbcUrlConstants.USER;
import static com.nuodb.migrator.jdbc.url.JdbcUrlConstants.PASSWORD;
import static com.nuodb.migrator.jdbc.url.JdbcUrlParsers.getInstance;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("all")
public abstract class ConnectionGroupValidator implements OptionValidator {

    protected transient final Logger logger = getLogger(getClass());

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
        JdbcUrl jdbcUrl = getInstance().parse(getUrlValue(commandLine));
        if (jdbcUrl == null)
            return;

        String jdbcUsername = (String) jdbcUrl.getParameters().get(USER);
        String jdbcPassword = (String) jdbcUrl.getParameters().get(PASSWORD);

        if (StringUtils.isBlank(jdbcUsername) && StringUtils.isBlank(jdbcPassword))
            return;

        String optionUsername = getUsernameValue(commandLine);
        String optionPasssword = getPasswordValue(commandLine);

        if (StringUtils.isBlank(optionUsername) && StringUtils.isBlank(optionPasssword))
            return;

        dbUserWarnMessage(jdbcUsername, jdbcPassword, optionUsername, optionPasssword);
    }

    protected void dbUserWarnMessage(String jdbcUsername, String jdbcPassword, String optionUsername,
            String optionPasssword) {
        if (!StringUtils.equals(optionUsername, jdbcUsername) || !StringUtils.equals(optionPasssword, jdbcPassword)) {
            logger.warn(format(
                    "JDBC URL parameters user: %s passowrd: %s are not matching with commandline options --source.username %s --source.password %s.",
                    jdbcUsername, jdbcPassword, optionUsername, optionPasssword));
            logger.warn(format(
                    "Commandline option values --source.username %s --source.password %s are used for database connection.",
                    optionUsername, optionPasssword));
        }
    }

    protected void validatePassword(CommandLine commandLine, Option option, String value) {
    }

    protected void validateTransactionIsolation(CommandLine commandLine, Option option, String value) {
        Integer transactionIsolation = INSTANCE.getTransactionIsolation(option, value);
        Map<String, Integer> transactionIsolations = getTransactionIsolations();
        if (transactionIsolation != null && transactionIsolations != null
                && !transactionIsolations.containsValue(transactionIsolation)) {
            StringBuilder result = new StringBuilder();
            for (Iterator<Map.Entry<String, Integer>> iterator = transactionIsolations.entrySet().iterator(); iterator
                    .hasNext();) {
                Map.Entry<String, Integer> entry = iterator.next();
                result.append(format("%s or %d", entry.getKey(), entry.getValue()));
                if (iterator.hasNext()) {
                    result.append(", ");
                }
            }
            throw new OptionException(format(
                    "Unexpected option value for %s. The database supports the following transaction isolations %s",
                    getTransactionIsolationOption(), result.toString()), option);
        }
    }

    /**
     * Returns collection of standard transaction isolations
     *
     * @return collection of standard transaction isolations
     */
    protected Map<String, Integer> getTransactionIsolations() {
        Map<String, Integer> transactionIsolations = newLinkedHashMap();
        transactionIsolations.put(TRANSACTION_ISOLATION_NONE, TRANSACTION_NONE);
        transactionIsolations.put(TRANSACTION_ISOLATION_READ_UNCOMMITTED, TRANSACTION_READ_UNCOMMITTED);
        transactionIsolations.put(TRANSACTION_ISOLATION_READ_COMMITTED, TRANSACTION_READ_COMMITTED);
        transactionIsolations.put(TRANSACTION_ISOLATION_REPEATABLE_READ, TRANSACTION_REPEATABLE_READ);
        transactionIsolations.put(TRANSACTION_ISOLATION_SERIALIZABLE, TRANSACTION_SERIALIZABLE);
        return transactionIsolations;
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
