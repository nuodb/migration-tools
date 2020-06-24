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
import com.nuodb.migrator.jdbc.url.JdbcUrl;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.nuodb.migrator.cli.run.CliOptionValues.*;
import static com.nuodb.migrator.jdbc.JdbcConstants.NUODB_DRIVER;
import static com.nuodb.migrator.jdbc.url.JdbcUrlConstants.NUODB_SUB_PROTOCOL;
import static com.nuodb.migrator.jdbc.url.JdbcUrlParsers.getInstance;
import static java.lang.String.format;
import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.sql.Connection.TRANSACTION_SERIALIZABLE;
import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * @author Sergey Bushik
 */
public class NuoDBConnectionGroupValidator extends ConnectionGroupValidator {

    public NuoDBConnectionGroupValidator(ConnectionGroupInfo connectionGroupInfo) {
        super(connectionGroupInfo);
    }

    @Override
    public boolean canValidate(CommandLine commandLine, Option option) {
        boolean driver = StringUtils.equals(getDriverValue(commandLine), NUODB_DRIVER);
        JdbcUrl jdbcUrl = getInstance().parse(getUrlValue(commandLine));
        return driver || ((jdbcUrl != null) && StringUtils.equals(jdbcUrl.getSubProtocol(), NUODB_SUB_PROTOCOL));
    }

    @Override
    protected void validateCatalog(CommandLine commandLine, Option option, String catalog) {
        if (!isEmpty(catalog)) {
            throw new OptionException(
                    format("Unexpected option %s. NuoDB doesn't supports catalogs", getCatalogOption()), option);
        }
    }

    @Override
    protected void validateUsername(CommandLine commandLine, Option option, String username) {
        super.validateUsername(commandLine, option, username);
        if (isEmpty(username)) {
            throw new OptionException(
                    format("Missing required option %s. The user name to authenticate with should be provided",
                            getUsernameOption()),
                    option);
        }
    }

    @Override
    protected void validatePassword(CommandLine commandLine, Option option, String password) {
        // Validated in PasswordOptionProcessor
    }

    /**
     * Retrieves map of NuoDB transaction isolation levels, which consists of 2
     * standard levels read committed, serializable and 2 NuoDB specific write
     * committed and consistent read.
     *
     * @return map of NuoDB transaction isolations
     */
    @Override
    protected Map<String, Integer> getTransactionIsolations() {
        Map<String, Integer> transactionIsolations = newLinkedHashMap();
        transactionIsolations.put(TRANSACTION_ISOLATION_READ_COMMITTED, TRANSACTION_READ_COMMITTED);
        transactionIsolations.put(TRANSACTION_ISOLATION_SERIALIZABLE, TRANSACTION_SERIALIZABLE);
        transactionIsolations.put(TRANSACTION_ISOLATION_WRITE_COMMITTED, 5);
        transactionIsolations.put(TRANSACTION_ISOLATION_CONSISTENT_READ, 7);
        return transactionIsolations;
    }
}
