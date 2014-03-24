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

import com.nuodb.migrator.cli.parse.CommandLine;
import com.nuodb.migrator.cli.parse.Option;
import com.nuodb.migrator.cli.parse.OptionException;
import com.nuodb.migrator.jdbc.url.JdbcUrl;
import org.apache.commons.lang3.StringUtils;

import static com.nuodb.migrator.jdbc.JdbcConstants.NUODB_DRIVER;
import static com.nuodb.migrator.jdbc.url.JdbcUrlConstants.NUODB_SUB_PROTOCOL;
import static com.nuodb.migrator.jdbc.url.JdbcUrlParsers.getInstance;
import static java.lang.String.format;
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
    public void validate(CommandLine commandLine, Option option) {
        String catalog = getCatalogValue(commandLine);
        if (!isEmpty(catalog)) {
            throw new OptionException(
                    format("Unexpected option %s. NuoDB doesn't supports catalogs", getCatalogOption()), option
            );
        }
        String username = getUsernameValue(commandLine);
        if (isEmpty(username)) {
            throw new OptionException(format("Missing required option %s. The user name to authenticate with should be provided",
                    getUsernameOption()), option
            );
        }
        String password = getPasswordValue(commandLine);
        if (isEmpty(password)) {
            throw new OptionException(format("Missing required option %s. The user's password should be provided",
                    getPasswordOption()), option
            );
        }
    }
}
