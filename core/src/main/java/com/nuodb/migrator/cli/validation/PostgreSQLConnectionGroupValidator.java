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
import org.apache.commons.lang3.StringUtils;

import static com.nuodb.migrator.jdbc.JdbcConstants.POSTGRESQL_DRIVER;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * @author Sergey Bushik
 */
public class PostgreSQLConnectionGroupValidator extends ConnectionGroupValidator {

    public PostgreSQLConnectionGroupValidator(ConnectionGroupInfo connectionGroupInfo) {
        super(connectionGroupInfo);
    }

    @Override
    public boolean canValidate(CommandLine commandLine, Option option) {
        return StringUtils.equals(getDriverValue(commandLine), POSTGRESQL_DRIVER);
    }

    @Override
    protected void validateCatalog(CommandLine commandLine, Option option, String catalog) {
        if (!isEmpty(catalog)) {
            throw new OptionException(
                    format("Unexpected option %s. PostgreSQL catalogs store meta data and built-in objects, "
                            + "use %s option to access user data", getCatalogOption(), getSchemaOption()),
                    option);
        }
    }

    @Override
    protected void dbUserWarnMessage(String jdbcUsername, String jdbcPassword, String optionUsername,
            String optionPasssword) {
        if (!StringUtils.equals(optionUsername, jdbcUsername) || !StringUtils.equals(optionPasssword, jdbcPassword)) {
            logger.warn(format(
                    "JDBC URL parameters user: %s passowrd: %s are not matching with commandline options --source.username %s --source.password %s.",
                    jdbcUsername, jdbcPassword, optionUsername, optionPasssword));
            logger.warn(format("JDBC URL parameters user: %s password: %s are used for database connection",
                    jdbcUsername, jdbcPassword));
        }
    }
}
