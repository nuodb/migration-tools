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
package com.nuodb.tools.migration.cli;

/**
 * @author Sergey Bushik
 */
public interface CliResources {
    /**
     * Root options resources
     */
    final String MIGRATION_GROUP_NAME = "migration.cli.migration.group.name";
    final String HELP_OPTION_DESCRIPTION = "migration.cli.help.option.description";
    final String HELP_ARGUMENT_NAME = "migration.cli.help.argument.name";
    final String LIST_OPTION_DESCRIPTION = "migration.cli.list.option.description";
    final String CONFIG_OPTION_DESCRIPTION = "migration.cli.config.option.description";
    final String CONFIG_ARGUMENT_NAME = "migration.cli.config.argument.name";
    final String COMMAND_OPTION_DESCRIPTION = "migration.cli.command.option.description";

    /**
     * Dump executor resources
     */
    final String DUMP_GROUP_NAME = "migration.cli.dump.group.name";
    final String SOURCE_GROUP_NAME = "migration.cli.source.group.name";
    final String SOURCE_DRIVER_OPTION_DESCRIPTION = "migration.cli.source.driver.option.description";
    final String SOURCE_DRIVER_ARGUMENT_NAME = "migration.cli.source.driver.argument.name";
    final String SOURCE_URL_OPTION_DESCRIPTION = "migration.cli.source.url.option.description";
    final String SOURCE_URL_ARGUMENT_NAME = "migration.cli.source.url.argument.name";
    final String SOURCE_USERNAME_OPTION_DESCRIPTION = "migration.cli.source.username.option.description";
    final String SOURCE_USERNAME_ARGUMENT_NAME = "migration.cli.source.username.argument.name";
    final String SOURCE_PASSWORD_OPTION_DESCRIPTION = "migration.cli.source.password.option.description";
    final String SOURCE_PASSWORD_ARGUMENT_NAME = "migration.cli.source.password.argument.name";
    final String SOURCE_PROPERTIES_OPTION_DESCRIPTION = "migration.cli.source.properties.option.description";
    final String SOURCE_PROPERTIES_ARGUMENT_NAME = "migration.cli.source.properties.argument.name";
    final String SOURCE_CATALOG_OPTION_DESCRIPTION = "migration.cli.source.catalog.option.description";
    final String SOURCE_CATALOG_ARGUMENT_NAME = "migration.cli.source.catalog.argument.name";
    final String SOURCE_SCHEMA_OPTION_DESCRIPTION = "migration.cli.source.schema.option.description";
    final String SOURCE_SCHEMA_ARGUMENT_NAME = "migration.cli.source.schema.argument.name";
    final String OUTPUT_GROUP_NAME = "migration.cli.output.group";
    final String OUTPUT_TYPE_OPTION_DESCRIPTION = "migration.cli.output.type.option.description";
    final String OUTPUT_TYPE_ARGUMENT_NAME = "migration.cli.output.type.argument.name";
    final String OUTPUT_PATH_OPTION_DESCRIPTION = "migration.cli.output.path.option.description";
    final String OUTPUT_PATH_ARGUMENT_NAME = "migration.cli.output.path.argument.name";
}
