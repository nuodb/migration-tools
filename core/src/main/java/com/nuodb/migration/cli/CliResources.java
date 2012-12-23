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
package com.nuodb.migration.cli;

/**
 * @author Sergey Bushik
 */
public interface CliResources {

    /**
     * Root options resources
     */
    final String ROOT_GROUP_NAME = "com.nuodb.migration.root.group.name";

    final String HELP_OPTION_DESCRIPTION = "com.nuodb.migration.help.option.description";
    final String HELP_ARGUMENT_NAME = "com.nuodb.migration.help.argument.name";
    final String LIST_OPTION_DESCRIPTION = "com.nuodb.migration.list.option.description";
    final String LIST_OPTION_OUTPUT = "com.nuodb.migration.list.option.output";
    final String CONFIG_OPTION_DESCRIPTION = "com.nuodb.migration.config.option.description";
    final String CONFIG_ARGUMENT_NAME = "com.nuodb.migration.config.argument.name";
    final String COMMAND_OPTION_DESCRIPTION = "com.nuodb.migration.command.option.description";
    /**
     * Dump plugin resources
     */
    final String DUMP_GROUP_NAME = "com.nuodb.migration.dump.group.name";
    final String SOURCE_GROUP_NAME = "com.nuodb.migration.source.group.name";
    final String SOURCE_DRIVER_OPTION_DESCRIPTION = "com.nuodb.migration.source.driver.option.description";
    final String SOURCE_DRIVER_ARGUMENT_NAME = "com.nuodb.migration.source.driver.argument.name";
    final String SOURCE_URL_OPTION_DESCRIPTION = "com.nuodb.migration.source.url.option.description";
    final String SOURCE_URL_ARGUMENT_NAME = "com.nuodb.migration.source.url.argument.name";
    final String SOURCE_USERNAME_OPTION_DESCRIPTION = "com.nuodb.migration.source.username.option.description";
    final String SOURCE_USERNAME_ARGUMENT_NAME = "com.nuodb.migration.source.username.argument.name";
    final String SOURCE_PASSWORD_OPTION_DESCRIPTION = "com.nuodb.migration.source.password.option.description";
    final String SOURCE_PASSWORD_ARGUMENT_NAME = "com.nuodb.migration.source.password.argument.name";
    final String SOURCE_PROPERTIES_OPTION_DESCRIPTION = "com.nuodb.migration.source.properties.option.description";
    final String SOURCE_PROPERTIES_ARGUMENT_NAME = "com.nuodb.migration.source.properties.argument.name";
    final String SOURCE_CATALOG_OPTION_DESCRIPTION = "com.nuodb.migration.source.catalog.option.description";
    final String SOURCE_CATALOG_ARGUMENT_NAME = "com.nuodb.migration.source.catalog.argument.name";
    final String SOURCE_SCHEMA_OPTION_DESCRIPTION = "com.nuodb.migration.source.schema.option.description";
    final String SOURCE_SCHEMA_ARGUMENT_NAME = "com.nuodb.migration.source.schema.argument.name";

    final String OUTPUT_GROUP_NAME = "com.nuodb.migration.output.group";
    final String OUTPUT_TYPE_OPTION_DESCRIPTION = "com.nuodb.migration.output.type.option.description";
    final String OUTPUT_TYPE_ARGUMENT_NAME = "com.nuodb.migration.output.type.argument.name";
    final String OUTPUT_PATH_OPTION_DESCRIPTION = "com.nuodb.migration.output.path.option.description";
    final String OUTPUT_PATH_ARGUMENT_NAME = "com.nuodb.migration.output.path.argument.name";
    final String OUTPUT_OPTION_DESCRIPTION = "com.nuodb.migration.output.option.description";
    final String OUTPUT_OPTION_ARGUMENT_NAME = "com.nuodb.migration.output.argument.description";

    final String TIME_ZONE_OPTION_DESCRIPTION = "com.nuodb.migration.time.zone.option.description";
    final String TIME_ZONE_ARGUMENT_NAME = "com.nuodb.migration.time.zone.argument.name";

    final String TABLE_GROUP_NAME = "com.nuodb.migration.table.group.name";
    final String TABLE_OPTION_DESCRIPTION = "com.nuodb.migration.table.option.description";
    final String TABLE_ARGUMENT_NAME = "com.nuodb.migration.table.argument.name";
    final String TABLE_FILTER_OPTION_DESCRIPTION = "com.nuodb.migration.table.filter.option.description";
    final String TABLE_FILTER_ARGUMENT_NAME = "com.nuodb.migration.table.filter.argument.name";

    final String QUERY_GROUP_NAME = "com.nuodb.migration.query.group.name";
    final String QUERY_OPTION_DESCRIPTION = "com.nuodb.migration.query.option.description";
    final String QUERY_ARGUMENT_NAME = "com.nuodb.migration.query.argument.name";
    
    final String LOAD_GROUP_NAME = "com.nuodb.migration.load.group.name";
    final String TARGET_GROUP_NAME = "com.nuodb.migration.target.group.name";
    final String TARGET_URL_OPTION_DESCRIPTION = "com.nuodb.migration.target.url.option.description";
    final String TARGET_URL_ARGUMENT_NAME = "com.nuodb.migration.target.url.argument.name";
    final String TARGET_USERNAME_OPTION_DESCRIPTION = "com.nuodb.migration.target.username.option.description";
    final String TARGET_USERNAME_ARGUMENT_NAME = "com.nuodb.migration.target.username.argument.name";
    final String TARGET_PASSWORD_OPTION_DESCRIPTION = "com.nuodb.migration.target.password.option.description";
    final String TARGET_PASSWORD_ARGUMENT_NAME = "com.nuodb.migration.target.password.argument.name";
    final String TARGET_PROPERTIES_OPTION_DESCRIPTION = "com.nuodb.migration.target.properties.option.description";
    final String TARGET_PROPERTIES_ARGUMENT_NAME = "com.nuodb.migration.target.properties.argument.name";
    final String TARGET_SCHEMA_OPTION_DESCRIPTION = "com.nuodb.migration.target.schema.option.description";
    final String TARGET_SCHEMA_ARGUMENT_NAME = "com.nuodb.migration.target.schema.argument.name";

    final String INPUT_GROUP_NAME = "com.nuodb.migration.input.group.name";
    final String INPUT_PATH_OPTION_DESCRIPTION = "com.nuodb.migration.input.path.option.description";
    final String INPUT_PATH_ARGUMENT_NAME = "com.nuodb.migration.input.path.argument.name";
    final String INPUT_OPTION_DESCRIPTION = "com.nuodb.migration.input.option.description";
    final String INPUT_OPTION_ARGUMENT_NAME = "com.nuodb.migration.input.argument.description";

    final String SCHEMA_GROUP_NAME = "com.nuodb.migration.schema.group.name";
    final String SCHEMA_OUTPUT_GROUP_NAME = "com.nuodb.migration.schema.output.group.name";
    final String SCHEMA_META_DATA_OPTION_DESCRIPTION = "com.nuodb.migration.schema.meta.data.option.description";
    final String SCHEMA_META_DATA_ARGUMENT_NAME = "com.nuodb.migration.schema.meta.data.argument.name";

    final String SCHEMA_SCRIPT_TYPE_OPTION_DESCRIPTION = "com.nuodb.migration.schema.script.type.option.description";
    final String SCHEMA_SCRIPT_TYPE_ARGUMENT_NAME = "com.nuodb.migration.schema.script.type.argument.name";
    
    final String SCHEMA_GROUP_SCRIPTS_BY_OPTION_DESCRIPTION = "com.nuodb.migration.schema.group.scripts.by.option.description";
    final String SCHEMA_GROUP_SCRIPTS_BY_ARGUMENT_NAME = "com.nuodb.migration.schema.group.scripts.by.argument.name";
}
