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
public interface CliConstants {

    final String SOURCE_DRIVER_OPTION = "source.driver";
    final String SOURCE_URL_OPTION = "source.url";
    final String SOURCE_USERNAME_OPTION = "source.username";
    final String SOURCE_PASSWORD_OPTION = "source.password";
    final String SOURCE_PROPERTIES_OPTION = "source.properties";
    final String SOURCE_CATALOG_OPTION = "source.catalog";
    final String SOURCE_SCHEMA_OPTION = "source.schema";

    final String OUTPUT_TYPE_OPTION = "output.type";
    final String OUTPUT_PATH_OPTION = "output.path";

    final String DUMP_GROUP_KEY = "com.nuodb.tools.migration.cli.dump.group";

    final String SOURCE_GROUP_KEY = "com.nuodb.tools.migration.cli.source.group";
    final String SOURCE_DRIVER_OPTION_KEY = "com.nuodb.tools.migration.cli.source.driver.option";
    final String SOURCE_DRIVER_ARGUMENT_KEY = "com.nuodb.tools.migration.cli.source.driver.argument";
    final String SOURCE_URL_OPTION_KEY = "com.nuodb.tools.migration.cli.source.url.option";
    final String SOURCE_URL_ARGUMENT_KEY = "com.nuodb.tools.migration.cli.source.url.argument";
    final String SOURCE_USERNAME_OPTION_KEY = "com.nuodb.tools.migration.cli.source.username.option";
    final String SOURCE_USERNAME_ARGUMENT_KEY = "com.nuodb.tools.migration.cli.source.username.argument";
    final String SOURCE_PASSWORD_OPTION_KEY = "com.nuodb.tools.migration.cli.source.password.option";
    final String SOURCE_PASSWORD_ARGUMENT_KEY = "com.nuodb.tools.migration.cli.source.password.argument";
    final String SOURCE_PROPERTIES_OPTION_KEY = "com.nuodb.tools.migration.cli.source.properties.option";
    final String SOURCE_PROPERTIES_ARGUMENT_KEY = "com.nuodb.tools.migration.cli.source.properties.argument";
    final String SOURCE_CATALOG_OPTION_KEY = "com.nuodb.tools.migration.cli.source.catalog.option";
    final String SOURCE_CATALOG_ARGUMENT_KEY = "com.nuodb.tools.migration.cli.source.catalog.argument";
    final String SOURCE_SCHEMA_OPTION_KEY = "com.nuodb.tools.migration.cli.source.schema.option";
    final String SOURCE_SCHEMA_ARGUMENT_KEY = "com.nuodb.tools.migration.cli.source.schema.argument";
    
    final String OUTPUT_GROUP_KEY = "com.nuodb.tools.migration.cli.output.group";
    final String OUTPUT_TYPE_OPTION_KEY = "com.nuodb.tools.migration.cli.output.type.option";
    final String OUTPUT_TYPE_ARGUMENT_KEY = "com.nuodb.tools.migration.cli.output.type.argument";
    final String OUTPUT_PATH_OPTION_KEY = "com.nuodb.tools.migration.cli.output.path.option";
    final String OUTPUT_PATH_ARGUMENT_KEY = "com.nuodb.tools.migration.cli.output.path.argument";
}
