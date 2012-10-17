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
 * Contains literal constants for the available, recognized command line options, without their prefixes (i.e. double
 * hyphen --). Option prefixes, argument separators, argument value separators are specified in the option format
 * instance.
 *
 * @author Sergey Bushik
 */
public interface CliOptions {

    final int HELP_OPTION_ID = 1;
    final int LIST_OPTION_ID = 2;
    final int CONFIG_OPTION_ID = 3;
    final int COMMAND_OPTION_ID = 4;

    /**
     * Formats & shows help.
     */
    final String HELP_OPTION = "help";
    /**
     * Lists available command line interface commands, such as dump, load, copy.
     */
    final String LIST_OPTION = "list";
    /**
     * Points the where migration process is specified in the form of XML file.
     */
    final String CONFIG_OPTION = "config";
    /**
     * Command line interface command to execute.
     */
    final String COMMAND_OPTION = "command";

    final String SOURCE_DRIVER_OPTION = "source.driver";
    final String SOURCE_URL_OPTION = "source.url";
    final String SOURCE_USERNAME_OPTION = "source.username";
    final String SOURCE_PASSWORD_OPTION = "source.password";
    final String SOURCE_PROPERTIES_OPTION = "source.properties";
    final String SOURCE_CATALOG_OPTION = "source.catalog";
    final String SOURCE_SCHEMA_OPTION = "source.schema";

    final String OUTPUT_OPTION = "output.*";
    final String OUTPUT_TYPE_OPTION = "output.type";
    final String OUTPUT_PATH_OPTION = "output.path";

    final String TABLE_OPTION = "table";
    final String TABLE_FILTER_OPTION = "table.*.filter";

    final String TARGET_URL_OPTION = "target.url";
    final String TARGET_USERNAME_OPTION = "target.username";
    final String TARGET_PASSWORD_OPTION = "target.password";
    final String TARGET_PROPERTIES_OPTION = "target.properties";
    final String TARGET_SCHEMA_OPTION = "target.schema";
}
