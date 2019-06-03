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
package com.nuodb.migrator.cli;

/**
 * Contains literal constants for the available, recognized command line
 * options, without their prefixes (i.e. double hyphen --). Option prefixes,
 * argument separators, argument value separators are specified in the option
 * getValue instance.
 *
 * @author Sergey Bushik
 */
public interface CliOptions {

    final int HELP_ID = 1;
    final int VERSION_ID = 2;
    final int LIST_ID = 3;
    final int CONFIG_ID = 4;
    final int COMMAND_ID = 5;
    final int MIGRATION_MODE_DATA_ID = 6;
    final int MIGRATION_MODE_SCHEMA_ID = 7;

    final String HELP = "help";
    final String HELP_SHORT = "h";
    final String LIST = "list";
    final String LIST_SHORT = "l";
    final String CONFIG = "config";
    final String VERSION = "version";
    final String VERSION_SHORT = "v";
    final String CONFIG_SHORT = "c";
    final String COMMAND = "command";

    final String DUMP = "dump";
    final String LOAD = "load";
    final String SCHEMA = "schema";

    final String SOURCE_DRIVER = "source.driver";
    final String SOURCE_URL = "source.url";
    final String SOURCE_USERNAME = "source.username";
    final String SOURCE_PASSWORD = "source.password";
    final String SOURCE_PROPERTIES = "source.properties";
    final String SOURCE_CATALOG = "source.catalog";
    final String SOURCE_SCHEMA = "source.schema";
    final String SOURCE_AUTO_COMMIT = "source.auto.commit";
    final String SOURCE_TRANSACTION_ISOLATION = "source.transaction.isolation";

    final String TIME_ZONE = "time.zone";
    final String TIME_ZONE_SHORT = "z";
    final String OUTPUT_OPTION = "output.*";
    final String OUTPUT_TYPE = "output.type";
    final String OUTPUT_PATH = "output.path";

    final String MIGRATION_MODE_DATA = "data";
    final String MIGRATION_MODE_SCHEMA = "schema";

    final String TABLE = "table";
    final String TABLE_EXCLUDE = "table.exclude";
    final String TABLE_TYPE = "table.type";
    final String TABLE_INSERT = "table.*.insert";
    final String TABLE_REPLACE = "table.*.replace";

    final String THREADS = "threads";
    final String THREADS_SHORT = "t";

    final String QUERY_LIMIT = "query.limit";

    final String QUERY = "query";

    final String TARGET_DRIVER = "target.driver";
    final String TARGET_URL = "target.url";
    final String TARGET_USERNAME = "target.username";
    final String TARGET_PASSWORD = "target.password";
    final String TARGET_PROPERTIES = "target.properties";
    final String TARGET_SCHEMA = "target.schema";
    final String TARGET_AUTO_COMMIT = "target.auto.commit";

    final String REPLACE = "replace";
    final String REPLACE_SHORT = "r";

    final String COMMIT_STRATEGY = "commit.strategy";
    final String COMMIT_STRATEGY_ATTRIBUTES = "commit.*";
    final String PARALLELIZER = "parallelizer";
    final String PARALLELIZER_ATTRIBUTES = "parallelizer.*";
    final String PARALLELIZER_SHORT = "p";

    final String INPUT = "input.*";
    final String INPUT_PATH = "input.path";

    final String META_DATA = "meta.data.*";
    final String FAIL_ON_EMPTY_DATABASE = "fail.on.empty.database";
    final String NAMING_STRATEGY = "naming.strategy";
    final String SCRIPT_TYPE = "script.type";
    final String GROUP_SCRIPTS_BY = "group.scripts.by";
    final String IDENTIFIER_QUOTING = "identifier.quoting";
    final String IDENTIFIER_NORMALIZER = "identifier.normalizer";

    final String USE_NUODB_TYPES = "use.nuodb.types";
    final String USE_EXPLICIT_DEFAULTS = "use.explicit.defaults";
    final String JDBC_TYPE_NAME = "type.name";
    final String JDBC_TYPE_CODE = "type.code";
    final String JDBC_TYPE_SIZE = "type.size";
    final String JDBC_TYPE_PRECISION = "type.precision";
    final String JDBC_TYPE_SCALE = "type.scale";
}
