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
package com.nuodb.tools.migration.cli.run;

import com.nuodb.tools.migration.cli.CliOptions;
import com.nuodb.tools.migration.cli.CliResources;
import com.nuodb.tools.migration.cli.parse.*;
import com.nuodb.tools.migration.cli.parse.option.OptionFormat;
import com.nuodb.tools.migration.cli.parse.option.OptionToolkit;
import com.nuodb.tools.migration.cli.parse.option.RegexOption;
import com.nuodb.tools.migration.i18n.Resources;
import com.nuodb.tools.migration.spec.*;
import com.nuodb.tools.migration.utils.Priority;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.*;

/**
 * @author Sergey Bushik
 */
public class CliOptionsSupport implements CliResources, CliOptions {

    protected Resources resources = Resources.getResources();

    /**
     * Builds the source group of options for the source database connection.
     *
     * @return group of options for the source database.
     */
    public Option createSourceGroup(OptionToolkit optionToolkit) {
        Option driver = optionToolkit.newOption().
                withName(SOURCE_DRIVER_OPTION).
                withDescription(resources.getMessage(SOURCE_DRIVER_OPTION_DESCRIPTION)).
                withRequired(true).
                withArgument(
                        optionToolkit.newArgument().
                                withName(resources.getMessage(SOURCE_DRIVER_ARGUMENT_NAME)).
                                withRequired(true).
                                withMinimum(1).build()
                ).build();
        Option url = optionToolkit.newOption().
                withName(SOURCE_URL_OPTION).
                withDescription(resources.getMessage(SOURCE_URL_OPTION_DESCRIPTION)).
                withRequired(true).
                withArgument(
                        optionToolkit.newArgument().
                                withName(resources.getMessage(SOURCE_URL_ARGUMENT_NAME)).
                                withRequired(true).
                                withMinimum(1).build()
                ).build();
        Option username = optionToolkit.newOption().
                withName(SOURCE_USERNAME_OPTION).
                withDescription(resources.getMessage(SOURCE_USERNAME_OPTION_DESCRIPTION)).
                withArgument(
                        optionToolkit.newArgument().
                                withName(resources.getMessage(SOURCE_USERNAME_ARGUMENT_NAME)).build()
                ).build();
        Option password = optionToolkit.newOption().
                withName(SOURCE_PASSWORD_OPTION).
                withDescription(resources.getMessage(SOURCE_PASSWORD_OPTION_DESCRIPTION)).
                withArgument(
                        optionToolkit.newArgument().
                                withName(resources.getMessage(SOURCE_PASSWORD_ARGUMENT_NAME)).build()
                ).build();
        Option properties = optionToolkit.newOption().
                withName(SOURCE_PROPERTIES_OPTION).
                withDescription(resources.getMessage(SOURCE_PROPERTIES_OPTION_DESCRIPTION)).
                withArgument(
                        optionToolkit.newArgument().
                                withName(resources.getMessage(SOURCE_PROPERTIES_ARGUMENT_NAME)).build()
                ).build();
        Option catalog = optionToolkit.newOption().
                withName(SOURCE_CATALOG_OPTION).
                withDescription(resources.getMessage(SOURCE_CATALOG_OPTION_DESCRIPTION)).
                withArgument(
                        optionToolkit.newArgument().
                                withName(resources.getMessage(SOURCE_CATALOG_ARGUMENT_NAME)).build()
                ).build();
        Option schema = optionToolkit.newOption().
                withName(SOURCE_SCHEMA_OPTION).
                withDescription(resources.getMessage(SOURCE_SCHEMA_OPTION_DESCRIPTION)).
                withArgument(
                        optionToolkit.newArgument().
                                withName(resources.getMessage(SOURCE_SCHEMA_ARGUMENT_NAME)).build()
                ).build();
        return optionToolkit.newGroup().
                withName(resources.getMessage(SOURCE_GROUP_NAME)).
                withRequired(true).
                withMinimum(1).
                withOption(driver).
                withOption(url).
                withOption(username).
                withOption(password).
                withOption(properties).
                withOption(catalog).
                withOption(schema).build();
    }

    public Option createOutputGroup(OptionToolkit optionToolkit) {
        OptionFormat optionFormat = optionToolkit.getOptionFormat();
        Resources resources = Resources.getResources();
        Option type = optionToolkit.newOption().
                withName(OUTPUT_TYPE_OPTION).
                withDescription(resources.getMessage(OUTPUT_TYPE_OPTION_DESCRIPTION)).
                withRequired(true).
                withArgument(
                        optionToolkit.newArgument().
                                withName(resources.getMessage(OUTPUT_TYPE_ARGUMENT_NAME)).
                                withRequired(true).
                                withMinimum(1).build()
                ).build();

        Option path = optionToolkit.newOption().
                withName(OUTPUT_PATH_OPTION).
                withDescription(resources.getMessage(OUTPUT_PATH_OPTION_DESCRIPTION)).
                withRequired(true).
                withArgument(
                        optionToolkit.newArgument().
                                withName(resources.getMessage(OUTPUT_PATH_ARGUMENT_NAME)).
                                withRequired(true).
                                withMinimum(1).build()
                ).build();

        RegexOption attributes = new RegexOption();
        attributes.setName(OUTPUT_OPTION);
        attributes.setDescription(resources.getMessage(OUTPUT_OPTION_DESCRIPTION));
        attributes.setPrefixes(optionFormat.getOptionPrefixes());
        attributes.setArgumentSeparator(optionFormat.getArgumentSeparator());
        attributes.addRegex("output.*", 1, Priority.LOW);
        attributes.setArgument(
                optionToolkit.newArgument().
                        withName("").
                        withValuesSeparator(null).withMinimum(1).build());
        return optionToolkit.newGroup().
                withName(resources.getMessage(OUTPUT_GROUP_NAME)).
                withRequired(true).
                withMinimum(1).
                withOption(type).
                withOption(path).
                withOption(attributes).build();
    }

    /**
     * Table option handles -table=users, -table=roles and stores it items the option in the  command line.
     */
    public Option createSelectQueryGroup(OptionToolkit optionToolkit) {
        OptionFormat optionFormat = optionToolkit.getOptionFormat();
        Option table = optionToolkit.newOption().
                withName(TABLE_OPTION).
                withDescription(resources.getMessage(TABLE_OPTION_DESCRIPTION)).
                withArgument(
                        optionToolkit.newArgument().
                                withName(resources.getMessage(TABLE_ARGUMENT_NAME)).
                                withMinimum(1).
                                withRequired(true).build()
                ).build();

        RegexOption tableFilter = new RegexOption();
        tableFilter.setName(TABLE_FILTER_OPTION);
        tableFilter.setDescription(resources.getMessage(TABLE_FILTER_OPTION_DESCRIPTION));
        tableFilter.setPrefixes(optionFormat.getOptionPrefixes());
        tableFilter.setArgumentSeparator(optionFormat.getArgumentSeparator());
        tableFilter.addRegex(TABLE_FILTER_OPTION, 1, Priority.LOW);
        tableFilter.setArgument(
                optionToolkit.newArgument().
                        withName(resources.getMessage(TABLE_FILTER_ARGUMENT_NAME)).
                        withValuesSeparator(null).
                        withMinimum(1).
                        withRequired(true).build()
        );
        return optionToolkit.newGroup().
                withName(resources.getMessage(TABLE_GROUP_NAME)).
                withOption(table).
                withOption(tableFilter).
                withMaximum(Integer.MAX_VALUE).
                build();
    }

    public ConnectionSpec parseSourceGroup(CommandLine commandLine, Option option) {
        DriverManagerConnectionSpec connection = new DriverManagerConnectionSpec();
        connection.setCatalog(commandLine.<String>getValue(SOURCE_CATALOG_OPTION));
        connection.setSchema(commandLine.<String>getValue(SOURCE_SCHEMA_OPTION));
        connection.setDriver(commandLine.<String>getValue(SOURCE_DRIVER_OPTION));
        connection.setUrl(commandLine.<String>getValue(SOURCE_URL_OPTION));
        connection.setUsername(commandLine.<String>getValue(SOURCE_USERNAME_OPTION));
        connection.setPassword(commandLine.<String>getValue(SOURCE_PASSWORD_OPTION));
        String properties = commandLine.getValue(SOURCE_PROPERTIES_OPTION);
        if (properties != null) {
            Map<String, String> map = parseUrl(option, properties);
            connection.setProperties(map);
        }
        return connection;
    }

    public OutputSpec parseOutputGroup(CommandLine commandLine, Option option) {
        OutputSpec output = new OutputSpecBase();
        output.setType(commandLine.<String>getValue(OUTPUT_TYPE_OPTION));
        output.setPath(commandLine.<String>getValue(OUTPUT_PATH_OPTION));
        output.setAttributes(parseOutputAttributes(
                commandLine.getOption(OUTPUT_OPTION), commandLine.<String>getValues(OUTPUT_OPTION)));
        return output;
    }

    public Map<String, String> parseOutputAttributes(Option option, List<String> values) {
        Map<String, String> attributes = new HashMap<String, String>();
        for (Iterator<String> iterator = values.iterator(); iterator.hasNext(); ) {
            attributes.put(iterator.next(), iterator.next());
        }
        return attributes;
    }

    public Collection<SelectQuerySpec> parseSelectQueryGroup(CommandLine commandLine, Option option) {
        Map<String, SelectQuerySpec> tableQueryMapping = new HashMap<String, SelectQuerySpec>();
        for (String table : commandLine.<String>getValues(TABLE_OPTION)) {
            tableQueryMapping.put(table, new SelectQuerySpec(table));
        }
        for (Iterator<String> iterator = commandLine.<String>getValues(TABLE_FILTER_OPTION).iterator(); iterator.hasNext(); ) {
            String name = iterator.next();
            SelectQuerySpec selectQuerySpec = tableQueryMapping.get(name);
            if (selectQuerySpec == null) {
                tableQueryMapping.put(name, selectQuerySpec = new SelectQuerySpec(name));
            }
            selectQuerySpec.setFilter(iterator.next());
        }
        return tableQueryMapping.values();
    }

    /**
     * Parses url name1=value1&name2=value2 encoded string.
     *
     * @param option the option which contains parsed url
     * @param url    to be parsed
     * @return map of strings to strings formed from key value pairs from url
     */
    public Map<String, String> parseUrl(Option option, String url) {
        try {
            url = URLDecoder.decode(url, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new OptionException(option, e.getMessage());
        }
        String[] params = url.split("&");
        Map<String, String> map = new HashMap<String, String>();
        for (String param : params) {
            String[] pair = param.split("=");
            if (pair.length != 2) {
                throw new OptionException(option, String.format("Malformed name value pair %1$s", pair));
            }
            map.put(pair[0], pair[1]);
        }
        return map;
    }
}
