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
package com.nuodb.tools.migration.cli.runnable;

import com.nuodb.tools.migration.cli.CliResources;
import com.nuodb.tools.migration.cli.parse.CommandLine;
import com.nuodb.tools.migration.cli.parse.Group;
import com.nuodb.tools.migration.cli.parse.Option;
import com.nuodb.tools.migration.cli.parse.OptionException;
import com.nuodb.tools.migration.cli.parse.option.OptionToolkit;
import com.nuodb.tools.migration.i18n.Resources;
import com.nuodb.tools.migration.spec.ConnectionSpec;
import com.nuodb.tools.migration.spec.DriverManagerConnectionSpec;
import com.nuodb.tools.migration.spec.OutputSpec;
import com.nuodb.tools.migration.spec.OutputSpecBase;

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
    protected Group createSourceGroup(OptionToolkit optionToolkit) {
        Option driver = optionToolkit.newOption().
                withName(SOURCE_DRIVER_OPTION).
                withDescription(resources.getMessage(SOURCE_DRIVER_OPTION_DESCRIPTION)).
                withRequired(true).
                withArgument(
                        optionToolkit.newArgument().
                                withName(resources.getMessage(SOURCE_DRIVER_ARGUMENT_NAME)).build()
                ).build();
        Option url = optionToolkit.newOption().
                withName(SOURCE_URL_OPTION).
                withDescription(resources.getMessage(SOURCE_URL_OPTION_DESCRIPTION)).
                withRequired(true).
                withArgument(
                        optionToolkit.newArgument().
                                withName(resources.getMessage(SOURCE_URL_ARGUMENT_NAME)).build()
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

    public Group createOutputGroup(OptionToolkit optionToolkit) {
        Resources resources = Resources.getResources();
        Option type = optionToolkit.newOption().
                withName(OUTPUT_TYPE_OPTION).
                withDescription(resources.getMessage(OUTPUT_TYPE_OPTION_DESCRIPTION)).
                withRequired(true).
                withArgument(
                        optionToolkit.newArgument().
                                withName(resources.getMessage(OUTPUT_TYPE_ARGUMENT_NAME)).build()
                ).build();
        Option path = optionToolkit.newOption().
                withName(OUTPUT_PATH_OPTION).
                withDescription(resources.getMessage(OUTPUT_PATH_OPTION_DESCRIPTION)).
                withRequired(true).
                withArgument(
                        optionToolkit.newArgument().
                                withName(resources.getMessage(OUTPUT_PATH_ARGUMENT_NAME)).build()
                ).build();
        return optionToolkit.newGroup().
                withName(resources.getMessage(OUTPUT_GROUP_NAME)).
                withRequired(true).
                withMinimum(1).
                withOption(type).
                withOption(path).build();
    }

    public ConnectionSpec createSource(CommandLine commandLine, Option option) {
        DriverManagerConnectionSpec connection = new DriverManagerConnectionSpec();
        connection.setCatalog(commandLine.<String>getValue(SOURCE_CATALOG_OPTION));
        connection.setSchema(commandLine.<String>getValue(SOURCE_SCHEMA_OPTION));
        connection.setDriver(commandLine.<String>getValue(SOURCE_DRIVER_OPTION));
        connection.setUrl(commandLine.<String>getValue(SOURCE_URL_OPTION));
        connection.setUsername(commandLine.<String>getValue(SOURCE_USERNAME_OPTION));
        connection.setPassword(commandLine.<String>getValue(SOURCE_PASSWORD_OPTION));
        String properties = commandLine.getValue(SOURCE_PROPERTIES_OPTION);
        if (properties != null) {
            Map<String, String> map = parseProperties(option, properties);
            connection.setProperties(map);
        }
        return connection;
    }

    public OutputSpec createOutput(CommandLine commandLine, Option option) {
        OutputSpec output = new OutputSpecBase();
        output.setType(commandLine.<String>getValue(OUTPUT_TYPE_OPTION));
        output.setPath(commandLine.<String>getValue(OUTPUT_PATH_OPTION));
        return output;
    }

    public Map<String, String> parseProperties(Option option, String properties) {
        try {
            properties = URLDecoder.decode(properties, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new OptionException(option, e.getMessage());
        }
        String[] params = properties.split("&");
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
