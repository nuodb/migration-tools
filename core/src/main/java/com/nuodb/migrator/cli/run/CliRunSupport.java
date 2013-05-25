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
package com.nuodb.migrator.cli.run;

import com.nuodb.migrator.cli.CliSupport;
import com.nuodb.migrator.cli.parse.Group;
import com.nuodb.migrator.cli.parse.Option;
import com.nuodb.migrator.cli.parse.OptionException;
import com.nuodb.migrator.cli.parse.OptionSet;
import com.nuodb.migrator.cli.parse.option.GroupBuilder;
import com.nuodb.migrator.cli.parse.option.OptionFormat;
import com.nuodb.migrator.cli.validation.ConnectionGroupInfo;
import com.nuodb.migrator.jdbc.JdbcConstants;
import com.nuodb.migrator.spec.DriverConnectionSpec;
import com.nuodb.migrator.spec.ResourceSpec;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static com.google.common.collect.Maps.newHashMap;
import static com.nuodb.migrator.cli.validation.ConnectionGroupValidators.addConnectionGroupValidators;
import static com.nuodb.migrator.utils.Priority.LOW;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
public class CliRunSupport extends CliSupport {

    public static final TimeZone DEFAULT_TIME_ZONE = TimeZone.getTimeZone("UTC");

    private TimeZone defaultTimeZone = DEFAULT_TIME_ZONE;

    /**
     * Builds the source group of options for the source database connection.
     *
     * @return group of options for the source database.
     */
    protected Group createSourceGroup() {
        GroupBuilder group = newGroupBuilder().
                withName(getMessage(SOURCE_GROUP_NAME)).
                withRequired(true).
                withMinimum(1);

        Option driver = newBasicOptionBuilder().
                withName(SOURCE_DRIVER_OPTION).
                withDescription(getMessage(SOURCE_DRIVER_OPTION_DESCRIPTION)).
                withRequired(true).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(SOURCE_DRIVER_ARGUMENT_NAME)).
                                withRequired(true).
                                withMinimum(1).build()
                ).build();
        group.withOption(driver);

        Option url = newBasicOptionBuilder().
                withName(SOURCE_URL_OPTION).
                withDescription(getMessage(SOURCE_URL_OPTION_DESCRIPTION)).
                withRequired(true).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(SOURCE_URL_ARGUMENT_NAME)).
                                withRequired(true).
                                withMinimum(1).build()
                ).build();
        group.withOption(url);

        Option username = newBasicOptionBuilder().
                withName(SOURCE_USERNAME_OPTION).
                withDescription(getMessage(SOURCE_USERNAME_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(SOURCE_USERNAME_ARGUMENT_NAME)).build()
                ).build();
        group.withOption(username);

        Option password = newBasicOptionBuilder().
                withName(SOURCE_PASSWORD_OPTION).
                withDescription(getMessage(SOURCE_PASSWORD_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(SOURCE_PASSWORD_ARGUMENT_NAME)).build()
                ).build();
        group.withOption(password);

        Option properties = newBasicOptionBuilder().
                withName(SOURCE_PROPERTIES_OPTION).
                withDescription(getMessage(SOURCE_PROPERTIES_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(SOURCE_PROPERTIES_ARGUMENT_NAME)).build()
                ).build();
        group.withOption(properties);

        Option catalog = newBasicOptionBuilder().
                withName(SOURCE_CATALOG_OPTION).
                withDescription(getMessage(SOURCE_CATALOG_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(SOURCE_CATALOG_ARGUMENT_NAME)).build()
                ).build();
        group.withOption(catalog);

        Option schema = newBasicOptionBuilder().
                withName(SOURCE_SCHEMA_OPTION).
                withDescription(getMessage(SOURCE_SCHEMA_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(SOURCE_SCHEMA_ARGUMENT_NAME)).build()
                ).build();
        group.withOption(schema);

        Option autoCommit = newBasicOptionBuilder().
                withName(SOURCE_AUTO_COMMIT_OPTION).
                withDescription(getMessage(SOURCE_AUTO_COMMIT_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(SOURCE_AUTO_COMMIT_ARGUMENT_NAME)).build()
                ).build();
        group.withOption(autoCommit);

        addConnectionGroupValidators(group, new ConnectionGroupInfo(
                SOURCE_DRIVER_OPTION, SOURCE_URL_OPTION, SOURCE_USERNAME_OPTION, SOURCE_PASSWORD_OPTION,
                SOURCE_CATALOG_OPTION, SOURCE_SCHEMA_OPTION, SOURCE_PROPERTIES_OPTION));
        return group.build();
    }

    protected Group createOutputGroup() {
        GroupBuilder group = newGroupBuilder().
                withName(getMessage(OUTPUT_GROUP_NAME)).
                withRequired(true).
                withMinimum(1);

        Option type = newBasicOptionBuilder().
                withName(OUTPUT_TYPE_OPTION).
                withDescription(getMessage(OUTPUT_TYPE_OPTION_DESCRIPTION)).
                withRequired(true).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(OUTPUT_TYPE_ARGUMENT_NAME)).
                                withRequired(true).withMinimum(1).build()
                ).build();
        group.withOption(type);

        Option path = newBasicOptionBuilder().
                withName(OUTPUT_PATH_OPTION).
                withDescription(getMessage(OUTPUT_PATH_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(OUTPUT_PATH_ARGUMENT_NAME)).build()
                ).build();
        group.withOption(path);

        OptionFormat optionFormat = new OptionFormat(getOptionFormat());
        optionFormat.setValuesSeparator(null);

        Option attributes = newRegexOptionBuilder().
                withName(OUTPUT_OPTION).
                withDescription(getMessage(OUTPUT_OPTION_DESCRIPTION)).
                withRegex(OUTPUT_OPTION, 1, LOW).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(OUTPUT_OPTION_ARGUMENT_NAME)).
                                withOptionFormat(optionFormat).withMinimum(1).withMaximum(MAX_VALUE).build()
                )
                .build();
        group.withOption(attributes);

        return group.build();
    }

    protected DriverConnectionSpec parseSourceGroup(OptionSet optionSet, Option option) {
        DriverConnectionSpec connectionSpec = new DriverConnectionSpec();
        connectionSpec.setDriver((String) optionSet.getValue(SOURCE_DRIVER_OPTION));
        connectionSpec.setUrl((String) optionSet.getValue(SOURCE_URL_OPTION));
        connectionSpec.setUsername((String) optionSet.getValue(SOURCE_USERNAME_OPTION));
        connectionSpec.setPassword((String) optionSet.getValue(SOURCE_PASSWORD_OPTION));
        connectionSpec.setProperties(parseProperties(optionSet, SOURCE_PROPERTIES_OPTION, option));
        connectionSpec.setCatalog((String) optionSet.getValue(SOURCE_CATALOG_OPTION));
        connectionSpec.setSchema((String) optionSet.getValue(SOURCE_SCHEMA_OPTION));
        if (optionSet.hasOption(SOURCE_AUTO_COMMIT_OPTION)) {
            connectionSpec.setAutoCommit(Boolean.parseBoolean((String) optionSet.getValue(SOURCE_AUTO_COMMIT_OPTION)));
        }
        return connectionSpec;
    }

    protected ResourceSpec parseOutputGroup(OptionSet optionSet, Option option) {
        ResourceSpec resource = new ResourceSpec();
        resource.setType((String) optionSet.getValue(OUTPUT_TYPE_OPTION));
        resource.setPath((String) optionSet.getValue(OUTPUT_PATH_OPTION));
        resource.setAttributes(parseAttributes(
                optionSet.<String>getValues(OUTPUT_OPTION), optionSet.getOption(OUTPUT_OPTION)));
        return resource;
    }

    protected Map<String, Object> parseAttributes(List<String> values, Option option) {
        Map<String, Object> attributes = newHashMap();
        for (Iterator<String> iterator = values.iterator(); iterator.hasNext(); ) {
            attributes.put(iterator.next(), iterator.next());
        }
        return attributes;
    }

    protected Option createTimeZoneOption() {
        return newBasicOptionBuilder().
                withName(TIME_ZONE_OPTION).
                withAlias(TIME_ZONE_SHORT_OPTION, OptionFormat.SHORT).
                withDescription(getMessage(TIME_ZONE_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(TIME_ZONE_ARGUMENT_NAME)).
                                withMinimum(1).
                                withRequired(true).build()
                ).build();
    }

    protected TimeZone parseTimeZoneOption(OptionSet optionSet, Option option) {
        String timeZone = (String) optionSet.getValue(TIME_ZONE_OPTION);
        if (timeZone != null) {
            TimeZone systemTimeZone = TimeZone.getDefault();
            try {
                TimeZone.setDefault(getDefaultTimeZone());
                return TimeZone.getTimeZone(timeZone);
            } finally {
                TimeZone.setDefault(systemTimeZone);
            }
        } else {
            return getDefaultTimeZone();
        }
    }

    /**
     * Parses URL encoded properties name1=value1&name2=value2
     *
     * @param optionSet holding command line options
     * @param trigger   key to key value pairs
     * @param option    the option which contains parsed url
     */
    protected Map<String, Object> parseProperties(OptionSet optionSet, String trigger, Option option) {
        Map<String, Object> properties = newHashMap();
        String url = (String) optionSet.getValue(trigger);
        if (url != null) {
            try {
                url = URLDecoder.decode(url, "UTF-8");
            } catch (UnsupportedEncodingException exception) {
                throw new OptionException(option, exception.getMessage());
            }
            String[] params = url.split("&");
            for (String param : params) {
                String[] pair = param.split("=");
                if (pair.length != 2) {
                    throw new OptionException(option, format("Malformed name-value pair %s", pair));
                }
                properties.put(pair[0], pair[1]);
            }
        }
        return properties;
    }

    protected Group createTargetGroup() {
        GroupBuilder group = newGroupBuilder().
                withName(getMessage(TARGET_GROUP_NAME));

        Option url = newBasicOptionBuilder().
                withName(TARGET_URL_OPTION).
                withDescription(getMessage(TARGET_URL_OPTION_DESCRIPTION)).
                withRequired(true).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(TARGET_URL_ARGUMENT_NAME)).
                                withRequired(true).
                                withMinimum(1).build()
                ).build();
        group.withOption(url);

        Option username = newBasicOptionBuilder().
                withName(TARGET_USERNAME_OPTION).
                withDescription(getMessage(TARGET_USERNAME_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(TARGET_USERNAME_ARGUMENT_NAME)).build()
                ).build();
        group.withOption(username);

        Option password = newBasicOptionBuilder().
                withName(TARGET_PASSWORD_OPTION).
                withDescription(getMessage(TARGET_PASSWORD_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(TARGET_PASSWORD_ARGUMENT_NAME)).build()
                ).build();
        group.withOption(password);

        Option properties = newBasicOptionBuilder().
                withName(TARGET_PROPERTIES_OPTION).
                withDescription(getMessage(TARGET_PROPERTIES_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(TARGET_PROPERTIES_ARGUMENT_NAME)).build()
                ).build();
        group.withOption(properties);

        Option schema = newBasicOptionBuilder().
                withName(TARGET_SCHEMA_OPTION).
                withDescription(getMessage(TARGET_SCHEMA_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(TARGET_SCHEMA_ARGUMENT_NAME)).build()
                ).build();
        group.withOption(schema);
        group.withRequired(true);
        group.withMinimum(1);

        addConnectionGroupValidators(group, new ConnectionGroupInfo(
                null, TARGET_URL_OPTION, TARGET_USERNAME_OPTION, TARGET_PASSWORD_OPTION,
                null, TARGET_SCHEMA_OPTION, TARGET_PROPERTIES_OPTION));
        return group.build();
    }

    protected Group createInputGroup() {
        Option path = newBasicOptionBuilder().
                withName(INPUT_PATH_OPTION).
                withDescription(getMessage(INPUT_PATH_OPTION_DESCRIPTION)).
                withRequired(true).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(INPUT_PATH_ARGUMENT_NAME)).build()
                ).build();

        OptionFormat optionFormat = new OptionFormat(getOptionFormat());
        optionFormat.setValuesSeparator(null);

        Option attributes = newRegexOptionBuilder().
                withName(INPUT_OPTION).
                withDescription(getMessage(INPUT_OPTION_DESCRIPTION)).
                withRegex(INPUT_OPTION, 1, LOW).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(INPUT_OPTION_ARGUMENT_NAME)).
                                withOptionFormat(optionFormat).withMinimum(1).withMaximum(MAX_VALUE).build()
                )
                .build();
        return newGroupBuilder().
                withName(getMessage(INPUT_GROUP_NAME)).
                withRequired(true).
                withMinimum(1).
                withOption(path).
                withOption(attributes).build();
    }

    protected DriverConnectionSpec parseTargetGroup(OptionSet optionSet, Option option) {
        if (optionSet.hasOption(TARGET_URL_OPTION)) {
            DriverConnectionSpec connection = new DriverConnectionSpec();
            connection.setDriver(JdbcConstants.NUODB_DRIVER);
            connection.setUrl((String) optionSet.getValue(TARGET_URL_OPTION));
            connection.setUsername((String) optionSet.getValue(TARGET_USERNAME_OPTION));
            connection.setPassword((String) optionSet.getValue(TARGET_PASSWORD_OPTION));
            connection.setSchema((String) optionSet.getValue(TARGET_SCHEMA_OPTION));
            connection.setProperties(parseProperties(optionSet, TARGET_PROPERTIES_OPTION, option));
            return connection;
        } else {
            return null;
        }
    }

    protected ResourceSpec parseInputGroup(OptionSet optionSet, Option option) {
        ResourceSpec resource = new ResourceSpec();
        resource.setPath((String) optionSet.getValue(INPUT_PATH_OPTION));
        resource.setAttributes(parseAttributes(
                optionSet.<String>getValues(INPUT_OPTION), optionSet.getOption(INPUT_OPTION)));
        return resource;
    }

    public TimeZone getDefaultTimeZone() {
        return defaultTimeZone;
    }

    public void setDefaultTimeZone(TimeZone defaultTimeZone) {
        this.defaultTimeZone = defaultTimeZone;
    }
}
