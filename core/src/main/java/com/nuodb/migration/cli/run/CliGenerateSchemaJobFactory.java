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
package com.nuodb.migration.cli.run;

import com.nuodb.migration.cli.CliResources;
import com.nuodb.migration.cli.parse.CommandLine;
import com.nuodb.migration.cli.parse.Group;
import com.nuodb.migration.cli.parse.Option;
import com.nuodb.migration.cli.parse.option.GroupBuilder;
import com.nuodb.migration.cli.parse.option.OptionToolkit;
import com.nuodb.migration.cli.parse.option.RegexOption;
import com.nuodb.migration.jdbc.metadata.MetaDataType;
import com.nuodb.migration.schema.GenerateSchemaJobFactory;
import com.nuodb.migration.spec.GenerateSchemaSpec;
import com.nuodb.migration.spec.ResourceSpec;
import com.nuodb.migration.utils.Priority;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static org.apache.commons.lang3.StringUtils.replace;

/**
 * @author Sergey Bushik
 */
public class CliGenerateSchemaJobFactory extends CliRunSupport implements CliRunFactory, CliResources {

    private static final String COMMAND = "schema";

    public CliGenerateSchemaJobFactory(OptionToolkit optionToolkit) {
        super(optionToolkit);
    }

    @Override
    public String getCommand() {
        return COMMAND;
    }

    @Override
    public CliRun createCliRun() {
        return new CliGenerateSchema();
    }

    class CliGenerateSchema extends CliRunJob {

        public CliGenerateSchema() {
            super(COMMAND, new GenerateSchemaJobFactory());
        }

        @Override
        protected Option createOption() {
            GroupBuilder group = newGroup().withName(getResources().getMessage(GENERATE_SCHEMA_GROUP_NAME));
            group.withRequired(true);
            group.withOption(createSourceGroup());
            group.withOption(createTargetGroup());
            group.withOption(createOutputGroup());
            createGenerateSchemaOptions(group);
            return group.build();
        }

        @Override
        protected void bind(CommandLine commandLine) {
            GenerateSchemaSpec generateSchemaSpec = new GenerateSchemaSpec();
            generateSchemaSpec.setSourceConnectionSpec(parseSourceGroup(commandLine, this));
            generateSchemaSpec.setTargetConnectionSpec(parseTargetGroup(commandLine, this));
            generateSchemaSpec.setOutputSpec(parseOutputGroup(commandLine, this));
            parserGenerateSchemaOptions(generateSchemaSpec, commandLine, this);
            ((GenerateSchemaJobFactory) getJobFactory()).setGenerateSchemaSpec(generateSchemaSpec);
        }
    }

    @Override
    protected Group createOutputGroup() {
        GroupBuilder group = newGroup().withName(getMessage(GENERATE_SCHEMA_OUTPUT_GROUP_NAME));
        Option path = newOption().
                withName(OUTPUT_PATH_OPTION).
                withRequired(true).
                withDescription(getMessage(OUTPUT_PATH_OPTION_DESCRIPTION)).
                withArgument(
                        newArgument().
                                withName(getMessage(OUTPUT_PATH_ARGUMENT_NAME)).
                                withMinimum(1).
                                withRequired(true).build()
                ).build();
        group.withOption(path);
        return group.build();
    }

    protected void createGenerateSchemaOptions(GroupBuilder group) {
        RegexOption generate = new RegexOption();
        generate.setName(GENERATE_META_DATA_OPTION);
        generate.setDescription(getMessage(GENERATE_SCHEMA_META_DATA_OPTION_DESCRIPTION));
        generate.setPrefixes(getOptionFormat().getOptionPrefixes());
        generate.setArgumentSeparator(getOptionFormat().getArgumentSeparator());
        generate.addRegex(GENERATE_META_DATA_OPTION, 1, Priority.LOW);
        generate.setArgument(
                newArgument().
                        withName(getMessage(GENERATE_SCHEMA_META_DATA_ARGUMENT_NAME)).
                        withValuesSeparator(null).withMinimum(1).withMaximum(Integer.MAX_VALUE).build());
        group.withOption(generate);

        Option dropBeforeCreate = newOption().
                withName(DROP_BEFORE_CREATE_OPTION).
                withDescription(getMessage(GENERATE_SCHEMA_DROP_BEFORE_CREATE_DESCRIPTION)).
                withArgument(
                        newArgument().
                                withName(getMessage(GENERATE_SCHEMA_DROP_BEFORE_CREATE_ARGUMENT_NAME)).build()
                ).build();
        group.withOption(dropBeforeCreate);
    }

    protected void parserGenerateSchemaOptions(GenerateSchemaSpec generateSchemaSpec, CommandLine commandLine, Option option) {
        if (commandLine.hasOption(GENERATE_META_DATA_OPTION)) {
            Collection<String> values = commandLine.getValues(GENERATE_META_DATA_OPTION);
            Set<MetaDataType> metaDataTypes = newHashSet(MetaDataType.ALL_TYPES);
            for (Iterator<String> iterator = values.iterator(); iterator.hasNext(); ) {
                MetaDataType metaDataType = new MetaDataType(replace(iterator.next(), ".", " "));
                String generate = iterator.next();
                if (generate == null || Boolean.parseBoolean(generate)) {
                    metaDataTypes.add(metaDataType);
                } else {
                    metaDataTypes.remove(metaDataType);
                }
            }
            generateSchemaSpec.setMetaDataTypes(metaDataTypes);
        }
        String dropBeforeCreate = commandLine.getValue(DROP_BEFORE_CREATE_OPTION);
        if (dropBeforeCreate != null) {
            generateSchemaSpec.setDropBeforeCreate(Boolean.parseBoolean(dropBeforeCreate));
        }
    }

    @Override
    protected ResourceSpec parseOutputGroup(CommandLine commandLine, Option option) {
        ResourceSpec resource = null;
        if (commandLine.hasOption(OUTPUT_PATH_OPTION)) {
            resource = new ResourceSpec();
            resource.setPath(commandLine.<String>getValue(OUTPUT_PATH_OPTION));
        }
        return resource;
    }

    @Override
    protected Group createTargetGroup() {
        GroupBuilder group = newGroup().withName(getMessage(TARGET_GROUP_NAME));

        Option url = newOption().
                withName(TARGET_URL_OPTION).
                withDescription(getMessage(TARGET_URL_OPTION_DESCRIPTION)).
                withRequired(true).
                withArgument(
                        newArgument().
                                withName(getMessage(TARGET_URL_ARGUMENT_NAME)).
                                withRequired(true).
                                withMinimum(1).build()
                ).build();
        group.withOption(url);

        Option username = newOption().
                withName(TARGET_USERNAME_OPTION).
                withDescription(getMessage(TARGET_USERNAME_OPTION_DESCRIPTION)).
                withArgument(
                        newArgument().
                                withName(getMessage(TARGET_USERNAME_ARGUMENT_NAME)).build()
                ).build();
        group.withOption(username);

        Option password = newOption().
                withName(TARGET_PASSWORD_OPTION).
                withDescription(getMessage(TARGET_PASSWORD_OPTION_DESCRIPTION)).
                withArgument(
                        newArgument().
                                withName(getMessage(TARGET_PASSWORD_ARGUMENT_NAME)).build()
                ).build();
        group.withOption(password);

        Option properties = newOption().
                withName(TARGET_PROPERTIES_OPTION).
                withDescription(getMessage(TARGET_PROPERTIES_OPTION_DESCRIPTION)).
                withArgument(
                        newArgument().
                                withName(getMessage(TARGET_PROPERTIES_ARGUMENT_NAME)).build()
                ).build();
        group.withOption(properties);

        Option schema = newOption().
                withName(TARGET_SCHEMA_OPTION).
                withDescription(getMessage(TARGET_SCHEMA_OPTION_DESCRIPTION)).
                withArgument(
                        newArgument().
                                withName(getMessage(TARGET_SCHEMA_ARGUMENT_NAME)).build()
                ).build();
        group.withOption(schema);
        return group.build();
    }
}

