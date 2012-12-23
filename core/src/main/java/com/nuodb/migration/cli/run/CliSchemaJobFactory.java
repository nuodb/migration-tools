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

import com.google.common.collect.Maps;
import com.nuodb.migration.cli.CliResources;
import com.nuodb.migration.cli.parse.CommandLine;
import com.nuodb.migration.cli.parse.Group;
import com.nuodb.migration.cli.parse.Option;
import com.nuodb.migration.cli.parse.OptionException;
import com.nuodb.migration.cli.parse.option.GroupBuilder;
import com.nuodb.migration.cli.parse.option.OptionToolkit;
import com.nuodb.migration.cli.parse.option.RegexOption;
import com.nuodb.migration.jdbc.metadata.MetaDataType;
import com.nuodb.migration.jdbc.metadata.generator.GroupScriptsBy;
import com.nuodb.migration.jdbc.metadata.generator.ScriptType;
import com.nuodb.migration.schema.SchemaJobFactory;
import com.nuodb.migration.spec.ResourceSpec;
import com.nuodb.migration.spec.SchemaSpec;
import com.nuodb.migration.utils.Priority;

import java.util.*;

import static com.google.common.collect.Sets.newHashSet;
import static com.nuodb.migration.jdbc.metadata.generator.ScriptType.valueOf;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.replace;

/**
 * @author Sergey Bushik
 */
public class CliSchemaJobFactory extends CliRunSupport implements CliRunFactory, CliResources {

    private static final String COMMAND = "schema";

    public CliSchemaJobFactory(OptionToolkit optionToolkit) {
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
            super(COMMAND, new SchemaJobFactory());
        }

        @Override
        protected Option createOption() {
            GroupBuilder group = newGroup().withName(getResources().getMessage(SCHEMA_GROUP_NAME));
            group.withRequired(true);
            group.withOption(createSourceGroup());
            group.withOption(createTargetGroup());
            group.withOption(createOutputGroup());
            createGenerateSchemaOptions(group);
            return group.build();
        }

        @Override
        protected void bind(CommandLine commandLine) {
            SchemaSpec schemaSpec = new SchemaSpec();
            schemaSpec.setSourceConnectionSpec(parseSourceGroup(commandLine, this));
            schemaSpec.setTargetConnectionSpec(parseTargetGroup(commandLine, this));
            schemaSpec.setOutputSpec(parseOutputGroup(commandLine, this));
            parserGenerateSchemaOptions(schemaSpec, commandLine, this);
            ((SchemaJobFactory) getJobFactory()).setSchemaSpec(schemaSpec);
        }
    }

    @Override
    protected Group createOutputGroup() {
        GroupBuilder group = newGroup().withName(getMessage(SCHEMA_OUTPUT_GROUP_NAME));
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
        generate.setName(SCHEMA_META_DATA_OPTION);
        generate.setDescription(getMessage(SCHEMA_META_DATA_OPTION_DESCRIPTION));
        generate.setPrefixes(getOptionFormat().getOptionPrefixes());
        generate.setArgumentSeparator(getOptionFormat().getArgumentSeparator());
        generate.addRegex(SCHEMA_META_DATA_OPTION, 1, Priority.LOW);
        generate.setArgument(
                newArgument().
                        withName(getMessage(SCHEMA_META_DATA_ARGUMENT_NAME)).
                        withValuesSeparator(null).withMinimum(1).withMaximum(Integer.MAX_VALUE).build());
        group.withOption(generate);

        Option scriptType = newOption().
                withName(SCHEMA_SCRIPT_TYPE_OPTION).
                withDescription(getMessage(SCHEMA_SCRIPT_TYPE_OPTION_DESCRIPTION)).
                withArgument(
                        newArgument().
                                withName(getMessage(SCHEMA_SCRIPT_TYPE_ARGUMENT_NAME)).
                                withMaximum(Integer.MAX_VALUE).build()
                ).build();
        group.withOption(scriptType);

        Option groupScriptsBy = newOption().
                withName(SCHEMA_GROUP_SCRIPTS_BY_OPTION).
                withDescription(getMessage(SCHEMA_GROUP_SCRIPTS_BY_OPTION_DESCRIPTION)).
                withArgument(
                        newArgument().
                                withName(getMessage(SCHEMA_GROUP_SCRIPTS_BY_ARGUMENT_NAME)).build()
                ).build();
        group.withOption(groupScriptsBy);
    }

    protected void parserGenerateSchemaOptions(SchemaSpec schemaSpec, CommandLine commandLine, Option option) {
        if (commandLine.hasOption(SCHEMA_META_DATA_OPTION)) {
            Collection<String> values = commandLine.getValues(SCHEMA_META_DATA_OPTION);
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
            schemaSpec.setMetaDataTypes(metaDataTypes);
        }

        List<String> scriptTypeValues = commandLine.getValues(SCHEMA_SCRIPT_TYPE_OPTION);
        Collection<ScriptType> scriptTypes = newHashSet();
        for (String scriptTypeValue : scriptTypeValues) {
            scriptTypes.add(valueOf(scriptTypeValue.toUpperCase()));
        }
        if (scriptTypes.isEmpty()) {
            scriptTypes = newHashSet(ScriptType.values());
        }
        schemaSpec.setScriptTypes(scriptTypes);

        Map<String, GroupScriptsBy> groupScriptsByConditionMap = Maps.newHashMap();
        for (GroupScriptsBy groupScriptsBy : GroupScriptsBy.values()) {
            groupScriptsByConditionMap.put(groupScriptsBy.getCondition(), groupScriptsBy);
        }
        String groupScriptsByCondition = commandLine.getValue(SCHEMA_GROUP_SCRIPTS_BY_OPTION);
        GroupScriptsBy groupScriptsBy;
        if (groupScriptsByCondition != null) {
            groupScriptsBy = groupScriptsByConditionMap.get(groupScriptsByCondition);
            if (groupScriptsBy == null) {
                throw new OptionException(option,
                        format("Unexpected value for %s option, valid values are %s",
                                SCHEMA_GROUP_SCRIPTS_BY_OPTION, groupScriptsByConditionMap.keySet()));
            }
        } else {
            groupScriptsBy = GroupScriptsBy.TABLE;
        }
        schemaSpec.setGroupScriptsBy(groupScriptsBy);
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

