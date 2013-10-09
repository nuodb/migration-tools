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

import com.google.common.base.Function;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.nuodb.migrator.cli.parse.Group;
import com.nuodb.migrator.cli.parse.Option;
import com.nuodb.migrator.cli.parse.OptionException;
import com.nuodb.migrator.cli.parse.OptionSet;
import com.nuodb.migrator.cli.parse.option.GroupBuilder;
import com.nuodb.migrator.cli.parse.option.OptionFormat;
import com.nuodb.migrator.cli.processor.JdbcTypeOptionProcessor;
import com.nuodb.migrator.cli.processor.NuoDBTypesOptionProcessor;
import com.nuodb.migrator.jdbc.dialect.IdentifierNormalizer;
import com.nuodb.migrator.jdbc.dialect.IdentifierQuoting;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.metadata.generator.GroupScriptsBy;
import com.nuodb.migrator.jdbc.metadata.generator.ScriptType;
import com.nuodb.migrator.jdbc.type.JdbcTypeCodes;
import com.nuodb.migrator.schema.SchemaJobFactory;
import com.nuodb.migrator.spec.JdbcTypeSpec;
import com.nuodb.migrator.spec.ResourceSpec;
import com.nuodb.migrator.spec.SchemaSpec;
import com.nuodb.migrator.utils.ReflectionUtils;

import java.util.*;

import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.newLinkedHashSet;
import static com.nuodb.migrator.context.ContextUtils.getMessage;
import static com.nuodb.migrator.jdbc.dialect.IdentifierNormalizers.*;
import static com.nuodb.migrator.jdbc.dialect.IdentifierQuotings.ALWAYS;
import static com.nuodb.migrator.jdbc.dialect.IdentifierQuotings.MINIMAL;
import static com.nuodb.migrator.jdbc.dialect.ImplicitDefaultsTranslator.USE_EXPLICIT_DEFAULTS;
import static com.nuodb.migrator.jdbc.metadata.generator.ScriptType.valueOf;
import static com.nuodb.migrator.utils.Priority.LOW;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.replace;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("PointlessBooleanExpression")
public class CliSchemaJob extends CliRunJob {

    public static final String COMMAND = "schema";

    public static final String IDENTIFIER_QUOTING_MINIMAL = "minimal";
    public static final String IDENTIFIER_QUOTING_ALWAYS = "always";
    public static final String IDENTIFIER_NORMALIZER_NOOP = "noop";
    public static final String IDENTIFIER_NORMALIZER_STANDARD = "standard";
    public static final String IDENTIFIER_NORMALIZER_LOWERCASE = "lowercase";

    public static final String IDENTIFIER_NORMALIZER_UPPERCASE = "uppercase";

    private JdbcTypeOptionProcessor jdbcTypeOptionProcessor = new JdbcTypeOptionProcessor();

    public CliSchemaJob() {
        super(COMMAND);
    }

    @Override
    protected Option createOption() {
        GroupBuilder group = newGroupBuilder().
                withName(getMessage(SCHEMA_GROUP_NAME));
        group.withRequired(true);
        group.withOption(createSourceGroup());
        group.withOption(createTargetGroup());
        group.withOption(createOutputGroup());
        createSchemaOptions(group);
        return group.build();
    }

    @Override
    protected void bind(OptionSet optionSet) {
        SchemaSpec schemaSpec = new SchemaSpec();
        schemaSpec.setSourceConnectionSpec(parseSourceGroup(optionSet, this));
        schemaSpec.setTargetConnectionSpec(parseTargetGroup(optionSet, this));
        schemaSpec.setOutputSpec(parseOutputGroup(optionSet, this));
        parseSchemaOptions(schemaSpec, optionSet, this);

        setJobFactory(new SchemaJobFactory(schemaSpec));
    }

    protected void createSchemaOptions(GroupBuilder group) {
        OptionFormat optionFormat = new OptionFormat(getOptionFormat());
        optionFormat.setValuesSeparator(null);

        GroupBuilder typeGroup = newGroupBuilder().withName(getMessage(JDBC_TYPE_GROUP_NAME));
        Option useNuoDBTypes = newBasicOptionBuilder().
                withName(USE_NUODB_TYPES_OPTION).
                withDescription(getMessage(USE_NUODB_TYPES_OPTION_DESCRIPTION)).
                withRequired(false).
                withOptionProcessor(new NuoDBTypesOptionProcessor()).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(USE_NUODB_TYPES_OPTION_ARGUMENT_NAME)).build()
                ).build();;
        typeGroup.withOption(useNuoDBTypes);

        Option useImplicitDefaults = newBasicOptionBuilder().
                withName(USE_EXPLICIT_DEFAULTS_OPTION).
                withDescription(getMessage(USE_EXPLICIT_DEFAULTS_OPTION_DESCRIPTION)).
                withRequired(false).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(USE_EXPLICIT_DEFAULTS_OPTION_ARGUMENT_NAME)).build()
                ).build();
        typeGroup.withOption(useImplicitDefaults);

        Option typeName = newBasicOptionBuilder().
                withName(JDBC_TYPE_NAME_OPTION).
                withDescription(getMessage(JDBC_TYPE_NAME_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(JDBC_TYPE_NAME_ARGUMENT_NAME)).
                                withOptionFormat(optionFormat).
                                withHelpValues(singleton(getMessage(JDBC_TYPE_NAME_ARGUMENT_NAME))).
                                withOptionProcessor(jdbcTypeOptionProcessor).
                                withMinimum(1).withMaximum(Integer.MAX_VALUE).withRequired(true).build()
                ).build();
        typeGroup.withOption(typeName);

        Option typeCode = newBasicOptionBuilder().
                withName(JDBC_TYPE_CODE_OPTION).
                withDescription(getMessage(JDBC_TYPE_CODE_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(JDBC_TYPE_CODE_ARGUMENT_NAME)).
                                withOptionFormat(optionFormat).
                                withHelpValues(singleton(getMessage(JDBC_TYPE_CODE_ARGUMENT_NAME))).
                                withMinimum(1).withMaximum(Integer.MAX_VALUE).withRequired(true).build()
                ).build();
        typeGroup.withOption(typeCode);

        Option typeSize = newBasicOptionBuilder().
                withName(JDBC_TYPE_SIZE_OPTION).
                withDescription(getMessage(JDBC_TYPE_SIZE_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(JDBC_TYPE_SIZE_ARGUMENT_NAME)).
                                withOptionFormat(optionFormat).
                                withHelpValues(singleton(getMessage(JDBC_TYPE_SIZE_ARGUMENT_NAME))).
                                withMaximum(Integer.MAX_VALUE).build()
                ).build();
        typeGroup.withOption(typeSize);

        Option typePrecision = newBasicOptionBuilder().
                withName(JDBC_TYPE_PRECISION_OPTION).
                withDescription(getMessage(JDBC_TYPE_PRECISION_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(JDBC_TYPE_PRECISION_ARGUMENT_NAME)).
                                withOptionFormat(optionFormat).
                                withHelpValues(singleton(getMessage(JDBC_TYPE_PRECISION_ARGUMENT_NAME))).
                                withMaximum(Integer.MAX_VALUE).build()
                ).build();
        typeGroup.withOption(typePrecision);

        Option typeScale = newBasicOptionBuilder().
                withName(JDBC_TYPE_SCALE_OPTION).
                withDescription(getMessage(JDBC_TYPE_SCALE_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(JDBC_TYPE_SCALE_ARGUMENT_NAME)).
                                withOptionFormat(optionFormat).
                                withHelpValues(singleton(getMessage(JDBC_TYPE_SCALE_ARGUMENT_NAME))).
                                withMaximum(Integer.MAX_VALUE).build()
                ).build();
        typeGroup.withOption(typeScale);
        typeGroup.withMaximum(Integer.MAX_VALUE);
        group.withOption(typeGroup.build());

        Option tableType = newBasicOptionBuilder().
                withName(TABLE_TYPE_OPTION).
                withDescription(getMessage(TABLE_TYPE_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(TABLE_TYPE_ARGUMENT_NAME)).
                                withOptionFormat(optionFormat).
                                withMaximum(Integer.MAX_VALUE).build()
                ).build();
        group.withOption(tableType);

        Option generate = newRegexOptionBuilder().
                withName(SCHEMA_META_DATA_OPTION).
                withDescription(getMessage(SCHEMA_META_DATA_OPTION_DESCRIPTION)).
                withRegex(SCHEMA_META_DATA_OPTION, 1, LOW).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(SCHEMA_META_DATA_ARGUMENT_NAME)).
                                withOptionFormat(optionFormat).
                                withMinimum(1).withMaximum(Integer.MAX_VALUE).build()
                )
                .build();
        group.withOption(generate);

        Collection<String> scriptTypeHelpValues = Lists.transform(asList(ScriptType.values()),
                new Function<ScriptType, String>() {
                    @Override
                    public String apply(ScriptType scriptType) {
                        return scriptType.name().toLowerCase();
                    }
                });
        Option scriptType = newBasicOptionBuilder().
                withName(SCHEMA_SCRIPT_TYPE_OPTION).
                withDescription(getMessage(SCHEMA_SCRIPT_TYPE_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(SCHEMA_SCRIPT_TYPE_ARGUMENT_NAME)).
                                withMinimum(1).
                                withMaximum(ScriptType.values().length).
                                withHelpValues(scriptTypeHelpValues).build()
                ).build();
        group.withOption(scriptType);

        Option groupScriptsBy = newBasicOptionBuilder().
                withName(SCHEMA_GROUP_SCRIPTS_BY_OPTION).
                withDescription(getMessage(SCHEMA_GROUP_SCRIPTS_BY_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(SCHEMA_GROUP_SCRIPTS_BY_ARGUMENT_NAME)).build()
                ).build();
        group.withOption(groupScriptsBy);

        Option identifierQuoting = newBasicOptionBuilder().
                withName(SCHEMA_IDENTIFIER_QUOTING).
                withDescription(getMessage(SCHEMA_IDENTIFIER_QUOTING_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(SCHEMA_IDENTIFIER_QUOTING_ARGUMENT_NAME)).build()
                ).build();
        group.withOption(identifierQuoting);

        Option identifierNormalizer = newBasicOptionBuilder().
                withName(SCHEMA_IDENTIFIER_NORMALIZER).
                withDescription(getMessage(SCHEMA_IDENTIFIER_NORMALIZER_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(SCHEMA_IDENTIFIER_NORMALIZER_ARGUMENT_NAME)).build()
                ).build();
        group.withOption(identifierNormalizer);
    }

    protected void parseSchemaOptions(SchemaSpec schemaSpec, OptionSet optionSet, Option option) {
        if (optionSet.hasOption(SCHEMA_META_DATA_OPTION)) {
            Collection<String> values = optionSet.getValues(SCHEMA_META_DATA_OPTION);
            Set<MetaDataType> objectTypes = newHashSet(MetaDataType.TYPES);
            Map<String, MetaDataType> objectTypeMap = new TreeMap<String, MetaDataType>(String.CASE_INSENSITIVE_ORDER);
            objectTypeMap.putAll(MetaDataType.NAME_TYPE_MAP);
            for (Iterator<String> iterator = values.iterator(); iterator.hasNext(); ) {
                MetaDataType objectType = objectTypeMap.get(replace(iterator.next(), ".", "_"));
                String booleanValue = iterator.next();
                if (booleanValue == null || parseBoolean(booleanValue)) {
                    objectTypes.add(objectType);
                } else {
                    objectTypes.remove(objectType);
                }
            }
            schemaSpec.setObjectTypes(objectTypes);
        }
        Object useExplicitDefaults = optionSet.getValue(USE_EXPLICIT_DEFAULTS_OPTION);
        schemaSpec.setUseExplicitDefaults(useExplicitDefaults != null ?
                parseBoolean(String.valueOf(useExplicitDefaults)) :
                (optionSet.hasOption(USE_EXPLICIT_DEFAULTS_OPTION) || USE_EXPLICIT_DEFAULTS));
        List<String> scriptTypeValues = optionSet.getValues(SCHEMA_SCRIPT_TYPE_OPTION);
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
        String groupScriptsByCondition = (String) optionSet.getValue(SCHEMA_GROUP_SCRIPTS_BY_OPTION);
        GroupScriptsBy groupScriptsBy;
        if (groupScriptsByCondition != null) {
            groupScriptsBy = groupScriptsByConditionMap.get(groupScriptsByCondition);
            if (groupScriptsBy == null) {
                throw new OptionException(format("Unexpected value for %s option, valid values are %s",
                        SCHEMA_GROUP_SCRIPTS_BY_OPTION, groupScriptsByConditionMap.keySet()), option
                );
            }
        } else {
            groupScriptsBy = GroupScriptsBy.TABLE;
        }
        schemaSpec.setGroupScriptsBy(groupScriptsBy);

        String identifierQuotingValue = (String) optionSet.getValue(SCHEMA_IDENTIFIER_QUOTING);
        IdentifierQuoting identifierQuoting = null;
        if (identifierQuotingValue != null) {
            identifierQuoting = getIdentifierQuotings().get(identifierQuotingValue);
            if (identifierQuoting == null) {
                Class<IdentifierQuoting> identifierQuotingClass = ReflectionUtils.loadClass(identifierQuotingValue);
                identifierQuoting = ReflectionUtils.newInstance(identifierQuotingClass);
            }
        }
        schemaSpec.setIdentifierQuoting(identifierQuoting != null ? identifierQuoting : ALWAYS);

        String identifierNormalizerValue = (String) optionSet.getValue(SCHEMA_IDENTIFIER_NORMALIZER);
        IdentifierNormalizer identifierNormalizer = null;
        if (identifierNormalizerValue != null) {
            identifierNormalizer = getIdentifierNormalizers().get(identifierNormalizerValue);
            if (identifierNormalizer == null) {
                Class<IdentifierNormalizer> identifierNormalizerClass =
                        ReflectionUtils.loadClass(identifierNormalizerValue);
                identifierNormalizer =
                        ReflectionUtils.newInstance(identifierNormalizerClass);
            }
        }
        schemaSpec.setIdentifierNormalizer(
                identifierNormalizer != null ? identifierNormalizer : NOOP);

        ListMultimap<String, Object> values = jdbcTypeOptionProcessor.getValues();
        int count = values.get(JDBC_TYPE_NAME_OPTION).size();
        Collection<JdbcTypeSpec> jdbcTypeSpecs = Lists.newArrayList();
        JdbcTypeCodes jdbcTypeCodes = JdbcTypeCodes.getInstance();
        for (int i = 0; i < count; i++) {
            String typeName = (String) values.get(JDBC_TYPE_NAME_OPTION).get(i);
            String typeCode = (String) values.get(JDBC_TYPE_CODE_OPTION).get(i);
            String size = (String) values.get(JDBC_TYPE_SIZE_OPTION).get(i);
            String precision = (String) values.get(JDBC_TYPE_PRECISION_OPTION).get(i);
            String scale = (String) values.get(JDBC_TYPE_SCALE_OPTION).get(i);
            JdbcTypeSpec jdbcTypeSpec = new JdbcTypeSpec();
            jdbcTypeSpec.setTypeName(typeName);
            Integer typeCodeInt = jdbcTypeCodes.getTypeCode(typeCode);
            jdbcTypeSpec.setTypeCode(typeCodeInt != null ? typeCodeInt : parseInt(typeCode));
            jdbcTypeSpec.setSize(!isEmpty(size) ? parseInt(size) : null);
            jdbcTypeSpec.setPrecision(!isEmpty(precision) ? parseInt(precision) : null);
            jdbcTypeSpec.setScale(!isEmpty(scale) ? parseInt(scale) : null);
            jdbcTypeSpecs.add(jdbcTypeSpec);
        }
        schemaSpec.setJdbcTypeSpecs(jdbcTypeSpecs);

        Collection<String> tableTypes = newLinkedHashSet();
        tableTypes.addAll(optionSet.<String>getValues(TABLE_TYPE_OPTION));
        if (tableTypes.isEmpty()) {
            tableTypes.add(Table.TABLE);
            tableTypes.add(Table.ALIAS);
        }
        schemaSpec.setTableTypes(tableTypes.toArray(new String[tableTypes.size()]));
    }

    protected Map<String, IdentifierNormalizer> getIdentifierNormalizers() {
        Map<String, IdentifierNormalizer> identifierNormalizers =
                new TreeMap<String, IdentifierNormalizer>(CASE_INSENSITIVE_ORDER);
        identifierNormalizers.put(IDENTIFIER_NORMALIZER_NOOP, NOOP);
        identifierNormalizers.put(IDENTIFIER_NORMALIZER_STANDARD, STANDARD);
        identifierNormalizers.put(IDENTIFIER_NORMALIZER_LOWERCASE, LOWER_CASE);
        identifierNormalizers.put(IDENTIFIER_NORMALIZER_UPPERCASE, UPPER_CASE);
        return identifierNormalizers;
    }

    protected Map<String, IdentifierQuoting> getIdentifierQuotings() {
        Map<String, IdentifierQuoting> identifierQuotings =
                new TreeMap<String, IdentifierQuoting>(CASE_INSENSITIVE_ORDER);
        identifierQuotings.put(IDENTIFIER_QUOTING_MINIMAL, MINIMAL);
        identifierQuotings.put(IDENTIFIER_QUOTING_ALWAYS, ALWAYS);
        return identifierQuotings;
    }

    @Override
    protected Group createOutputGroup() {
        GroupBuilder group = newGroupBuilder().withName(getMessage(SCHEMA_OUTPUT_GROUP_NAME));
        Option path = newBasicOptionBuilder().
                withName(OUTPUT_PATH_OPTION).
                withRequired(true).
                withDescription(getMessage(OUTPUT_PATH_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(OUTPUT_PATH_ARGUMENT_NAME)).
                                withMinimum(1).
                                withRequired(true).build()
                ).build();
        group.withOption(path);
        return group.build();
    }

    @Override
    protected ResourceSpec parseOutputGroup(OptionSet optionSet, Option option) {
        ResourceSpec resource = null;
        if (optionSet.hasOption(OUTPUT_PATH_OPTION)) {
            resource = new ResourceSpec();
            resource.setPath((String) optionSet.getValue(OUTPUT_PATH_OPTION));
        }
        return resource;
    }

    @Override
    protected Group createTargetGroup() {
        GroupBuilder group = newGroupBuilder().withName(getMessage(TARGET_GROUP_NAME));

        Option url = newBasicOptionBuilder().
                withName(TARGET_URL_OPTION).
                withDescription(getMessage(TARGET_URL_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(TARGET_URL_ARGUMENT_NAME)).
                                withMinimum(1).withRequired(true).build()
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

        Option autoCommit = newBasicOptionBuilder().
                withName(TARGET_AUTO_COMMIT_OPTION).
                withDescription(getMessage(TARGET_AUTO_COMMIT_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(TARGET_AUTO_COMMIT_ARGUMENT_NAME)).build()
                ).build();
        group.withOption(autoCommit);
        return group.build();
    }
}