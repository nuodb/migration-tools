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
package com.nuodb.migrator.cli.run;

import com.google.common.base.Function;
import com.google.common.collect.ListMultimap;
import com.nuodb.migrator.cli.CliSupport;
import com.nuodb.migrator.cli.parse.Group;
import com.nuodb.migrator.cli.parse.Option;
import com.nuodb.migrator.cli.parse.OptionException;
import com.nuodb.migrator.cli.parse.OptionSet;
import com.nuodb.migrator.cli.parse.option.GroupBuilder;
import com.nuodb.migrator.cli.parse.option.OptionFormat;
import com.nuodb.migrator.cli.processor.JdbcTypeOptionProcessor;
import com.nuodb.migrator.cli.processor.NuoDBTypesOptionProcessor;
import com.nuodb.migrator.cli.processor.PasswordOptionProcessor;
import com.nuodb.migrator.cli.validation.ConnectionGroupInfo;
import com.nuodb.migrator.jdbc.commit.CommitStrategy;
import com.nuodb.migrator.jdbc.dialect.IdentifierNormalizer;
import com.nuodb.migrator.jdbc.dialect.IdentifierQuoting;
import com.nuodb.migrator.jdbc.dialect.TranslationConfig;
import com.nuodb.migrator.jdbc.metadata.Identifiable;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.metadata.filter.MetaDataFilter;
import com.nuodb.migrator.jdbc.metadata.filter.MetaDataFilterManager;
import com.nuodb.migrator.jdbc.metadata.generator.ForeignKeyAutoNamingStrategy;
import com.nuodb.migrator.jdbc.metadata.generator.ForeignKeyHashNamingStrategy;
import com.nuodb.migrator.jdbc.metadata.generator.ForeignKeyQualifyNamingStrategy;
import com.nuodb.migrator.jdbc.metadata.generator.GroupScriptsBy;
import com.nuodb.migrator.jdbc.metadata.generator.IndexAutoNamingStrategy;
import com.nuodb.migrator.jdbc.metadata.generator.IndexHashNamingStrategy;
import com.nuodb.migrator.jdbc.metadata.generator.IndexQualifyNamingStrategy;
import com.nuodb.migrator.jdbc.metadata.generator.NamingStrategy;
import com.nuodb.migrator.jdbc.metadata.generator.ScriptType;
import com.nuodb.migrator.jdbc.metadata.generator.SequenceAutoNamingStrategy;
import com.nuodb.migrator.jdbc.metadata.generator.SequenceHashNamingStrategy;
import com.nuodb.migrator.jdbc.metadata.generator.SequenceQualifyNamingStrategy;
import com.nuodb.migrator.jdbc.metadata.generator.TriggerAutoNamingStrategy;
import com.nuodb.migrator.jdbc.metadata.generator.TriggerHashNamingStrategy;
import com.nuodb.migrator.jdbc.metadata.generator.TriggerQualifyNamingStrategy;
import com.nuodb.migrator.jdbc.type.JdbcTypeCodes;
import com.nuodb.migrator.spec.DriverConnectionSpec;
import com.nuodb.migrator.spec.JdbcTypeSpec;
import com.nuodb.migrator.spec.MigrationMode;
import com.nuodb.migrator.spec.ResourceSpec;
import com.nuodb.migrator.spec.ScriptGeneratorJobSpecBase;
import com.nuodb.migrator.utils.PrioritySet;
import com.nuodb.migrator.utils.ReflectionException;
import com.nuodb.migrator.utils.StringUtils;
import org.slf4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.transform;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.newLinkedHashSet;
import static com.nuodb.migrator.backup.format.csv.CsvFormat.TYPE;
import static com.nuodb.migrator.cli.parse.option.OptionUtils.optionUnexpected;
import static com.nuodb.migrator.cli.run.CliOptionValues.*;
import static com.nuodb.migrator.cli.validation.ConnectionGroupValidators.addConnectionGroupValidators;
import static com.nuodb.migrator.context.ContextUtils.getMessage;
import static com.nuodb.migrator.jdbc.JdbcConstants.NUODB_DRIVER;
import static com.nuodb.migrator.jdbc.dialect.IdentifierNormalizers.*;
import static com.nuodb.migrator.jdbc.dialect.IdentifierQuotings.ALWAYS;
import static com.nuodb.migrator.jdbc.metadata.filter.MetaDataFilters.*;
import static com.nuodb.migrator.jdbc.metadata.generator.ScriptType.valueOf;
import static com.nuodb.migrator.match.AntRegexCompiler.isPattern;
import static com.nuodb.migrator.utils.Collections.*;
import static com.nuodb.migrator.utils.Collections.isEmpty;
import static com.nuodb.migrator.utils.Priority.HIGH;
import static com.nuodb.migrator.utils.Priority.LOW;
import static com.nuodb.migrator.utils.ReflectionUtils.loadClass;
import static com.nuodb.migrator.utils.ReflectionUtils.newInstance;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.TimeZone.getTimeZone;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.trim;
import static org.apache.commons.lang3.StringUtils.trimToNull;
import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;
import static org.apache.commons.lang3.StringUtils.replace;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings({ "PointlessBooleanExpression", "ConstantConditions" })
public class CliRunSupport extends CliSupport {

    protected Logger logger = getLogger(getClass());

    public static final TimeZone DEFAULT_TIME_ZONE = getTimeZone("UTC");

    private JdbcTypeOptionProcessor jdbcTypeOptionProcessor = new JdbcTypeOptionProcessor();

    private PasswordOptionProcessor sourcePasswordOptionProcessor = new PasswordOptionProcessor(SOURCE_PASSWORD);

    private PasswordOptionProcessor targetPasswordOptionProcessor = new PasswordOptionProcessor(TARGET_PASSWORD);

    private TimeZone defaultTimeZone = DEFAULT_TIME_ZONE;

    /**
     * Builds the source group of options for the source database connection.
     *
     * @return group of options for the source database.
     */
    protected Group createSourceGroup() {
        GroupBuilder group = newGroupBuilder().withName(getMessage(SOURCE_GROUP_NAME)).withRequired(true)
                .withMinimum(1);

        OptionFormat optionFormat = new OptionFormat(getOptionFormat());
        optionFormat.setValuesSeparator(null);

        Option driver = newBasicOptionBuilder().withName(SOURCE_DRIVER)
                .withDescription(getMessage(SOURCE_DRIVER_OPTION_DESCRIPTION)).withRequired(true)
                .withArgument(newArgumentBuilder().withName(getMessage(SOURCE_DRIVER_ARGUMENT_NAME)).withRequired(true)
                        .withMinimum(1).withOptionFormat(optionFormat).build())
                .build();
        group.withOption(driver);

        Option url = newBasicOptionBuilder().withName(SOURCE_URL)
                .withDescription(getMessage(SOURCE_URL_OPTION_DESCRIPTION)).withRequired(true)
                .withArgument(newArgumentBuilder().withName(getMessage(SOURCE_URL_ARGUMENT_NAME)).withRequired(true)
                        .withMinimum(1).withOptionFormat(optionFormat).build())
                .build();
        group.withOption(url);

        Option username = newBasicOptionBuilder().withName(SOURCE_USERNAME)
                .withDescription(getMessage(SOURCE_USERNAME_OPTION_DESCRIPTION)).withArgument(newArgumentBuilder()
                        .withName(getMessage(SOURCE_USERNAME_ARGUMENT_NAME)).withOptionFormat(optionFormat).build())
                .build();
        group.withOption(username);

        Option password = newBasicOptionBuilder().withName(SOURCE_PASSWORD)
                .withDescription(getMessage(SOURCE_PASSWORD_OPTION_DESCRIPTION))
                .withRequired(false)
                .withArgument(newArgumentBuilder()
                        .withName(getMessage(SOURCE_PASSWORD_ARGUMENT_NAME)).withOptionFormat(optionFormat).build())
                .withOptionProcessor(sourcePasswordOptionProcessor)
                .build();
        group.withOption(password);

        Option properties = newBasicOptionBuilder().withName(SOURCE_PROPERTIES)
                .withDescription(getMessage(SOURCE_PROPERTIES_OPTION_DESCRIPTION))
                .withArgument(newArgumentBuilder().withName(getMessage(SOURCE_PROPERTIES_ARGUMENT_NAME)).build())
                .build();
        group.withOption(properties);

        Option catalog = newBasicOptionBuilder().withName(SOURCE_CATALOG)
                .withDescription(getMessage(SOURCE_CATALOG_OPTION_DESCRIPTION)).withArgument(newArgumentBuilder()
                        .withName(getMessage(SOURCE_CATALOG_ARGUMENT_NAME)).withOptionFormat(optionFormat).build())
                .build();
        group.withOption(catalog);

        Option schema = newBasicOptionBuilder().withName(SOURCE_SCHEMA)
                .withDescription(getMessage(SOURCE_SCHEMA_OPTION_DESCRIPTION)).withArgument(newArgumentBuilder()
                        .withName(getMessage(SOURCE_SCHEMA_ARGUMENT_NAME)).withOptionFormat(optionFormat).build())
                .build();
        group.withOption(schema);

        Option autoCommit = newBasicOptionBuilder().withName(SOURCE_AUTO_COMMIT)
                .withDescription(getMessage(SOURCE_AUTO_COMMIT_OPTION_DESCRIPTION)).withArgument(newArgumentBuilder()
                        .withName(getMessage(SOURCE_AUTO_COMMIT_ARGUMENT_NAME)).withOptionFormat(optionFormat).build())
                .build();
        group.withOption(autoCommit);

        Option transactionIsolation = newBasicOptionBuilder().withName(SOURCE_TRANSACTION_ISOLATION)
                .withDescription(getMessage(SOURCE_TRANSACTION_ISOLATION_OPTION_DESCRIPTION))
                .withArgument(newArgumentBuilder().withName(getMessage(SOURCE_TRANSACTION_ISOLATION_ARGUMENT_NAME))
                        .withOptionFormat(optionFormat).build())
                .build();
        group.withOption(transactionIsolation);

        addConnectionGroupValidators(group, new ConnectionGroupInfo(SOURCE_DRIVER, SOURCE_URL, SOURCE_USERNAME,
                SOURCE_PASSWORD, SOURCE_CATALOG, SOURCE_SCHEMA, SOURCE_PROPERTIES, SOURCE_TRANSACTION_ISOLATION));
        return group.build();
    }

    protected Group createOutputGroup() {
        GroupBuilder group = newGroupBuilder().withName(getMessage(OUTPUT_GROUP_NAME));

        Option type = newBasicOptionBuilder().withName(OUTPUT_TYPE)
                .withDescription(getMessage(OUTPUT_TYPE_OPTION_DESCRIPTION))
                .withArgument(newArgumentBuilder().withName(getMessage(OUTPUT_TYPE_ARGUMENT_NAME)).build()).build();
        group.withOption(type);

        Option path = newBasicOptionBuilder().withName(OUTPUT_PATH)
                .withDescription(getMessage(OUTPUT_PATH_OPTION_DESCRIPTION))
                .withArgument(newArgumentBuilder().withName(getMessage(OUTPUT_PATH_ARGUMENT_NAME)).build()).build();
        group.withOption(path);

        OptionFormat optionFormat = new OptionFormat(getOptionFormat());
        optionFormat.setValuesSeparator(null);

        Option attributes = newRegexOptionBuilder().withName(OUTPUT_OPTION)
                .withDescription(getMessage(OUTPUT_OPTION_DESCRIPTION)).withRegex(OUTPUT_OPTION, 1, LOW)
                .withArgument(newArgumentBuilder().withName(getMessage(OUTPUT_OPTION_ARGUMENT_NAME))
                        .withOptionFormat(optionFormat).withMinimum(1).withMaximum(MAX_VALUE).build())
                .build();
        group.withOption(attributes);

        return group.build();
    }

    protected Option createMigrationModeGroup() {
        GroupBuilder group = newGroupBuilder().withName(getMessage(MIGRATION_MODE_GROUP_NAME));
        Option data = newBasicOptionBuilder().withId(MIGRATION_MODE_DATA_ID).withName(MIGRATION_MODE_DATA)
                .withDescription(getMessage(MIGRATION_MODE_DATA_OPTION_DESCRIPTION)).withRequired(false)
                .withArgument(newArgumentBuilder().withName(getMessage(MIGRATION_MODE_DATA_ARGUMENT_NAME)).build())
                .build();
        group.withOption(data);
        Option schema = newBasicOptionBuilder().withId(MIGRATION_MODE_SCHEMA_ID).withName(MIGRATION_MODE_SCHEMA)
                .withDescription(getMessage(MIGRATION_MODE_SCHEMA_OPTION_DESCRIPTION)).withRequired(false)
                .withArgument(newArgumentBuilder().withName(getMessage(MIGRATION_MODE_SCHEMA_ARGUMENT_NAME)).build())
                .build();
        group.withOption(schema);
        return group.build();
    }

    protected Group createCommitGroup() {
        Option commitStrategy = newBasicOptionBuilder().withName(COMMIT_STRATEGY)
                .withDescription(getMessage(COMMIT_STRATEGY_OPTION_DESCRIPTION))
                .withArgument(newArgumentBuilder().withName(getMessage(COMMIT_STRATEGY_ARGUMENT_NAME)).build()).build();

        OptionFormat optionFormat = new OptionFormat(getOptionFormat());
        optionFormat.setValuesSeparator(null);

        Option commitStrategyAttributes = newRegexOptionBuilder().withName(COMMIT_STRATEGY_ATTRIBUTES)
                .withDescription(getMessage(COMMIT_STRATEGY_ATTRIBUTES_OPTION_DESCRIPTION))
                .withRegex(COMMIT_STRATEGY_ATTRIBUTES, 1, LOW)
                .withArgument(newArgumentBuilder().withName(getMessage(COMMIT_STRATEGY_ATTRIBUTES_ARGUMENT_NAME))
                        .withOptionFormat(optionFormat).withMinimum(1).withMaximum(MAX_VALUE).build())
                .build();
        return newGroupBuilder().withName(getMessage(COMMIT_STRATEGY_GROUP_NAME)).withOption(commitStrategy)
                .withOption(commitStrategyAttributes).build();
    }

    protected Option createTimeZoneOption() {
        return newBasicOptionBuilder().withName(TIME_ZONE).withAlias(TIME_ZONE_SHORT, OptionFormat.SHORT)
                .withDescription(getMessage(TIME_ZONE_OPTION_DESCRIPTION)).withArgument(newArgumentBuilder()
                        .withName(getMessage(TIME_ZONE_ARGUMENT_NAME)).withMinimum(1).withRequired(true).build())
                .build();
    }

    protected Group createTargetGroup() {
        GroupBuilder group = newGroupBuilder().withName(getMessage(TARGET_GROUP_NAME));

        OptionFormat optionFormat = new OptionFormat(getOptionFormat());
        optionFormat.setValuesSeparator(null);

        Option driver = newBasicOptionBuilder().withName(TARGET_DRIVER)
                .withDescription(getMessage(TARGET_DRIVER_OPTION_DESCRIPTION))
                .withArgument(newArgumentBuilder().withName(getMessage(TARGET_DRIVER_ARGUMENT_NAME)).withRequired(true)
                        .withMinimum(1).withOptionFormat(optionFormat).build())
                .build();
        group.withOption(driver);

        Option url = newBasicOptionBuilder().withName(TARGET_URL)
                .withDescription(getMessage(TARGET_URL_OPTION_DESCRIPTION)).withRequired(true)
                .withArgument(newArgumentBuilder().withName(getMessage(TARGET_URL_ARGUMENT_NAME)).withRequired(true)
                        .withMinimum(1).withOptionFormat(optionFormat).build())
                .build();
        group.withOption(url);

        Option username = newBasicOptionBuilder().withName(TARGET_USERNAME)
                .withDescription(getMessage(TARGET_USERNAME_OPTION_DESCRIPTION)).withArgument(newArgumentBuilder()
                        .withName(getMessage(TARGET_USERNAME_ARGUMENT_NAME)).withOptionFormat(optionFormat).build())
                .build();
        group.withOption(username);

        Option password = newBasicOptionBuilder().withName(TARGET_PASSWORD)
                .withDescription(getMessage(TARGET_PASSWORD_OPTION_DESCRIPTION)).withArgument(newArgumentBuilder()
                        .withName(getMessage(TARGET_PASSWORD_ARGUMENT_NAME)).withOptionFormat(optionFormat).build())
                .withOptionProcessor(targetPasswordOptionProcessor)
                .build();
        group.withOption(password);

        Option properties = newBasicOptionBuilder().withName(TARGET_PROPERTIES)
                .withDescription(getMessage(TARGET_PROPERTIES_OPTION_DESCRIPTION))
                .withArgument(newArgumentBuilder().withName(getMessage(TARGET_PROPERTIES_ARGUMENT_NAME)).build())
                .build();
        group.withOption(properties);

        Option schema = newBasicOptionBuilder().withName(TARGET_SCHEMA)
                .withDescription(getMessage(TARGET_SCHEMA_OPTION_DESCRIPTION)).withArgument(newArgumentBuilder()
                        .withName(getMessage(TARGET_SCHEMA_ARGUMENT_NAME)).withOptionFormat(optionFormat).build())
                .build();
        group.withOption(schema);
        group.withRequired(true);
        group.withMinimum(1);

        addConnectionGroupValidators(group, new ConnectionGroupInfo(null, TARGET_URL, TARGET_USERNAME, TARGET_PASSWORD,
                null, TARGET_SCHEMA, TARGET_PROPERTIES, null));
        return group.build();
    }

    protected Group createInputGroup() {
        Option path = newBasicOptionBuilder().withName(INPUT_PATH)
                .withDescription(getMessage(INPUT_PATH_OPTION_DESCRIPTION)).withRequired(true)
                .withArgument(newArgumentBuilder().withName(getMessage(INPUT_PATH_ARGUMENT_NAME)).build()).build();

        OptionFormat optionFormat = new OptionFormat(getOptionFormat());
        optionFormat.setValuesSeparator(null);

        Option attributes = newRegexOptionBuilder().withName(INPUT)
                .withDescription(getMessage(INPUT_OPTION_DESCRIPTION)).withRegex(INPUT, 1, LOW)
                .withArgument(newArgumentBuilder().withName(getMessage(INPUT_OPTION_ARGUMENT_NAME))
                        .withOptionFormat(optionFormat).withMinimum(1).withMaximum(MAX_VALUE).build())
                .build();
        return newGroupBuilder().withName(getMessage(INPUT_GROUP_NAME)).withRequired(true).withMinimum(1)
                .withOption(path).withOption(attributes).build();
    }

    protected Group createSchemaMigrationGroup() {
        GroupBuilder group = newGroupBuilder().withName(getMessage(SCHEMA_MIGRATION_GROUP_NAME));

        OptionFormat optionFormat = new OptionFormat(getOptionFormat());
        optionFormat.setValuesSeparator(null);

        group.withOption(createJdbcTypeGroup());

        Option tableType = newBasicOptionBuilder().withName(TABLE_TYPE)
                .withDescription(getMessage(TABLE_TYPE_OPTION_DESCRIPTION)).withArgument(newArgumentBuilder()
                        .withName(getMessage(TABLE_TYPE_ARGUMENT_NAME)).withMaximum(MAX_VALUE).build())
                .build();
        group.withOption(tableType);

        Option metaData = newRegexOptionBuilder().withName(META_DATA)
                .withDescription(getMessage(META_DATA_OPTION_DESCRIPTION)).withRegex(META_DATA, 1, LOW)
                .withArgument(newArgumentBuilder().withName(getMessage(META_DATA_ARGUMENT_NAME))
                        .withOptionFormat(optionFormat).withMinimum(1).withMaximum(MAX_VALUE).build())
                .build();
        group.withOption(metaData);

        Collection<String> scriptTypeHelpValues = transform(asList(ScriptType.values()),
                new Function<ScriptType, String>() {
                    @Override
                    public String apply(ScriptType scriptType) {
                        return scriptType.name().toLowerCase();
                    }
                });
        Option scriptType = newBasicOptionBuilder().withName(SCRIPT_TYPE)
                .withDescription(getMessage(SCRIPT_TYPE_OPTION_DESCRIPTION))
                .withArgument(newArgumentBuilder().withName(getMessage(SCRIPT_TYPE_ARGUMENT_NAME)).withMinimum(1)
                        .withMaximum(ScriptType.values().length).withHelpValues(scriptTypeHelpValues).build())
                .build();
        group.withOption(scriptType);

        Option groupScriptsBy = newBasicOptionBuilder().withName(GROUP_SCRIPTS_BY)
                .withDescription(getMessage(GROUP_SCRIPTS_BY_OPTION_DESCRIPTION))
                .withArgument(newArgumentBuilder().withName(getMessage(GROUP_SCRIPTS_BY_ARGUMENT_NAME)).build())
                .build();
        group.withOption(groupScriptsBy);

        Option namingStrategy = newBasicOptionBuilder().withName(NAMING_STRATEGY)
                .withDescription(getMessage(NAMING_STRATEGY_OPTION_DESCRIPTION)).withArgument(newArgumentBuilder()
                        .withName(getMessage(NAMING_STRATEGY_ARGUMENT_NAME)).withOptionFormat(optionFormat).build())
                .build();
        group.withOption(namingStrategy);

        Option identifierQuoting = newBasicOptionBuilder().withName(IDENTIFIER_QUOTING)
                .withDescription(getMessage(IDENTIFIER_QUOTING_OPTION_DESCRIPTION)).withArgument(newArgumentBuilder()
                        .withName(getMessage(IDENTIFIER_QUOTING_ARGUMENT_NAME)).withOptionFormat(optionFormat).build()

                ).build();
        group.withOption(identifierQuoting);

        Option identifierNormalizer = newBasicOptionBuilder().withName(IDENTIFIER_NORMALIZER)
                .withDescription(getMessage(IDENTIFIER_NORMALIZER_OPTION_DESCRIPTION))
                .withArgument(newArgumentBuilder().withName(getMessage(IDENTIFIER_NORMALIZER_ARGUMENT_NAME))
                        .withOptionFormat(optionFormat).build())
                .build();
        group.withOption(identifierNormalizer);

        return group.build();
    }

    protected Option createMetaDataFilterManagerGroup() {
        GroupBuilder group = newGroupBuilder().withName(getMessage(META_DATA_FILTER_MANAGER_GROUP_NAME))
                .withMaximum(MAX_VALUE);

        Option table = newBasicOptionBuilder().withName(TABLE).withDescription(getMessage(TABLE_OPTION_DESCRIPTION))
                .withArgument(newArgumentBuilder().withName(getMessage(TABLE_ARGUMENT_NAME)).withMinimum(1)
                        .withMaximum(1).withValueMinimum(1).withValueMaximum(MAX_VALUE).withRequired(true).build())
                .build();
        group.withOption(table);

        Option tableExclude = newBasicOptionBuilder().withName(TABLE_EXCLUDE)
                .withDescription(getMessage(TABLE_EXCLUDE_OPTION_DESCRIPTION))
                .withArgument(newArgumentBuilder().withName(getMessage(TABLE_EXCLUDE_ARGUMENT_NAME)).withMinimum(1)
                        .withMaximum(MAX_VALUE).withRequired(true).build())
                .build();
        group.withOption(tableExclude);
        return group.build();
    }

    protected Group createJdbcTypeGroup() {
        GroupBuilder group = newGroupBuilder().withName(getMessage(JDBC_TYPE_GROUP_NAME));

        OptionFormat optionFormat = new OptionFormat(getOptionFormat());
        optionFormat.setValuesSeparator(null);

        Option useNuoDBTypes = newBasicOptionBuilder().withName(USE_NUODB_TYPES)
                .withDescription(getMessage(USE_NUODB_TYPES_OPTION_DESCRIPTION)).withRequired(false)
                .withOptionProcessor(new NuoDBTypesOptionProcessor())
                .withArgument(newArgumentBuilder().withName(getMessage(USE_NUODB_TYPES_OPTION_ARGUMENT_NAME)).build())
                .build();
        group.withOption(useNuoDBTypes);

        Option useImplicitDefaults = newBasicOptionBuilder().withName(USE_EXPLICIT_DEFAULTS)
                .withDescription(getMessage(USE_EXPLICIT_DEFAULTS_OPTION_DESCRIPTION)).withRequired(false)
                .withArgument(
                        newArgumentBuilder().withName(getMessage(USE_EXPLICIT_DEFAULTS_OPTION_ARGUMENT_NAME)).build())
                .build();
        group.withOption(useImplicitDefaults);

        Option typeName = newBasicOptionBuilder().withName(JDBC_TYPE_NAME)
                .withDescription(getMessage(JDBC_TYPE_NAME_OPTION_DESCRIPTION))
                .withArgument(newArgumentBuilder().withName(getMessage(JDBC_TYPE_NAME_ARGUMENT_NAME))
                        .withOptionFormat(optionFormat)
                        .withHelpValues(singleton(getMessage(JDBC_TYPE_NAME_ARGUMENT_NAME)))
                        .withOptionProcessor(jdbcTypeOptionProcessor).withMinimum(1).withMaximum(MAX_VALUE)
                        .withRequired(true).build())
                .build();
        group.withOption(typeName);

        Option typeCode = newBasicOptionBuilder().withName(JDBC_TYPE_CODE)
                .withDescription(getMessage(JDBC_TYPE_CODE_OPTION_DESCRIPTION))
                .withArgument(newArgumentBuilder().withName(getMessage(JDBC_TYPE_CODE_ARGUMENT_NAME))
                        .withOptionFormat(optionFormat)
                        .withHelpValues(singleton(getMessage(JDBC_TYPE_CODE_ARGUMENT_NAME))).withMinimum(1)
                        .withMaximum(MAX_VALUE).withRequired(true).build())
                .build();
        group.withOption(typeCode);

        Option typeSize = newBasicOptionBuilder().withName(JDBC_TYPE_SIZE)
                .withDescription(getMessage(JDBC_TYPE_SIZE_OPTION_DESCRIPTION))
                .withArgument(newArgumentBuilder().withName(getMessage(JDBC_TYPE_SIZE_ARGUMENT_NAME))
                        .withOptionFormat(optionFormat)
                        .withHelpValues(singleton(getMessage(JDBC_TYPE_SIZE_ARGUMENT_NAME))).withMaximum(MAX_VALUE)
                        .build())
                .build();
        group.withOption(typeSize);

        Option typePrecision = newBasicOptionBuilder().withName(JDBC_TYPE_PRECISION)
                .withDescription(getMessage(JDBC_TYPE_PRECISION_OPTION_DESCRIPTION))
                .withArgument(newArgumentBuilder().withName(getMessage(JDBC_TYPE_PRECISION_ARGUMENT_NAME))
                        .withOptionFormat(optionFormat)
                        .withHelpValues(singleton(getMessage(JDBC_TYPE_PRECISION_ARGUMENT_NAME))).withMaximum(MAX_VALUE)
                        .build())
                .build();
        group.withOption(typePrecision);

        Option typeScale = newBasicOptionBuilder().withName(JDBC_TYPE_SCALE)
                .withDescription(getMessage(JDBC_TYPE_SCALE_OPTION_DESCRIPTION))
                .withArgument(newArgumentBuilder().withName(getMessage(JDBC_TYPE_SCALE_ARGUMENT_NAME))
                        .withOptionFormat(optionFormat)
                        .withHelpValues(singleton(getMessage(JDBC_TYPE_SCALE_ARGUMENT_NAME))).withMaximum(MAX_VALUE)
                        .build())
                .build();
        group.withOption(typeScale);
        group.withMaximum(MAX_VALUE);
        return group.build();
    }

    protected Group createExecutorGroup() {
        GroupBuilder group = newGroupBuilder().withName(getMessage(EXECUTOR_GROUP_NAME)).withRequired(false);
        createExecutorGroup(group);
        return group.build();
    }

    protected void createExecutorGroup(GroupBuilder group) {
        Option threads = newBasicOptionBuilder().withName(THREADS).withAlias(THREADS_SHORT, OptionFormat.SHORT)
                .withDescription(getMessage(THREADS_OPTION_DESCRIPTION))
                .withArgument(newArgumentBuilder().withName(getMessage(THREADS_ARGUMENT_NAME)).build()).build();
        group.withOption(threads);
    }

    protected MetaDataFilterManager parseMetaDataFilterManagerGroup(OptionSet optionSet, Option option) {
        MetaDataFilterManager filterManager = new MetaDataFilterManager();
        Collection<MetaDataFilter<Identifiable>> filters = newArrayList();
        // add --table=table1,table2,*table* filters
        addIgnoreNull(filters, getMetaDataFilter(optionSet, TABLE, false, true));
        // add --table.exclude=table3,table4,*table* filters
        addIgnoreNull(filters, getMetaDataFilter(optionSet, TABLE_EXCLUDE, true, false));
        if (!isEmpty(filters)) {
            filterManager.addMetaDataFilter(newAllOfFilters(MetaDataType.TABLE, filters));
        }
        return filterManager;
    }

    protected MetaDataFilter<Identifiable> getMetaDataFilter(OptionSet optionSet, String option, boolean invertAccept,
            boolean eitherOfFilters) {
        Collection<MetaDataFilter<Identifiable>> filters = newArrayList();
        for (String table : optionSet.<String>getValues(option)) {
            MetaDataFilter<Identifiable> filter;
            boolean qualifyName = table.contains(".");
            if (isPattern(table)) {
                filter = newNameMatchesFilter(MetaDataType.TABLE, qualifyName, table);
            } else {
                filter = newNameEqualsFilter(MetaDataType.TABLE, qualifyName, table);
            }
            filters.add(invertAccept ? newInvertAcceptFilter(MetaDataType.TABLE, filter) : filter);
        }
        MetaDataFilter<Identifiable> filter = null;
        if (!filters.isEmpty()) {
            filter = eitherOfFilters ? newEitherOfFilters(MetaDataType.TABLE, filters)
                    : newAllOfFilters(MetaDataType.TABLE, filters);
        }
        return filter;
    }

    protected DriverConnectionSpec parseSourceGroup(OptionSet optionSet, Option option) {
        DriverConnectionSpec connectionSpec = new DriverConnectionSpec();
        connectionSpec.setDriver((String) optionSet.getValue(SOURCE_DRIVER));
        connectionSpec.setUrl((String) optionSet.getValue(SOURCE_URL));
        connectionSpec.setUsername((String) optionSet.getValue(SOURCE_USERNAME));
        connectionSpec.setPassword(sourcePasswordOptionProcessor.getPassword());
        connectionSpec.setProperties(parseProperties(optionSet, SOURCE_PROPERTIES, option));
        connectionSpec.setCatalog((String) optionSet.getValue(SOURCE_CATALOG));
        connectionSpec.setSchema((String) optionSet.getValue(SOURCE_SCHEMA));
        if (optionSet.hasOption(SOURCE_AUTO_COMMIT)) {
            connectionSpec.setAutoCommit(Boolean.parseBoolean((String) optionSet.getValue(SOURCE_AUTO_COMMIT)));
        }
        Integer transactionIsolation = INSTANCE.getTransactionIsolation(option,
                (String) optionSet.getValue(SOURCE_TRANSACTION_ISOLATION));
        connectionSpec.setTransactionIsolation(transactionIsolation);
        return connectionSpec;
    }

    protected ResourceSpec parseOutputGroup(OptionSet optionSet, Option option) {
        ResourceSpec resource = new ResourceSpec();
        resource.setType((String) optionSet.getValue(OUTPUT_TYPE, TYPE));
        resource.setPath((String) optionSet.getValue(OUTPUT_PATH));
        resource.setAttributes(
                parseAttributes(optionSet.<String>getValues(OUTPUT_OPTION), optionSet.getOption(OUTPUT_OPTION)));
        return resource;
    }

    protected Collection<MigrationMode> parseMigrationModeGroup(OptionSet optionSet, Option option) {
        return parseMigrationModeGroup(optionSet, option, newHashSet(MigrationMode.values()));
    }

    protected Collection<MigrationMode> parseMigrationModeGroup(OptionSet optionSet, Option option,
            Collection<MigrationMode> defaultMigrationModes) {
        Collection<MigrationMode> migrationModes = newHashSet();
        Option dataOption = optionSet.getOption(MIGRATION_MODE_DATA_ID);
        Object dataValue = optionSet.getValue(dataOption);
        if (dataValue != null ? parseBoolean(String.valueOf(dataValue))
                : (optionSet.hasOption(dataOption) || defaultMigrationModes.contains(MigrationMode.DATA))) {
            migrationModes.add(MigrationMode.DATA);
        }
        Option schemaOption = optionSet.getOption(MIGRATION_MODE_SCHEMA_ID);
        Object schemaValue = optionSet.getValue(schemaOption);
        if (schemaValue != null ? parseBoolean(String.valueOf(schemaValue))
                : (optionSet.hasOption(MIGRATION_MODE_SCHEMA)
                        || defaultMigrationModes.contains(MigrationMode.SCHEMA))) {
            migrationModes.add(MigrationMode.SCHEMA);
        }
        return migrationModes;
    }

    protected Map<String, Object> parseAttributes(List<String> values, Option option) {
        Map<String, Object> attributes = newHashMap();
        for (Iterator<String> iterator = values.iterator(); iterator.hasNext();) {
            attributes.put(iterator.next(), iterator.next());
        }
        return attributes;
    }

    protected TimeZone parseTimeZoneOption(OptionSet optionSet, Option option) {
        String timeZone = (String) optionSet.getValue(TIME_ZONE);
        if (timeZone != null) {
            TimeZone systemTimeZone = TimeZone.getDefault();
            try {
                TimeZone.setDefault(getDefaultTimeZone());
                return getTimeZone(timeZone);
            } finally {
                TimeZone.setDefault(systemTimeZone);
            }
        } else {
            return getDefaultTimeZone();
        }
    }

    protected CommitStrategy parseCommitGroup(OptionSet optionSet, Option option) {
        Map<String, CommitStrategy> commitStrategyMapping = INSTANCE.getCommitStrategyMap();
        String commitStrategyValue = (String) optionSet.getValue(COMMIT_STRATEGY, COMMIT_STRATEGY_BATCH);
        CommitStrategy commitStrategy = commitStrategyMapping.get(commitStrategyValue);
        if (commitStrategy == null) {
            commitStrategy = newInstance(commitStrategyValue);
        }
        commitStrategy.setAttributes(parseAttributes(optionSet.<String>getValues(COMMIT_STRATEGY_ATTRIBUTES),
                optionSet.getOption(COMMIT_STRATEGY_ATTRIBUTES)));
        return commitStrategy;
    }

    /**
     * Parses URL encoded properties name1=value1&name2=value2
     *
     * @param optionSet
     *            holding command line options
     * @param trigger
     *            key to key value pairs
     * @param option
     *            the option which contains parsed url
     */
    protected Map<String, Object> parseProperties(OptionSet optionSet, String trigger, Option option) {
        Map<String, Object> properties = newHashMap();
        String url = (String) optionSet.getValue(trigger);
        if (url != null) {
            try {
                url = URLDecoder.decode(url, "UTF-8");
            } catch (UnsupportedEncodingException exception) {
                throw new OptionException(exception.getMessage(), option);
            }
            String[] params = url.split("&");
            for (String param : params) {
                String[] pair = param.split("=");
                if (pair.length != 2) {
                    throw new OptionException(format("Malformed name-value pair %s", pair), option);
                }
                properties.put(pair[0], pair[1]);
            }
        }
        return properties;
    }

    protected DriverConnectionSpec parseTargetGroup(OptionSet optionSet, Option option) {
        if (optionSet.hasOption(TARGET_URL)) {
            DriverConnectionSpec connection = new DriverConnectionSpec();
            connection.setDriver((String) optionSet.getValue(TARGET_DRIVER, NUODB_DRIVER));
            connection.setUrl((String) optionSet.getValue(TARGET_URL));
            connection.setUsername((String) optionSet.getValue(TARGET_USERNAME));
            connection.setPassword(targetPasswordOptionProcessor.getPassword());
            connection.setSchema((String) optionSet.getValue(TARGET_SCHEMA));
            connection.setProperties(parseProperties(optionSet, TARGET_PROPERTIES, option));
            return connection;
        } else {
            return null;
        }
    }

    protected ResourceSpec parseInputGroup(OptionSet optionSet, Option option) {
        ResourceSpec resource = new ResourceSpec();
        resource.setPath((String) optionSet.getValue(INPUT_PATH));
        resource.setAttributes(parseAttributes(optionSet.<String>getValues(INPUT), optionSet.getOption(INPUT)));
        return resource;
    }

    protected void parseSchemaMigrationGroup(OptionSet optionSet, ScriptGeneratorJobSpecBase jobSpec, Option option) {
        jobSpec.setJdbcTypeSpecs(parseJdbcTypeSpecs(optionSet, option));
        jobSpec.setTranslationConfig(parseTranslationConfig(optionSet, option));
        jobSpec.setObjectTypes(parseObjectTypes(optionSet));
        jobSpec.setScriptTypes(parseScriptTypes(optionSet, option));
        jobSpec.setGroupScriptsBy(parseGroupScriptsBy(optionSet, option));
        jobSpec.setNamingStrategies(parseNamingStrategies(optionSet));
        jobSpec.setIdentifierQuoting(parseIdentifierQuoting(optionSet, option));
        jobSpec.setIdentifierNormalizer(parseIdentifierNormalizer(optionSet, option));
        jobSpec.setTableTypes(parseTableTypes(optionSet));
    }

    protected TranslationConfig parseTranslationConfig(OptionSet optionSet, Option option) {
        Object useExplicitDefaultsValue = optionSet.getValue(USE_EXPLICIT_DEFAULTS);
        TranslationConfig translationConfig = new TranslationConfig();
        translationConfig.setUseExplicitDefaults(
                useExplicitDefaultsValue != null ? parseBoolean(String.valueOf(useExplicitDefaultsValue))
                        : (optionSet.hasOption(USE_EXPLICIT_DEFAULTS) || TranslationConfig.USE_EXPLICIT_DEFAULTS));
        return translationConfig;
    }

    protected Collection<JdbcTypeSpec> parseJdbcTypeSpecs(OptionSet optionSet, Option option) {
        ListMultimap<String, Object> values = jdbcTypeOptionProcessor.getValues();
        int count = values.get(JDBC_TYPE_NAME).size();
        Collection<JdbcTypeSpec> jdbcTypeSpecs = newArrayList();
        JdbcTypeCodes jdbcTypeCodes = JdbcTypeCodes.getInstance();
        for (int i = 0; i < count; i++) {
            String typeName = (String) values.get(JDBC_TYPE_NAME).get(i);
            String typeCode = (String) values.get(JDBC_TYPE_CODE).get(i);
            String size = (String) values.get(JDBC_TYPE_SIZE).get(i);
            String precision = (String) values.get(JDBC_TYPE_PRECISION).get(i);
            String scale = (String) values.get(JDBC_TYPE_SCALE).get(i);
            JdbcTypeSpec jdbcTypeSpec = new JdbcTypeSpec();
            jdbcTypeSpec.setTypeName(typeName);
            Integer typeCodeInt = jdbcTypeCodes.getTypeCode(typeCode);
            jdbcTypeSpec.setTypeCode(typeCodeInt != null ? typeCodeInt : parseInt(typeCode));
            jdbcTypeSpec.setSize(!StringUtils.isEmpty(size) ? parseLong(size) : null);
            jdbcTypeSpec.setPrecision(!StringUtils.isEmpty(precision) ? parseInt(precision) : null);
            jdbcTypeSpec.setScale(!StringUtils.isEmpty(scale) ? parseInt(scale) : null);
            jdbcTypeSpecs.add(jdbcTypeSpec);
        }
        return jdbcTypeSpecs;
    }

    protected Collection<ScriptType> parseScriptTypes(OptionSet optionSet, Option option) {
        List<String> scriptTypeValues = optionSet.getValues(SCRIPT_TYPE);
        Collection<ScriptType> scriptTypes = newHashSet();
        for (String scriptTypeValue : scriptTypeValues) {
            scriptTypes.add(valueOf(scriptTypeValue.toUpperCase()));
        }
        if (scriptTypes.isEmpty()) {
            scriptTypes = newHashSet(ScriptType.values());
        }
        return scriptTypes;
    }

    protected GroupScriptsBy parseGroupScriptsBy(OptionSet optionSet, Option option) {
        Map<String, GroupScriptsBy> groupScriptsByConditionMap = newHashMap();
        for (GroupScriptsBy groupScriptsBy : GroupScriptsBy.values()) {
            groupScriptsByConditionMap.put(groupScriptsBy.getCondition(), groupScriptsBy);
        }
        String groupScriptsByCondition = (String) optionSet.getValue(GROUP_SCRIPTS_BY);
        GroupScriptsBy groupScriptsBy;
        if (groupScriptsByCondition != null) {
            groupScriptsBy = groupScriptsByConditionMap.get(groupScriptsByCondition);
            if (groupScriptsBy == null) {
                throw new OptionException(format("Unexpected value for %s option, valid values are %s",
                        GROUP_SCRIPTS_BY, groupScriptsByConditionMap.keySet()), option);
            }
        } else {
            groupScriptsBy = GroupScriptsBy.TABLE;
        }
        return groupScriptsBy;
    }

    protected PrioritySet<NamingStrategy> parseNamingStrategies(OptionSet optionSet) {
        String namingStrategy = trim((String) optionSet.getValue(NAMING_STRATEGY));
        PrioritySet<NamingStrategy> namingStrategies = newPrioritySet();
        if (equalsIgnoreCase(namingStrategy, NAMING_STRATEGY_QUALIFY)) {
            namingStrategies.add(new ForeignKeyQualifyNamingStrategy(), HIGH);
            namingStrategies.add(new IndexQualifyNamingStrategy(), HIGH);
            namingStrategies.add(new SequenceQualifyNamingStrategy(), HIGH);
            namingStrategies.add(new TriggerQualifyNamingStrategy(), HIGH);
        } else if (equalsIgnoreCase(namingStrategy, NAMING_STRATEGY_HASH)) {
            namingStrategies.add(new ForeignKeyHashNamingStrategy(), HIGH);
            namingStrategies.add(new IndexHashNamingStrategy(), HIGH);
            namingStrategies.add(new SequenceHashNamingStrategy(), HIGH);
            namingStrategies.add(new TriggerHashNamingStrategy(), HIGH);
        } else if (equalsIgnoreCase(namingStrategy, NAMING_STRATEGY_AUTO)) {
            namingStrategies.add(new ForeignKeyAutoNamingStrategy(), HIGH);
            namingStrategies.add(new IndexAutoNamingStrategy(), HIGH);
            namingStrategies.add(new SequenceAutoNamingStrategy(), HIGH);
            namingStrategies.add(new TriggerAutoNamingStrategy(), HIGH);
        } else if (isNotEmpty(namingStrategy)) {
            try {
                namingStrategies.add((NamingStrategy) newInstance(namingStrategy), HIGH);
            } catch (ReflectionException exception) {
                optionUnexpected(optionSet.getOption(NAMING_STRATEGY), namingStrategy);
            }
        }
        return namingStrategies;
    }

    protected IdentifierQuoting parseIdentifierQuoting(OptionSet optionSet, Option option) {
        String identifierQuotingValue = (String) optionSet.getValue(IDENTIFIER_QUOTING);
        IdentifierQuoting identifierQuoting = null;
        if (identifierQuotingValue != null) {
            identifierQuoting = INSTANCE.getIdentifierQuotingMap().get(identifierQuotingValue);
            if (identifierQuoting == null) {
                Class<IdentifierQuoting> identifierQuotingClass = loadClass(identifierQuotingValue);
                identifierQuoting = newInstance(identifierQuotingClass);
            }
        }
        return identifierQuoting != null ? identifierQuoting : ALWAYS;
    }

    protected IdentifierNormalizer parseIdentifierNormalizer(OptionSet optionSet, Option option) {
        String identifierNormalizerValue = (String) optionSet.getValue(IDENTIFIER_NORMALIZER);
        IdentifierNormalizer identifierNormalizer = null;
        if (identifierNormalizerValue != null) {
            identifierNormalizer = CliOptionValues.INSTANCE.getIdentifierNormalizerMap().get(identifierNormalizerValue);
            if (identifierNormalizer == null) {
                Class<IdentifierNormalizer> identifierNormalizerClass = loadClass(identifierNormalizerValue);
                identifierNormalizer = newInstance(identifierNormalizerClass);
            }
        }
        return identifierNormalizer != null ? identifierNormalizer : NOOP;
    }

    protected String[] parseTableTypes(OptionSet optionSet) {
        Collection<String> tableTypes = newLinkedHashSet();
        tableTypes.addAll(optionSet.<String>getValues(TABLE_TYPE));
        if (tableTypes.isEmpty()) {
            tableTypes.add(Table.TABLE);
        }
        return tableTypes.toArray(new String[tableTypes.size()]);
    }

    protected Collection<MetaDataType> parseObjectTypes(OptionSet optionSet) {
        Collection<MetaDataType> objectTypes = newArrayList(MetaDataType.TYPES);
        if (optionSet.hasOption(META_DATA)) {
            Collection<String> names = optionSet.getValues(META_DATA);
            Map<String, MetaDataType> objectTypeMap = new TreeMap<String, MetaDataType>(String.CASE_INSENSITIVE_ORDER);
            objectTypeMap.putAll(MetaDataType.NAME_TYPE_MAP);
            for (Iterator<String> iterator = names.iterator(); iterator.hasNext();) {
                String name = iterator.next();
                MetaDataType objectType = objectTypeMap.get(replace(name, ".", "_"));
                String booleanValue = iterator.next();
                if (booleanValue == null || parseBoolean(booleanValue)) {
                    objectTypes.add(objectType);
                } else {
                    if (logger.isWarnEnabled()) {
                        logger.warn(format("Objects of %s type are excluded from processing", name));
                    }
                    objectTypes.remove(objectType);
                }
            }
        }
        return objectTypes;
    }

    protected Integer parseThreadsOption(OptionSet optionSet, Option option) {
        String threadsValue = (String) optionSet.getValue(THREADS);
        return !StringUtils.isEmpty(threadsValue) ? parseInt(threadsValue) : null;
    }

    public TimeZone getDefaultTimeZone() {
        return defaultTimeZone;
    }

    public void setDefaultTimeZone(TimeZone defaultTimeZone) {
        this.defaultTimeZone = defaultTimeZone;
    }
}
