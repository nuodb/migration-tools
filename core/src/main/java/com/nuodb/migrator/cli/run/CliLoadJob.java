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

import com.google.common.collect.Maps;
import com.nuodb.migrator.backup.loader.Parallelizer;
import com.nuodb.migrator.backup.loader.RowLevelParallelizer;
import com.nuodb.migrator.backup.loader.TableLevelParallelizer;
import com.nuodb.migrator.cli.parse.Option;
import com.nuodb.migrator.cli.parse.OptionSet;
import com.nuodb.migrator.cli.parse.option.GroupBuilder;
import com.nuodb.migrator.cli.parse.option.OptionFormat;
import com.nuodb.migrator.jdbc.query.InsertType;
import com.nuodb.migrator.spec.LoadJobSpec;

import java.util.Map;
import java.util.TreeMap;

import static com.nuodb.migrator.context.ContextUtils.getMessage;
import static com.nuodb.migrator.utils.Priority.LOW;
import static com.nuodb.migrator.utils.ReflectionUtils.newInstance;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.String.CASE_INSENSITIVE_ORDER;

/**
 * @author Sergey Bushik
 */
public class CliLoadJob extends CliJob<LoadJobSpec> {

    public static final String PARALLELIZER_TABLE_LEVEL = "table.level";
    public static final String PARALLELIZER_ROW_LEVEL = "row.level";

    public CliLoadJob() {
        super(LOAD);
    }

    @Override
    protected Option createOption() {
        GroupBuilder group = newGroupBuilder().withName(getMessage(LOAD_GROUP_NAME)).withRequired(true);
        group.withOption(createTargetGroup());
        group.withOption(createInputGroup());
        group.withOption(createMigrationModeGroup());
        group.withOption(createDataMigrationGroup());
        group.withOption(createSchemaMigrationGroup());
        group.withOption(createExecutorGroup());
        return group.build();
    }

    @Override
    protected void bind(OptionSet optionSet) {
        LoadJobSpec jobSpec = new LoadJobSpec();
        jobSpec.setTargetSpec(parseTargetGroup(optionSet, this));
        jobSpec.setInputSpec(parseInputGroup(optionSet, this));
        jobSpec.setMigrationModes(parseMigrationModeGroup(optionSet, this));
        parseDataMigrationGroup(optionSet, jobSpec);
        parseSchemaMigrationGroup(optionSet, jobSpec, this);
        parseExecutorGroup(optionSet, jobSpec);
        setJobSpec(jobSpec);
    }

    @Override
    public void execute(Map<Object, Object> context) {
        getMigrator().execute(getJobSpec(), context);
    }

    protected Option createDataMigrationGroup() {
        GroupBuilder group = newGroupBuilder().withName(getMessage(DATA_MIGRATION_GROUP_NAME));
        group.withOption(createMetaDataFilterManagerGroup());
        group.withOption(createCommitGroup());
        group.withOption(createInsertTypeGroup());
        group.withOption(createTimeZoneOption());
        return group.build();
    }

    protected Option createInsertTypeGroup() {
        GroupBuilder group = newGroupBuilder().withName(getMessage(INSERT_TYPE_GROUP_NAME));

        Option replace = newBasicOptionBuilder().withName(REPLACE).withAlias(REPLACE_SHORT, OptionFormat.SHORT)
                .withDescription(getMessage(REPLACE_OPTION_DESCRIPTION)).build();
        group.withOption(replace);

        Option replaceType = newRegexOptionBuilder().withName(TABLE_REPLACE)
                .withDescription(getMessage(TABLE_REPLACE_OPTION_DESCRIPTION)).withRegex(TABLE_REPLACE, 1, LOW).build();
        group.withOption(replaceType);

        Option insertType = newRegexOptionBuilder().withName(TABLE_INSERT)
                .withDescription(getMessage(TABLE_INSERT_OPTION_DESCRIPTION)).withRegex(TABLE_INSERT, 1, LOW).build();
        group.withOption(insertType);

        return group.build();
    }

    @Override
    protected void createExecutorGroup(GroupBuilder group) {
        super.createExecutorGroup(group);

        Option parallelizer = newBasicOptionBuilder().withName(PARALLELIZER)
                .withAlias(PARALLELIZER_SHORT, OptionFormat.SHORT)
                .withDescription(getMessage(PARALLELIZER_OPTION_DESCRIPTION))
                .withArgument(newArgumentBuilder().withName(getMessage(PARALLELIZER_ARGUMENT_NAME)).build()).build();
        group.withOption(parallelizer);

        OptionFormat optionFormat = new OptionFormat(getOptionFormat());
        optionFormat.setValuesSeparator(null);

        Option parallelizerAttributes = newRegexOptionBuilder().withName(PARALLELIZER_ATTRIBUTES)
                .withDescription(getMessage(PARALLELIZER_ATTRIBUTES_OPTION_DESCRIPTION))
                .withRegex(PARALLELIZER_ATTRIBUTES, 1, LOW)
                .withArgument(newArgumentBuilder().withName(getMessage(PARALLELIZER_ATTRIBUTES_ARGUMENT_NAME))
                        .withOptionFormat(optionFormat).withMinimum(1).withMaximum(MAX_VALUE).build())
                .build();
        group.withOption(parallelizerAttributes);
    }

    protected void parseDataMigrationGroup(OptionSet optionSet, LoadJobSpec jobSpec) {
        jobSpec.setMetaDataFilterManager(parseMetaDataFilterManagerGroup(optionSet, this));
        jobSpec.setCommitStrategy(parseCommitGroup(optionSet, this));
        jobSpec.setTimeZone(parseTimeZoneOption(optionSet, this));
        parseInsertTypeGroup(optionSet, jobSpec);
    }

    protected void parseInsertTypeGroup(OptionSet optionSet, LoadJobSpec loadJobSpec) {
        loadJobSpec.setInsertType(optionSet.hasOption(REPLACE) ? InsertType.REPLACE : InsertType.INSERT);
        Map<String, InsertType> tableInsertTypes = Maps.newHashMap();
        for (String table : optionSet.<String>getValues(TABLE_INSERT)) {
            tableInsertTypes.put(table, InsertType.INSERT);
        }
        for (String table : optionSet.<String>getValues(TABLE_REPLACE)) {
            tableInsertTypes.put(table, InsertType.REPLACE);
        }
        loadJobSpec.setTableInsertTypes(tableInsertTypes);
    }

    protected void parseExecutorGroup(OptionSet optionSet, LoadJobSpec jobSpec) {
        jobSpec.setThreads(parseThreadsOption(optionSet, this));
        String parallelizerValue = (String) optionSet.getValue(PARALLELIZER, PARALLELIZER_TABLE_LEVEL);
        Parallelizer parallelizer = createParallelizerMapping().get(parallelizerValue);
        if (parallelizer == null) {
            parallelizer = newInstance(parallelizerValue);
        }
        parallelizer.setAttributes(parseAttributes(optionSet.<String>getValues(PARALLELIZER_ATTRIBUTES),
                optionSet.getOption(PARALLELIZER_ATTRIBUTES)));
        jobSpec.setParallelizer(parallelizer);
    }

    protected Map<String, Parallelizer> createParallelizerMapping() {
        Map<String, Parallelizer> parallelizerMapping = new TreeMap<String, Parallelizer>(CASE_INSENSITIVE_ORDER);
        parallelizerMapping.put(PARALLELIZER_TABLE_LEVEL, new TableLevelParallelizer());
        parallelizerMapping.put(PARALLELIZER_ROW_LEVEL, new RowLevelParallelizer());
        return parallelizerMapping;
    }
}