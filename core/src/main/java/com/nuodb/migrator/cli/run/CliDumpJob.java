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

import com.nuodb.migrator.cli.parse.Group;
import com.nuodb.migrator.cli.parse.Option;
import com.nuodb.migrator.cli.parse.OptionSet;
import com.nuodb.migrator.cli.parse.option.GroupBuilder;
import com.nuodb.migrator.cli.parse.option.OptionFormat;
import com.nuodb.migrator.jdbc.query.QueryLimit;
import com.nuodb.migrator.spec.DumpJobSpec;
import com.nuodb.migrator.spec.QuerySpec;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.context.ContextUtils.getMessage;
import static com.nuodb.migrator.utils.Priority.LOW;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.Long.parseLong;
import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * An implementation of {@link CliRunAdapter} which assembles dump spec from
 * provided command line after the validation is passed.
 *
 * @author Sergey Bushik
 */
public class CliDumpJob extends CliJob<DumpJobSpec> {

    public CliDumpJob() {
        super(DUMP);
    }

    @Override
    protected Option createOption() {
        GroupBuilder group = newGroupBuilder().withName(getMessage(DUMP_GROUP_NAME)).withRequired(true);
        group.withOption(createSourceGroup());
        group.withOption(createOutputGroup());
        group.withOption(createMigrationModeGroup());
        group.withOption(createDataMigrationGroup());
        group.withOption(createSchemaMigrationGroup());
        group.withOption(createExecutorGroup());
        return group.build();
    }

    @Override
    protected void bind(OptionSet optionSet) {
        DumpJobSpec dumpJobSpec = new DumpJobSpec();
        dumpJobSpec.setSourceSpec(parseSourceGroup(optionSet, this));
        dumpJobSpec.setOutputSpec(parseOutputGroup(optionSet, this));
        dumpJobSpec.setMigrationModes(parseMigrationModeGroup(optionSet, this));
        parseDataMigrationGroup(optionSet, dumpJobSpec);
        parseSchemaMigrationGroup(optionSet, dumpJobSpec);
        setJobSpec(dumpJobSpec);
    }

    @Override
    public void execute(Map<Object, Object> context) {
        getMigrator().execute(getJobSpec(), context);
    }

    protected Option createDataMigrationGroup() {
        GroupBuilder group = newGroupBuilder().withName(getMessage(DATA_MIGRATION_GROUP_NAME));
        group.withOption(createMetaDataFilterManagerGroup());
        group.withOption(createQueryGroup());
        group.withOption(createTimeZoneOption());
        group.withOption(createQueryLimitOption());
        return group.build();
    }

    protected Option createQueryGroup() {
        GroupBuilder group = newGroupBuilder().withName(getMessage(QUERY_GROUP_NAME)).withMaximum(MAX_VALUE);

        OptionFormat optionFormat = new OptionFormat(getOptionFormat());
        optionFormat.setValuesSeparator(null);
        Option query = newBasicOptionBuilder().withName(QUERY).withDescription(getMessage(QUERY_OPTION_DESCRIPTION))
                .withArgument(newArgumentBuilder().withName(getMessage(QUERY_ARGUMENT_NAME)).withMinimum(1)
                        .withMaximum(MAX_VALUE).withOptionFormat(optionFormat).withRequired(true).build())
                .build();
        group.withOption(query);

        return group.build();
    }

    protected Option createQueryLimitOption() {
        return newBasicOptionBuilder().withName(QUERY_LIMIT).withDescription(getMessage(QUERY_LIMIT_OPTION_DESCRIPTION))
                .withArgument(newArgumentBuilder().withName(getMessage(QUERY_LIMIT_ARGUMENT_NAME)).build()).build();
    }

    protected void parseDataMigrationGroup(OptionSet optionSet, DumpJobSpec jobSpec) {
        jobSpec.setMetaDataFilterManager(parseMetaDataFilterManagerGroup(optionSet, this));
        jobSpec.setQuerySpecs(parseQueryGroup(optionSet));
        jobSpec.setTimeZone(parseTimeZoneOption(optionSet, this));
        jobSpec.setThreads(parseThreadsOption(optionSet, this));
        jobSpec.setQueryLimit(parseQueryLimitOption(optionSet, this));
    }

    protected Collection<QuerySpec> parseQueryGroup(OptionSet optionSet) {
        List<QuerySpec> querySpecs = newArrayList();
        for (String query : optionSet.<String>getValues(QUERY)) {
            querySpecs.add(new QuerySpec(query));
        }
        return querySpecs;
    }

    protected QueryLimit parseQueryLimitOption(OptionSet optionSet, Option option) {
        String queryLimitValue = (String) optionSet.getValue(QUERY_LIMIT);
        return !isEmpty(queryLimitValue) ? new QueryLimit(parseLong(queryLimitValue)) : null;
    }

    @Override
    protected Group createSchemaMigrationGroup() {
        GroupBuilder group = newGroupBuilder().withName(getMessage(SCHEMA_MIGRATION_GROUP_NAME));

        OptionFormat optionFormat = new OptionFormat(getOptionFormat());
        optionFormat.setValuesSeparator(null);

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
        return group.build();
    }

    protected void parseSchemaMigrationGroup(OptionSet optionSet, DumpJobSpec jobSpec) {
        if (optionSet.hasOption(META_DATA)) {
            jobSpec.setObjectTypes(parseObjectTypes(optionSet));
        }
        jobSpec.setTableTypes(parseTableTypes(optionSet));
    }
}
