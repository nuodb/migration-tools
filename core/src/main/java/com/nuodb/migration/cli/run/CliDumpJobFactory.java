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

import com.google.common.collect.Lists;
import com.nuodb.migration.cli.CliResources;
import com.nuodb.migration.cli.parse.CommandLine;
import com.nuodb.migration.cli.parse.Group;
import com.nuodb.migration.cli.parse.Option;
import com.nuodb.migration.cli.parse.option.GroupBuilder;
import com.nuodb.migration.cli.parse.option.OptionFormat;
import com.nuodb.migration.cli.parse.option.OptionToolkit;
import com.nuodb.migration.dump.DumpJobFactory;
import com.nuodb.migration.jdbc.metadata.Table;
import com.nuodb.migration.spec.DumpSpec;
import com.nuodb.migration.spec.NativeQuerySpec;
import com.nuodb.migration.spec.SelectQuerySpec;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newTreeSet;
import static com.nuodb.migration.utils.Priority.LOW;
import static java.lang.Integer.MAX_VALUE;

/**
 * The factory instantiates a {@link CliDumpJobFactory.CliDumpJob}.
 *
 * @author Sergey Bushik
 */
public class CliDumpJobFactory extends CliRunSupport implements CliRunFactory, CliResources {

    /**
     * The "dump" literal command which is matched against the value on the command line. If matched the CliDump object
     * is constructed with {@link #createCliRun()} method.
     */
    private static final String COMMAND = "dump";

    public CliDumpJobFactory(OptionToolkit optionToolkit) {
        super(optionToolkit);
    }

    @Override
    public String getCommand() {
        return COMMAND;
    }

    @Override
    public CliRun createCliRun() {
        return new CliDumpJob();
    }

    /**
     * Table option handles -table=users, -table=roles and stores it items the option in the  command line.
     */
    protected Group createTableGroup() {
        GroupBuilder group = newGroupBuilder().withName(getMessage(TABLE_GROUP_NAME)).withMaximum(MAX_VALUE);

        Option table = newBasicOptionBuilder().
                withName(TABLE_OPTION).
                withDescription(getMessage(TABLE_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(TABLE_ARGUMENT_NAME)).
                                withMinimum(1).
                                withRequired(true).build()
                ).build();
        group.withOption(table);

        Option tableType = newBasicOptionBuilder().
                withName(TABLE_TYPE_OPTION).
                withDescription(getMessage(TABLE_TYPE_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(TABLE_TYPE_ARGUMENT_NAME)).build()
                ).build();
        group.withOption(tableType);

        OptionFormat optionFormat = new OptionFormat(getOptionFormat());
        optionFormat.setValuesSeparator(null);

        Option tableFilter = newRegexOptionBuilder().
                withName(TABLE_FILTER_OPTION).
                withDescription(getMessage(TABLE_FILTER_OPTION_DESCRIPTION)).
                withRegex(TABLE_FILTER_OPTION, 1, LOW).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(TABLE_FILTER_ARGUMENT_NAME)).
                                withOptionFormat(optionFormat).
                                withMinimum(1).
                                withRequired(true).build()
                ).build();

        group.withOption(tableFilter);
        return group.build();
    }

    protected void parseTableGroup(CommandLine commandLine, DumpSpec dumpSpec) {
        Collection<String> tableTypes = newTreeSet(String.CASE_INSENSITIVE_ORDER);
        tableTypes.addAll(commandLine.<String>getValues(TABLE_TYPE_OPTION));
        if (tableTypes.isEmpty()) {
            tableTypes.add(Table.TABLE);
        }
        dumpSpec.setTableTypes(tableTypes);

        Map<String, SelectQuerySpec> tableQueryMapping = newHashMap();
        for (String table : commandLine.<String>getValues(TABLE_OPTION)) {
            tableQueryMapping.put(table, new SelectQuerySpec(table));
        }
        for (Iterator<String> iterator = commandLine.<String>getValues(
                TABLE_FILTER_OPTION).iterator(); iterator.hasNext(); ) {
            String name = iterator.next();
            SelectQuerySpec selectQuerySpec = tableQueryMapping.get(name);
            if (selectQuerySpec == null) {
                tableQueryMapping.put(name, selectQuerySpec = new SelectQuerySpec(name));
            }
            selectQuerySpec.setFilter(iterator.next());
        }
        dumpSpec.setSelectQuerySpecs(tableQueryMapping.values());
    }

    protected Option createNativeQueryGroup() {
        GroupBuilder group = newGroupBuilder().withName(getMessage(QUERY_GROUP_NAME)).withMaximum(MAX_VALUE);

        OptionFormat optionFormat = new OptionFormat(getOptionFormat());
        optionFormat.setValuesSeparator(null);

        Option query = newBasicOptionBuilder().
                withName(QUERY_OPTION).
                withDescription(getMessage(QUERY_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(QUERY_ARGUMENT_NAME)).
                                withMinimum(1).
                                withOptionFormat(optionFormat).
                                withRequired(true).build()
                ).build();
        group.withOption(query);

        return group.build();
    }

    protected Collection<NativeQuerySpec> parseNativeQueryGroup(CommandLine commandLine) {
        List<NativeQuerySpec> nativeQuerySpecs = Lists.newArrayList();
        for (String query : commandLine.<String>getValues(QUERY_OPTION)) {
            NativeQuerySpec nativeQuerySpec = new NativeQuerySpec();
            nativeQuerySpec.setQuery(query);
            nativeQuerySpecs.add(nativeQuerySpec);
        }
        return nativeQuerySpecs;
    }

    /**
     * An implementation of {@link CliRunAdapter} which assembles withConnection spec from provided command line after
     * the validation is passed.
     */
    class CliDumpJob extends CliRunJob {

        public CliDumpJob() {
            super(CliDumpJobFactory.this.getOptionFormat(), COMMAND, new DumpJobFactory());
        }

        @Override
        protected Option createOption() {
            return newGroupBuilder()
                    .withName(getResources().getMessage(DUMP_GROUP_NAME))
                    .withOption(createSourceGroup())
                    .withOption(createOutputGroup())
                    .withOption(createTableGroup())
                    .withOption(createNativeQueryGroup())
                    .withOption(createTimeZoneOption())
                    .withRequired(true).build();
        }

        @Override
        protected void bind(CommandLine commandLine) {
            DumpSpec spec = new DumpSpec();
            spec.setConnectionSpec(parseSourceGroup(commandLine, this));
            spec.setOutputSpec(parseOutputGroup(commandLine, this));
            parseTableGroup(commandLine, spec);
            spec.setNativeQuerySpecs(parseNativeQueryGroup(commandLine));
            spec.setTimeZone(parseTimeZoneOption(commandLine, this));

            ((DumpJobFactory) getJobFactory()).setDumpSpec(spec);
        }
    }
}
