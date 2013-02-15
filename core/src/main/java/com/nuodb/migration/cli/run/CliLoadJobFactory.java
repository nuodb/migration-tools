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
import com.nuodb.migration.cli.parse.Option;
import com.nuodb.migration.cli.parse.option.GroupBuilder;
import com.nuodb.migration.cli.parse.option.OptionFormat;
import com.nuodb.migration.cli.parse.option.OptionToolkit;
import com.nuodb.migration.cli.parse.option.RegexOption;
import com.nuodb.migration.jdbc.query.InsertType;
import com.nuodb.migration.load.LoadJobFactory;
import com.nuodb.migration.spec.LoadSpec;

import java.util.Map;

import static com.nuodb.migration.utils.Priority.LOW;

/**
 * @author Sergey Bushik
 */
public class CliLoadJobFactory extends CliRunSupport implements CliRunFactory, CliResources {

    private static final String COMMAND = "load";

    public CliLoadJobFactory(OptionToolkit optionToolkit) {
        super(optionToolkit);
    }

    @Override
    public String getCommand() {
        return COMMAND;
    }

    @Override
    public CliRun createCliRun() {
        return new CliLoadJob();
    }

    class CliLoadJob extends CliRunJob {

        public CliLoadJob() {
            super(CliLoadJobFactory.this.getOptionFormat(), COMMAND, new LoadJobFactory());
        }

        @Override
        protected Option createOption() {
            return newGroup()
                    .withName(getResources().getMessage(LOAD_GROUP_NAME))
                    .withOption(createTargetGroup())
                    .withOption(createInputGroup())
                    .withOption(createInsertTypeGroup())
                    .withOption(createTimeZoneOption())
                    .withRequired(true).build();
        }

        @Override
        protected void bind(CommandLine commandLine) {
            LoadSpec loadSpec = new LoadSpec();
            loadSpec.setConnectionSpec(parseTargetGroup(commandLine, this));
            loadSpec.setInputSpec(parseInputGroup(commandLine, this));
            loadSpec.setTimeZone(parseTimeZoneOption(commandLine, this));
            parseInsertTypeGroup(commandLine, loadSpec);
            ((LoadJobFactory) getJobFactory()).setLoadSpec(loadSpec);
        }
    }

    protected Option createInsertTypeGroup() {
        GroupBuilder group = newGroup().withName(getMessage(INSERT_TYPE_GROUP_NAME));

        Option replace = newOption().
                withName(REPLACE_OPTION).
                withAlias(REPLACE_SHORT_OPTION, OptionFormat.SHORT).
                withDescription(getMessage(REPLACE_OPTION_DESCRIPTION)).build();
        group.withOption(replace);

        RegexOption replaceType = new RegexOption();
        replaceType.setOptionFormat(getOptionFormat());
        replaceType.setName(TABLE_REPLACE_OPTION);
        replaceType.setDescription(getMessage(TABLE_REPLACE_OPTION_DESCRIPTION));
        replaceType.addRegex(TABLE_REPLACE_OPTION, 1, LOW);
        group.withOption(replaceType);

        RegexOption insertType = new RegexOption();
        insertType.setOptionFormat(getOptionFormat());
        insertType.setName(TABLE_INSERT_OPTION);
        insertType.setDescription(getMessage(TABLE_INSERT_OPTION_DESCRIPTION));
        insertType.addRegex(TABLE_INSERT_OPTION, 1, LOW);
        group.withOption(insertType);

        return group.build();
    }

    protected void parseInsertTypeGroup(CommandLine commandLine, LoadSpec loadSpec) {
        loadSpec.setInsertType(commandLine.hasOption(REPLACE_OPTION) ? InsertType.REPLACE : InsertType.INSERT);
        Map<String, InsertType> tableInsertTypes = Maps.newHashMap();
        for (String table : commandLine.<String>getValues(TABLE_INSERT_OPTION)) {
            tableInsertTypes.put(table, InsertType.INSERT);
        }
        for (String table : commandLine.<String>getValues(TABLE_REPLACE_OPTION)) {
            tableInsertTypes.put(table, InsertType.REPLACE);
        }
        loadSpec.setTableInsertTypes(tableInsertTypes);
    }
}
