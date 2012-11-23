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
import com.nuodb.migration.cli.parse.Option;
import com.nuodb.migration.cli.parse.option.OptionToolkit;
import com.nuodb.migration.dump.DumpJobFactory;
import com.nuodb.migration.job.Job;
import com.nuodb.migration.spec.DumpSpec;

/**
 * The Factory instantiates a {@link CliDumpJobFactory.CliDumpJob} which is a setValue of groups of options for source database connection spec
 * {@link CliRunSupport#createSourceGroup}, spec of withConnection getValue & type via {@link CliRunSupport#createOutputGroup},
 * table option {@link CliRunSupport#createSelectQueryGroup} and after a validation is passed assembles command line
 * option arguments into a {@link DumpSpec} object.
 *
 * @author Sergey Bushik
 */
public class CliDumpJobFactory implements CliRunFactory, CliResources {

    /**
     * The "withConnection" literal command which is matched against the value on the command line. If matched the CliDump
     * object is constructed with {@link #createCliRun(OptionToolkit)} method.
     */
    public static final String COMMAND = "dump";

    @Override
    public String getCommand() {
        return COMMAND;
    }

    @Override
    public CliRun createCliRun(OptionToolkit optionToolkit) {
        return new CliDumpJob(optionToolkit);
    }

    /**
     * An implementation of {@link CliRunAdapter} which assembles withConnection spec from provided command line after the
     * validation is passed.
     */
    class CliDumpJob extends CliRunJob {

        private DumpJobFactory dumpJobFactory = new DumpJobFactory();

        public CliDumpJob(OptionToolkit optionToolkit) {
            super(optionToolkit, COMMAND);
        }

        @Override
        protected Option createOption() {
            return newGroup()
                    .withName(getResources().getMessage(DUMP_GROUP_NAME))
                    .withOption(createSourceGroup())
                    .withOption(createOutputGroup())
                    .withOption(createSelectQueryGroup())
                    .withOption(createNativeQueryGroup())
                    .withOption(createTimeZoneOption())
                    .withRequired(true).build();
        }

        @Override
        protected void bind(CommandLine commandLine) {
            DumpSpec dumpSpec = new DumpSpec();
            dumpSpec.setSourceSpec(parseSourceGroup(commandLine, this));
            dumpSpec.setOutputSpec(parseOutputGroup(commandLine, this));
            dumpSpec.setSelectQuerySpecs(parseSelectQueryGroup(commandLine, this));
            dumpSpec.setNativeQuerySpecs(parseNativeQueryGroup(commandLine, this));
            dumpSpec.setTimeZone(parseTimeZone(commandLine, this));
            dumpJobFactory.setDumpSpec(dumpSpec);
        }

        @Override
        protected Job createJob() {
            return dumpJobFactory.createJob();
        }
    }
}
