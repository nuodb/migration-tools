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
package com.nuodb.tools.migration.cli.run;

import com.nuodb.tools.migration.cli.CliResources;
import com.nuodb.tools.migration.cli.parse.CommandLine;
import com.nuodb.tools.migration.cli.parse.Option;
import com.nuodb.tools.migration.cli.parse.option.OptionToolkit;
import com.nuodb.tools.migration.dump.DumpWriter;
import com.nuodb.tools.migration.spec.DumpSpec;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.SQLException;

/**
 * @author Sergey Bushik
 */
public class CliDumpFactory extends CliOptionsSupport implements CliRunnableFactory, CliResources {

    public static final String DUMP_COMMAND = "dump";
    private transient final Log log = LogFactory.getLog(getClass());

    @Override
    public String getCommand() {
        return DUMP_COMMAND;
    }

    @Override
    public CliRunnable createRunnable(OptionToolkit optionToolkit) {
        return new CliDump(
                optionToolkit.newGroup().
                        withName(resources.getMessage(DUMP_GROUP_NAME)).
                        withOption(createSourceGroup(optionToolkit)).
                        withOption(createOutputGroup(optionToolkit)).
                        withRequired(true).build()
        );
    }

    class CliDump extends CliRunnableOption {

        private DumpSpec dump;

        public CliDump(Option option) {
            super(option, DUMP_COMMAND);
        }

        @Override
        protected void bind(CommandLine commandLine, Option option) {
            dump = new DumpSpec();
            dump.setConnectionSpec(createSource(commandLine, option));
            dump.setOutputSpec(createOutput(commandLine, option));
        }

        @Override
        public void run() {
            try {
                new DumpWriter().write(dump);
            } catch (SQLException e) {
                if (log.isWarnEnabled()) {
                    log.warn(e);
                }
            }
        }
    }
}
