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
import com.nuodb.tools.migration.cli.parse.option.OptionToolkit;
import com.nuodb.tools.migration.load.LoadExecutor;
import com.nuodb.tools.migration.spec.LoadSpec;

/**
 * @author Sergey Bushik
 */
public class CliLoadFactory extends CliOptionsSupport implements CliRunFactory, CliResources {

    public static final String COMMAND = "load";

    @Override
    public String getCommand() {
        return COMMAND;
    }

    @Override
    public CliRun createCliRun(OptionToolkit optionToolkit) {
        return new CliLoad(optionToolkit);
    }

    class CliLoad extends CliRunAdapter {

        private LoadSpec loadSpec;

        public CliLoad(OptionToolkit optionToolkit) {
            super(optionToolkit.newGroup()
                    .withName(resources.getMessage(LOAD_GROUP_NAME))
                    .withOption(createTargetGroup(optionToolkit))
                    .withRequired(true).build(), COMMAND);
        }

        @Override
        protected void bind(CommandLine commandLine) {
            // TODO: parse load spec
            loadSpec = new LoadSpec();
            loadSpec.setConnectionSpec(parseTargetGroup(commandLine, this));
            System.out.println("CliLoadFactory$CliLoad.bind");
        }

        @Override
        public void run() {
            new LoadExecutor().load(loadSpec);
        }
    }
}
