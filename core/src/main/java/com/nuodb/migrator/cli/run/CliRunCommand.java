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

import com.nuodb.migrator.cli.parse.CommandLine;
import com.nuodb.migrator.cli.parse.OptionException;
import com.nuodb.migrator.cli.parse.option.CommandBase;

import java.util.Collection;
import java.util.ListIterator;

/**
 * @author Sergey Bushik
 */
public class CliRunCommand extends CommandBase {

    private CliRunLookup cliRunLookup;

    public CliRunCommand(CliRunLookup cliRunLookup) {
        this.cliRunLookup = cliRunLookup;
    }

    @Override
    public Collection<String> getCommands() {
        return cliRunLookup.getCommands();
    }

    @Override
    public boolean canProcess(CommandLine commandLine, String argument) {
        CliRun cliRun = (CliRun) commandLine.getValue(this);
        return (cliRun != null && cliRun.canProcess(commandLine, argument)) || super.canProcess(commandLine, argument);
    }

    @Override
    protected void preProcessOption(CommandLine commandLine, ListIterator<String> arguments) {
        CliRun cliRun = getCliRun(commandLine, arguments);
        cliRun.preProcess(commandLine, arguments);
    }

    @Override
    protected void process(CommandLine commandLine, ListIterator<String> arguments, String argument) {
        CliRun cliRun = getCliRun(commandLine, arguments);
        cliRun.process(commandLine, arguments);
    }

    protected CliRun getCliRun(CommandLine commandLine, ListIterator<String> arguments) {
        CliRun cliRun = (CliRun) commandLine.getValue(this);
        if (cliRun == null) {
            String command = arguments.next();
            commandLine.addValue(this, cliRun = cliRunLookup.get(command));
        }
        return cliRun;
    }

    @Override
    public void postProcess(CommandLine commandLine) {
        super.postProcess(commandLine);
        CliRun cliRun = (CliRun) commandLine.getValue(this);
        try {
            if (cliRun != null) {
                cliRun.postProcess(commandLine);
            }
        } catch (OptionException exception) {
            throw new OptionException(exception.getMessage(), cliRun);
        }
    }
}
