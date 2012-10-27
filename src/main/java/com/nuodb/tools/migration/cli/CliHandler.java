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
package com.nuodb.tools.migration.cli;

import com.nuodb.tools.migration.cli.parse.Option;
import com.nuodb.tools.migration.cli.parse.OptionException;
import com.nuodb.tools.migration.cli.parse.OptionSet;
import com.nuodb.tools.migration.cli.parse.option.OptionToolkit;
import com.nuodb.tools.migration.cli.parse.parser.ParserImpl;
import com.nuodb.tools.migration.cli.run.CliRunFactoryLookup;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Main entry point for the command line interface, for the names of the available options see {@see CliOptions}.
 *
 * @author Sergey Bushik
 */
public class CliHandler extends CliHandlerSupport {

    public CliHandler() {
    }

    public CliHandler(OptionToolkit optionToolkit) {
        super(optionToolkit);
    }

    public CliHandler(OptionToolkit optionToolkit, CliRunFactoryLookup cliRunFactoryLookup) {
        super(optionToolkit, cliRunFactoryLookup);
    }

    public void handle(String[] arguments) throws OptionException {
        Option root = createOption();
        try {
            if (log.isTraceEnabled()) {
                log.trace(String.format("Parsing cli arguments: %1$s", Arrays.asList(arguments)));
            }
            OptionSet options = new ParserImpl().parse(arguments, root);
            handleOptionSet(options, root);
        } catch (OptionException exception) {
            handleOptionException(exception);
        }
    }

    public static void main(String[] args) throws IOException {
        CliHandler handler = new CliHandler();
        // handler.handle(args);
        handler.handle(loadArguments("dump.arguments"));
    }

    private static String[] loadArguments(String resource) throws IOException {
        InputStream input = CliHandler.class.getResourceAsStream(resource);
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        List<String> arguments = new ArrayList<String>();
        String line;
        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.length() != 0 && !line.startsWith("#")) {
                arguments.add(line);
            }
        }
        return arguments.toArray(new String[arguments.size()]);
    }
}
