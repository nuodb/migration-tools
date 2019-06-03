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
package com.nuodb.migrator.cli.parse.option;

import com.google.common.collect.Lists;
import com.nuodb.migrator.cli.parse.Argument;
import com.nuodb.migrator.cli.parse.CommandLine;
import com.nuodb.migrator.cli.parse.Help;
import com.nuodb.migrator.cli.parse.HelpHint;
import com.nuodb.migrator.cli.parse.Option;
import com.nuodb.migrator.cli.parse.OptionException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static com.nuodb.migrator.cli.parse.HelpHint.*;
import static com.nuodb.migrator.cli.parse.option.OptionFactory.createArgumentSpy;
import static com.nuodb.migrator.cli.parse.option.OptionFactory.createArguments;
import static com.nuodb.migrator.cli.parse.option.OptionFactory.createCommandLineMock;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.singleton;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Tests methods of the {@link Argument} option type.
 *
 * @author Sergey Bushik
 */
public class ArgumentTest {

    private Argument argument;

    @BeforeMethod
    public void setUp() {
        argument = createArgumentSpy();
    }

    /**
     * Tests that different option values can be processed as arguments.
     */
    @Test
    public void testCanProcess() {
        CommandLine commandLine = createCommandLineMock();

        assertTrue(argument.canProcess(commandLine, "--option"));
        assertTrue(argument.canProcess(commandLine, "value1"));
        assertTrue(argument.canProcess(commandLine, "value2"));

        ListIterator<String> arguments = createArguments("--option", "value1", "value2");
        assertTrue(argument.canProcess(commandLine, arguments));
    }

    /**
     * Verifies that the arguments correctly collected from the command line and
     * saved as argument values.
     */
    @Test
    public void testProcessArgument() {
        argument.setMaximum(Integer.MAX_VALUE);

        CommandLine commandLine = createCommandLineMock();
        when(commandLine.getValues(argument)).thenReturn(newArrayList());

        ListIterator<String> arguments = createArguments("value1", "value2", "argument2");
        argument.process(commandLine, arguments);

        verify(commandLine).addValue(argument, "value1");
        verify(commandLine).addValue(argument, "value2");
        verify(commandLine).addValue(argument, "argument2");
    }

    /**
     * Verifies that no options are added to the parsed command line, when
     * current value looks like an option.
     */
    @Test
    public void testProcessOption() {
        CommandLine commandLine = createCommandLineMock();
        when(commandLine.getValues(argument)).thenReturn(newArrayList());

        ListIterator<String> arguments = createArguments("--option", "value1,value2", "argument2");
        argument.process(commandLine, arguments);

        verify(commandLine, never()).addValue(any(Option.class), anyString());
    }

    /**
     * Verifies that no options are added to the parsed command line, when
     * current value looks like an option.
     */
    @Test
    public void testProcessSeparatedValues() {
        argument.setMaximum(Integer.MAX_VALUE);

        CommandLine commandLine = createCommandLineMock();
        when(commandLine.getValues(argument)).thenReturn(newArrayList());

        ListIterator<String> arguments = createArguments("value1,value2", "argument2");
        argument.process(commandLine, arguments);
        argument.process(commandLine, arguments);

        verify(commandLine).addValue(argument, "value1");
        verify(commandLine).addValue(argument, "value2");
        verify(commandLine).addValue(argument, "argument2");
    }

    /**
     * Verifies that if minimum required number of values is not reached option
     * exception is thrown during post processing.
     */
    @Test(expectedExceptions = OptionException.class)
    public void testPostProcessMinimum() {
        argument.setMinimumValue(1);

        CommandLine commandLine = createCommandLineMock();
        when(commandLine.getValues(argument)).thenReturn(EMPTY_LIST);

        argument.postProcess(commandLine);
    }

    /**
     * Test that argument validates number of stored values against a maximum
     * allowed number.
     */
    @Test(expectedExceptions = OptionException.class)
    public void testPostProcessMaximum() {
        argument.setMaximumValue(1);

        CommandLine commandLine = createCommandLineMock();
        when(commandLine.getValues(argument)).thenReturn(Lists.<Object>newArrayList("value1", "value2"));

        argument.postProcess(commandLine);
    }

    @Test
    public void testHelp() {
        Set<HelpHint> hints = newHashSet(OPTIONAL, ARGUMENT_NUMBERED, ARGUMENT_BRACKETED);
        List<Help> helps = argument.help(0, hints, null);
        assertEquals(helps.size(), 1);

        String help = helps.get(0).help(hints, null);
        assertEquals(help, "[<null>]");

        argument.setHelpValues(singleton("default"));
        help = helps.get(0).help(hints, null);
        assertEquals(help, "[<default>]");
    }
}
