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

import com.nuodb.migrator.cli.parse.BasicOption;
import com.nuodb.migrator.cli.parse.CommandLine;
import com.nuodb.migrator.cli.parse.Help;
import com.nuodb.migrator.cli.parse.HelpHint;
import com.nuodb.migrator.cli.parse.OptionException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static com.nuodb.migrator.cli.parse.HelpHint.*;
import static com.nuodb.migrator.cli.parse.option.OptionFormat.SHORT;
import static com.nuodb.migrator.cli.parse.option.OptionFactory.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

/**
 * Verifies that basic option is able to process arguments being triggered by
 * it's name or aliases.
 *
 * @author Sergey Bushik
 */
public class BasicOptionTest {

    private static final String NAME = "option";
    private static final String ALIAS = "o";

    private BasicOption basicOption;

    @BeforeMethod
    public void setUp() {
        basicOption = createBasicOptionSpy();
        basicOption.setName(NAME);
        basicOption.setOptionFormat(OptionFormat.LONG);
    }

    @Test(groups = "cli.parse.option.BasicOption.canProcess")
    public void testCanProcessByName() {
        CommandLine commandLine = createCommandLineMock();

        assertTrue(basicOption.canProcess(commandLine, createArguments("--option")),
                "Option should be processable by name");
        assertFalse(basicOption.canProcess(commandLine, createArguments("--non-option")),
                "Unexpected option can't be processed by name");
    }

    @Test(groups = "cli.parse.option.BasicOption.canProcess")
    public void testCanProcessByAlias() {
        basicOption.addAlias(ALIAS, SHORT);

        CommandLine commandLine = createCommandLineMock();
        assertTrue(basicOption.canProcess(commandLine, createArguments("-o")), "Option should be processable by alias");
        assertFalse(basicOption.canProcess(commandLine, createArguments("-non-o")),
                "Unexpected argument can't be processed by alias");
    }

    @Test(dependsOnGroups = "cli.parse.option.BasicOption.canProcess")
    public void testProcessByName() {
        basicOption.addAlias(ALIAS, SHORT);

        CommandLine commandLine = createCommandLineMock();
        basicOption.process(commandLine, createArguments("--option"));
        verify(commandLine).addOption(basicOption);
    }

    @Test(dependsOnGroups = "cli.parse.option.BasicOption.canProcess", expectedExceptions = OptionException.class)
    public void testProcessByNameUnexpected() {
        CommandLine commandLine = createCommandLineMock();
        basicOption.process(commandLine, createArguments("--non-option"));
    }

    @Test(dependsOnGroups = "cli.parse.option.BasicOption.canProcess")
    public void testProcessArgument() {
        basicOption.setArgument(createArgumentSpy());

        CommandLine commandLine = createCommandLineMock();

        ListIterator<String> arguments = createArguments("--option=argument");
        assertTrue(basicOption.canProcess(commandLine, arguments));

        basicOption.preProcess(commandLine, arguments);
        basicOption.process(commandLine, arguments);

        verify(commandLine).addValue(basicOption, "argument");
    }

    @Test
    public void testHelp() {
        Set<HelpHint> hints = newHashSet(OPTIONAL, ALIASES, AUGMENT_ARGUMENT, AUGMENT_GROUP);
        List<Help> helps = basicOption.help(0, hints, null);
        assertEquals(helps.size(), 1);

        String help = helps.get(0).help(hints, null);
        assertEquals(help, "[--option]");
    }
}
