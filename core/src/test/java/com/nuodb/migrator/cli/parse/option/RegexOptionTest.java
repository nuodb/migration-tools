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

import com.nuodb.migrator.cli.parse.CommandLine;
import com.nuodb.migrator.cli.parse.RegexOption;
import com.nuodb.migrator.cli.parse.Trigger;
import com.nuodb.migrator.match.AntRegexCompiler;
import com.nuodb.migrator.match.RegexCompiler;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.ListIterator;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.cli.parse.option.OptionFactory.createArguments;
import static com.nuodb.migrator.cli.parse.option.OptionFactory.createRegexOptionSpy;
import static com.nuodb.migrator.utils.Priority.NORMAL;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertTrue;

/**
 * @author Sergey Bushik
 */
public class RegexOptionTest {

    private RegexOption regexOption;

    @BeforeMethod
    public void setUp() {
        regexOption = createRegexOptionSpy();
        regexOption.setName("regex");
    }

    @DataProvider(name = "addRegex")
    public Object[][] createAddRegexData() {
        RegexCompiler regexCompiler = AntRegexCompiler.INSTANCE;
        return new Object[][] { { "option.*", new RegexTrigger(regexCompiler.compile("--option.*")) },
                { "*.option", new RegexTrigger(regexCompiler.compile("--*.option")) } };
    }

    @Test(dataProvider = "addRegex")
    public void testAddRegex(String regex, Trigger trigger) {
        regexOption.addRegex(regex, 1, NORMAL);

        assertTrue(regexOption.getTriggers().contains(trigger));
    }

    @DataProvider(name = "canProcess")
    public Object[][] createCanProcessData() {
        return new Object[][] { { "option.*", "--option.name" }, { "option.*", "--option.comma.name" },
                { "option.*", "--option.?*" }, { "option.*", "--option.option" } };
    }

    @Test(groups = "cli.parse.option.regexOption.canProcess", dataProvider = "canProcess")
    public void testCanProcess(String regex, String argument) {
        regexOption.addRegex(regex, 1, NORMAL);

        CommandLine commandLine = OptionFactory.createCommandLineMock();
        assertTrue(regexOption.canProcess(commandLine, createArguments(argument)),
                "Regex option is expected to be able to process the argument");
    }

    @DataProvider(name = "process")
    public Object[][] createProcessData() {
        return new Object[][] { { "option.*", newArrayList("--option.name1=value1", "--option.name2=value2"),
                newArrayList("name1", "value1", "name2", "value2") } };
    }

    @Test(dependsOnGroups = "cli.parse.option.regexOption.canProcess", dataProvider = "process")
    public void testProcess(String regex, List<String> arguments, List<String> expected) {
        regexOption.setArgument(new ArgumentImpl());
        regexOption.addRegex(regex, 1, NORMAL);

        CommandLine commandLine = OptionFactory.createCommandLineMock();

        ListIterator<String> iterator = arguments.listIterator();
        while (regexOption.canProcess(commandLine, iterator)) {
            regexOption.preProcess(commandLine, iterator);
            regexOption.process(commandLine, iterator);
        }
        verify(commandLine, atLeastOnce()).addOption(regexOption);

        for (String value : expected) {
            verify(commandLine).addValue(regexOption, value);
        }
    }
}
