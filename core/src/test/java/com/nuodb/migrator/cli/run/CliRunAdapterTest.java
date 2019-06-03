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

import com.nuodb.migrator.cli.parse.*;
import com.nuodb.migrator.cli.parse.parser.ParserImpl;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ListIterator;
import java.util.Map;

import static org.mockito.Mockito.*;

/**
 * @author Sergey Bushik
 */
public class CliRunAdapterTest {

    private Parser parser;
    private CliRunAdapter cliRunAdapter;

    @BeforeMethod
    public void setUp() {
        parser = spy(new ParserImpl());
        cliRunAdapter = spy(new CliRunAdapter() {
            @Override
            protected Option createOption() {
                return mock(Option.class);
            }

            @Override
            public void execute() {
            }

            @Override
            public void execute(Map<Object, Object> context) {
            }
        });
    }

    @Test
    public void testBind() {
        Option option = cliRunAdapter.getOption();
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                ListIterator<String> arguments = (ListIterator<String>) invocation.getArguments()[1];
                return arguments.hasNext();
            }
        }).when(option).canProcess(any(CommandLine.class), any(ListIterator.class));
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                ListIterator<String> arguments = (ListIterator<String>) invocation.getArguments()[1];
                arguments.remove();
                return null;
            }
        }).when(option).process(any(CommandLine.class), any(ListIterator.class));

        OptionSet optionSet = parser.parse(new String[] { "--option" }, cliRunAdapter);
        verify(cliRunAdapter).bind(optionSet);
    }
}
