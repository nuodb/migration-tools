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

import com.nuodb.migrator.cli.parse.*;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ListIterator;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.commons.lang3.StringUtils.startsWith;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * @author Sergey Bushik
 */
public class OptionFactory {

    public static BasicOption createBasicOptionSpy() {
        return spy(new BasicOptionImpl());
    }

    public static Argument createArgumentSpy() {
        return spy(new ArgumentImpl());
    }

    public static Group createGroupSpy() {
        return spy(new GroupImpl());
    }

    public static RegexOption createRegexOptionSpy() {
        return spy(new RegexOptionImpl());
    }

    public static CommandLine createCommandLineMock() {
        return createCommandLineMock(OptionFormat.LONG);
    }

    public static CommandLine createCommandLineMock(OptionFormat optionFormat) {
        CommandLine commandLine = mock(CommandLine.class);
        when(commandLine.isOption(anyString())).thenAnswer(new IsOptionAnswer(optionFormat));
        return commandLine;
    }

    public static ListIterator<String> createArguments(String... arguments) {
        return newArrayList(arguments).listIterator();
    }

    static class IsOptionAnswer implements Answer<Boolean> {

        private OptionFormat optionFormat;

        public IsOptionAnswer(OptionFormat optionFormat) {
            this.optionFormat = optionFormat;
        }

        @Override
        public Boolean answer(InvocationOnMock invocation) throws Throwable {
            boolean isOption = false;
            for (String prefix : optionFormat.getPrefixes()) {
                if ((isOption = startsWith((CharSequence) invocation.getArguments()[0], prefix))) {
                    break;
                }
            }
            return isOption;
        }
    }
}
