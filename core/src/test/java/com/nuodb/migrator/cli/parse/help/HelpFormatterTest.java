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
package com.nuodb.migrator.cli.parse.help;

import com.nuodb.migrator.cli.parse.Option;
import com.nuodb.migrator.cli.parse.OptionException;
import com.nuodb.migrator.utils.ValidationException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.Writer;

import static com.nuodb.migrator.cli.parse.help.HelpFormatter.*;
import static org.mockito.Mockito.*;

/**
 * Verifies proper help formatting for showing on the command line.
 *
 * @author Sergey Bushik
 */
public class HelpFormatterTest {

    private HelpFormatter helpFormatter;

    @BeforeMethod
    public void setUp() {
        helpFormatter = spy(new HelpFormatter());
        helpFormatter.setUsageOutputHints(USAGE_OUTPUT_HINTS);
        helpFormatter.setHelpOutputHints(HELP_OUTPUT_HINTS);
        helpFormatter.setOptionOutputHints(OPTION_OUTPUT_HINTS);
    }

    @Test
    public void testFormat() throws IOException {
        Option option = mock(Option.class);
        helpFormatter.setOption(option);

        Writer writer = mock(Writer.class);
        helpFormatter.format(writer);

        verify(helpFormatter).header(writer);
        verify(helpFormatter).exception(writer);
        verify(helpFormatter).usage(writer);
        verify(helpFormatter).help(writer);
        verify(helpFormatter).footer(writer);
    }

    @Test(expectedExceptions = ValidationException.class)
    public void testUsageNoOption() throws IOException {
        Writer writer = mock(Writer.class);
        helpFormatter.usage(writer);
    }

    @Test
    public void testHelp() throws IOException {
        String footer = "------------";
        String divider = "\n";

        helpFormatter.setFooter(footer);
        helpFormatter.setDivider(divider);

        OptionException exception = mock(OptionException.class);
        when(exception.getOption()).thenReturn(mock(Option.class));
        helpFormatter.setException(exception);

        Writer writer = mock(Writer.class);
        helpFormatter.help(writer);

        verify(helpFormatter, times(2)).line(writer, divider);
        verify(exception, times(2)).getOption();
    }

    @Test
    public void testException() throws IOException {
        String message = "Option exception";

        OptionException exception = mock(OptionException.class);
        when(exception.getMessage()).thenReturn(message);

        helpFormatter.setException(exception);

        Writer writer = mock(Writer.class);
        helpFormatter.exception(writer);

        verify(helpFormatter).divider(writer);
        verify(helpFormatter).line(writer, message);
        verify(helpFormatter).line(writer);
        verify(exception).getMessage();
    }

    @Test
    public void testFooter() throws IOException {
        String footer = "------------";
        helpFormatter.setFooter(footer);

        Writer writer = mock(Writer.class);
        helpFormatter.footer(writer);

        verify(helpFormatter).line(writer, footer);
    }
}
