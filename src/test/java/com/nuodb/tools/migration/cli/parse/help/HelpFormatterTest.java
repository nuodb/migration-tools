package com.nuodb.tools.migration.cli.parse.help;

import com.nuodb.tools.migration.cli.parse.Option;
import com.nuodb.tools.migration.cli.parse.OptionException;
import org.junit.Before;
import org.junit.Test;

import java.io.Writer;

import static org.mockito.Mockito.*;

public class HelpFormatterTest {


    private HelpFormatter formatter;

    @Before
    public void setUp() throws Exception {
        formatter = new HelpFormatter();
    }

    @Test
    public void testException() throws Exception {
        final String testException = "Test exception";
        final String divider = "/";
        final HelpFormatter spy = spy(formatter);

        final OptionException exception = mock(OptionException.class);
        when(exception.getMessage()).thenReturn(testException);
        spy.setException(exception);
        spy.setDivider(divider);

        final Writer writer = mock(Writer.class);

        spy.exception(writer);
        
        verify(spy).divider(writer);
        verify(spy).line(writer, testException);
        verify(spy).line(writer, divider);
        verify(exception).getMessage();
    }

    @Test
    public void testFooter() throws Exception {
        final String footer = "______";
        final String divider = "/";
        final HelpFormatter spy = spy(formatter);

        spy.setFooter(footer);
        spy.setDivider(divider);

        final Writer writer = mock(Writer.class);

        spy.footer(writer);

        verify(spy).line(writer, footer);
        verify(spy).line(writer, divider);
    }

    @Test
    public void testHelp() throws Exception {
        final String footer = "______";
        final String divider = "/";
        final HelpFormatter spy = spy(formatter);

        spy.setFooter(footer);
        spy.setDivider(divider);

        final OptionException exception = mock(OptionException.class);
        when(exception.getOption()).thenReturn(mock(Option.class));
        spy.setException(exception);

        final Writer writer = mock(Writer.class);

        spy.help(writer);

        verify(spy, times(2)).line(writer, divider);
        verify(exception, times(2)).getOption();
    }
}
