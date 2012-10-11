package com.nuodb.tools.migration.cli;


import com.nuodb.tools.migration.cli.parse.Argument;
import com.nuodb.tools.migration.cli.parse.Group;
import com.nuodb.tools.migration.cli.parse.Option;
import com.nuodb.tools.migration.cli.parse.OptionSet;
import com.nuodb.tools.migration.cli.parse.option.ArgumentBuilder;
import com.nuodb.tools.migration.cli.parse.option.GroupBuilder;
import com.nuodb.tools.migration.cli.parse.option.OptionBuilder;
import com.nuodb.tools.migration.cli.parse.option.OptionToolkit;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class CliHandlerTest {

    CliHandler handler;
    OptionToolkit optionToolkitMock;

    @Before
    public void setUp() throws Exception {
        handler = new CliHandler();
        optionToolkitMock = mock(OptionToolkit.class);

    }

    @Test
    public void testCreateOptions() throws Exception {

        OptionBuilder optionBuilder = mock(OptionBuilder.class);
        ArgumentBuilder argumentBuilder = mock(ArgumentBuilder.class);
        GroupBuilder groupBuilder = mock(GroupBuilder.class);


        when(optionToolkitMock.newOption()).thenReturn(optionBuilder);
        when(optionBuilder.withId(anyInt())).thenReturn(optionBuilder);
        when(optionBuilder.withName(anyString())).thenReturn(optionBuilder);
        when(optionBuilder.withDescription(anyString())).thenReturn(optionBuilder);
        when(optionBuilder.withRequired(anyBoolean())).thenReturn(optionBuilder);
        when(optionBuilder.withAlias(anyString())).thenReturn(optionBuilder);
        when(optionBuilder.withArgument(any(Argument.class))).thenReturn(optionBuilder);

        when(optionToolkitMock.newArgument()).thenReturn(argumentBuilder);
        when(argumentBuilder.withId(anyInt())).thenReturn(argumentBuilder);
        when(argumentBuilder.withMaximum(anyInt())).thenReturn(argumentBuilder);
        when(argumentBuilder.withMinimum(anyInt())).thenReturn(argumentBuilder);
        when(argumentBuilder.withName(anyString())).thenReturn(argumentBuilder);
        when(argumentBuilder.withRequired(anyBoolean())).thenReturn(argumentBuilder);

        when(optionToolkitMock.newGroup()).thenReturn(groupBuilder);
        when(groupBuilder.withId(anyInt())).thenReturn(groupBuilder);
        when(groupBuilder.withOption(any(Option.class))).thenReturn(groupBuilder);
        when(groupBuilder.withName(anyString())).thenReturn(groupBuilder);
        when(groupBuilder.withRequired(anyBoolean())).thenReturn(groupBuilder);


        final Group option = handler.createOption(optionToolkitMock);
        verify(optionToolkitMock, times(2)).newArgument();
        verify(optionToolkitMock, times(3)).newOption();
        verify(optionToolkitMock, times(1)).newGroup();

        verify(optionBuilder, times(1)).withId(CliHandler.CONFIG_OPTION_ID);
        verify(optionBuilder, times(1)).withId(CliHandler.LIST_OPTION_ID);
        verify(optionBuilder, times(3)).build();
        verify(argumentBuilder, times(2)).build();
        verify(groupBuilder, times(1)).build();


    }

    @Test
    public void testHandleOptionSet() throws Exception {
        OptionSet optionSet = mock(OptionSet.class);
        Option option = mock(Option.class);
        handler.handleOptionSet(optionSet, option);
        verify(optionSet).hasOption(CliHandler.HELP_OPTION);
        verify(optionSet).hasOption(CliHandler.COMMAND_OPTION);
        verify(optionSet).hasOption(CliHandler.CONFIG_OPTION);
    }
}
