package com.nuodb.migration.cli.parse.option;

import com.nuodb.migration.cli.parse.Argument;
import com.nuodb.migration.cli.parse.Group;
import com.nuodb.migration.cli.parse.Option;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class OptionToolkitTest {

    private OptionToolkit toolkit;
    private OptionFormat optionFormat;

    private final int OPTION_ID = 1;
    private final int OPTION_MINIMUM = 0;
    private final int OPTION_MAXIMUM = 1;

    @Before
    public void setUp() throws Exception {
        optionFormat = mock(OptionFormat.class);
        toolkit = new OptionToolkit(optionFormat);
    }

    @Test
    public void testNewBuilders() throws Exception {
        toolkit.newOption();
        toolkit.newArgument();
        toolkit.newGroup();
    }

    @Test
    public void testArgumentBuilder() throws Exception {
        final ArgumentBuilder builder = toolkit.newArgument();
        builder.withMaximum(OPTION_MAXIMUM).withMinimum(OPTION_MINIMUM).withId(OPTION_ID);
        final Argument argument = builder.build();

        verify(optionFormat, times(1)).getArgumentValuesSeparator();
        Assert.assertEquals(argument.getId(), OPTION_ID);
        Assert.assertEquals(argument.getMinimum(), OPTION_MINIMUM);
        Assert.assertEquals(argument.getMaximum(), OPTION_MAXIMUM);
    }

    @Test
    public void testOptionBuilder() throws Exception {
        final OptionBuilder builder = toolkit.newOption();
        builder.withId(OPTION_ID);
        final Option option = builder.build();

        verify(optionFormat, times(1)).getArgumentSeparator();
        verify(optionFormat, times(1)).getOptionPrefixes();
        Assert.assertEquals(option.getId(), OPTION_ID);
    }

    @Test
    public void testGroupBuilder() throws Exception {
        final GroupBuilder builder = toolkit.newGroup();
        builder.withId(OPTION_ID).withMaximum(OPTION_MAXIMUM).withMinimum(OPTION_MINIMUM);

        final Group group = builder.build();
        Assert.assertEquals(group.getId(), OPTION_ID);
        Assert.assertEquals(group.getMinimum(), OPTION_MINIMUM);
        Assert.assertEquals(group.getMaximum(), OPTION_MAXIMUM);
    }
}
