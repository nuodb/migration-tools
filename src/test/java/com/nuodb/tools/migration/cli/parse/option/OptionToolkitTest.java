package com.nuodb.tools.migration.cli.parse.option;

import com.nuodb.tools.migration.cli.parse.Argument;
import com.nuodb.tools.migration.cli.parse.Group;
import com.nuodb.tools.migration.cli.parse.Option;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class OptionToolkitTest {

    private OptionToolkit toolkit;
    private OptionFormat optionFormat;

    private final int testID = 1;
    private final int testMin = 0;
    private final int testMax = 1;


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
        final ArgumentBuilder argumentBuilder = toolkit.newArgument();
        argumentBuilder.withMaximum(testMax).withMinimum(testMin).withId(testID);
        final Argument argument = argumentBuilder.build();

        verify(optionFormat, times(1)).getArgumentValuesSeparator();
        Assert.assertEquals(argument.getId(), testID);
        Assert.assertEquals(argument.getMinimum(), testMin);
        Assert.assertEquals(argument.getMaximum(), testMax);
    }

    @Test
    public void testOptionBuilder() throws Exception {
        final OptionBuilder optionBuilder = toolkit.newOption();
        optionBuilder.withId(testID);
        final Option option = optionBuilder.build();

        verify(optionFormat, times(1)).getArgumentSeparator();
        verify(optionFormat, times(1)).getOptionPrefixes();
        Assert.assertEquals(option.getId(), testID);
    }

    @Test
    public void testGroupBuilder() throws Exception {
        final GroupBuilder groupBuilder = toolkit.newGroup();
        groupBuilder.withId(testID).withMaximum(testMax).withMinimum(testMin);

        final Group group = groupBuilder.build();
        Assert.assertEquals(group.getId(), testID);
        Assert.assertEquals(group.getMinimum(), testMin);
        Assert.assertEquals(group.getMaximum(), testMax);
    }
}
