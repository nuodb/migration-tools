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

    private final int TEST_ID = 1;
    private final int TEST_MIN = 0;
    private final int TEST_MAX = 1;


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
        argumentBuilder.withMaximum(TEST_MAX).withMinimum(TEST_MIN).withId(TEST_ID);
        final Argument argument = argumentBuilder.build();

        verify(optionFormat, times(1)).getArgumentValuesSeparator();
        Assert.assertEquals(argument.getId(), TEST_ID);
        Assert.assertEquals(argument.getMinimum(), TEST_MIN);
        Assert.assertEquals(argument.getMaximum(), TEST_MAX);
    }

    @Test
    public void testOptionBuilder() throws Exception {
        final OptionBuilder optionBuilder = toolkit.newOption();
        optionBuilder.withId(TEST_ID);
        final Option option = optionBuilder.build();

        verify(optionFormat, times(1)).getArgumentSeparator();
        verify(optionFormat, times(1)).getOptionPrefixes();
        Assert.assertEquals(option.getId(), TEST_ID);
    }

    @Test
    public void testGroupBuilder() throws Exception {
        final GroupBuilder groupBuilder = toolkit.newGroup();
        groupBuilder.withId(TEST_ID).withMaximum(TEST_MAX).withMinimum(TEST_MIN);

        final Group group = groupBuilder.build();
        Assert.assertEquals(group.getId(), TEST_ID);
        Assert.assertEquals(group.getMinimum(), TEST_MIN);
        Assert.assertEquals(group.getMaximum(), TEST_MAX);
    }
}
