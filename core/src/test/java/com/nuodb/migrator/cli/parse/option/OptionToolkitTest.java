package com.nuodb.migrator.cli.parse.option;

import com.nuodb.migrator.cli.parse.Argument;
import com.nuodb.migrator.cli.parse.BasicOption;
import com.nuodb.migrator.cli.parse.Group;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * Performs functional testing of the CLI option toolkit.
 *
 * @author Sergey Bushik
 */
public class OptionToolkitTest {

    private OptionToolkit optionToolkit;

    private static final int OPTION_ID = 1;
    private static final int OPTION_MINIMUM = 0;
    private static final int OPTION_MAXIMUM = 1;

    @BeforeMethod
    public void setUp() throws Exception {
        optionToolkit = spy(OptionToolkit.getInstance());
    }

    @Test
    public void testNewBuilder() throws Exception {
        assertNotNull(optionToolkit.newArgumentBuilder());
        assertNotNull(optionToolkit.newBasicOptionBuilder());
        assertNotNull(optionToolkit.newGroupBuilder());
        assertNotNull(optionToolkit.newRegexOptionBuilder());
    }

    @Test
    public void testBuildArgument() throws Exception {
        Argument argument = optionToolkit.newArgumentBuilder().withId(OPTION_ID).withMinimum(OPTION_MINIMUM)
                .withMaximum(OPTION_MAXIMUM).build();

        assertNotNull(argument);
        assertEquals(argument.getId(), OPTION_ID);
        assertEquals(argument.getMinimum(), OPTION_MINIMUM);
        assertEquals(argument.getMaximum(), OPTION_MAXIMUM);
    }

    @Test
    public void testBuildOption() throws Exception {
        BasicOption option = optionToolkit.newBasicOptionBuilder().withId(OPTION_ID).build();

        assertNotNull(option);
        assertEquals(option.getId(), OPTION_ID);
    }

    @Test
    public void testBuildGroup() throws Exception {
        Group group = optionToolkit.newGroupBuilder().withId(OPTION_ID).withMaximum(OPTION_MAXIMUM)
                .withMinimum(OPTION_MINIMUM).build();

        assertNotNull(group);
        assertEquals(group.getId(), OPTION_ID);
        assertEquals(group.getMinimum(), OPTION_MINIMUM);
        assertEquals(group.getMaximum(), OPTION_MAXIMUM);
    }
}
