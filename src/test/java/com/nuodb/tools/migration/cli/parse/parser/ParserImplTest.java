package com.nuodb.tools.migration.cli.parse.parser;

import com.nuodb.tools.migration.CliConstants;
import com.nuodb.tools.migration.cli.parse.Group;
import com.nuodb.tools.migration.cli.parse.OptionException;
import com.nuodb.tools.migration.cli.parse.Parser;
import com.nuodb.tools.migration.cli.parse.option.Property;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public class ParserImplTest {

    private Parser parser = new ParserImpl();
    private Group build;

    @Before
    public void setUp() throws Exception {
        parser = new ParserImpl();
        build = mock(Group.class);

    }

    @Test(expected = OptionException.class)
    public void testParseError() throws Exception {
        parser.parse(CliConstants.ARGUMENTS, new Property());
    }
}
