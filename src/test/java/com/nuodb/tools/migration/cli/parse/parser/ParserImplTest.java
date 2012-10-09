package com.nuodb.tools.migration.cli.parse.parser;

import com.nuodb.tools.migration.TestUtils;
import com.nuodb.tools.migration.cli.parse.Group;
import com.nuodb.tools.migration.cli.parse.OptionException;
import com.nuodb.tools.migration.cli.parse.Parser;
import com.nuodb.tools.migration.cli.parse.option.Property;
import org.junit.Before;
import org.junit.Test;

public class ParserImplTest {
    Parser parser = new ParserImpl();
    Group build;

    @Before
    public void setUp() throws Exception {
        parser = new ParserImpl();


    }


    @Test
    public void testParseGroup() {
        parser.parse(TestUtils.testArguments, build);
    }

    @Test(expected = OptionException.class)
    public void testParseError() throws Exception {
        parser.parse(TestUtils.testArguments, new Property());
    }
}
