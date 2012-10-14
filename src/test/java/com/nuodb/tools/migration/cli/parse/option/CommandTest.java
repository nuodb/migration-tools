package com.nuodb.tools.migration.cli.parse.option;

import org.junit.Before;

public class CommandTest {

    private Command command;

    @Before
    public void setUp() throws Exception {
        command = new Command(1, "TEST_NAME", "DESC", true);
    }
}
