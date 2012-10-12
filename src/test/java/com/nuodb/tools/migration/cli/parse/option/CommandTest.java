package com.nuodb.tools.migration.cli.parse.option;

import com.nuodb.tools.migration.cli.parse.CommandLine;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public class CommandTest {

    private Command command;

    @Before
    public void setUp() throws Exception {
        command = new Command(1, "TEST_NAME", "DESC", true);
    }


}
