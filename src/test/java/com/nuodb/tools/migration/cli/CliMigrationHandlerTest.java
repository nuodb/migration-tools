package com.nuodb.tools.migration.cli;



import com.nuodb.tools.migration.TestUtils;
import com.nuodb.tools.migration.cli.parse.Group;
import com.nuodb.tools.migration.cli.parse.Option;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CliMigrationHandlerTest {

    CliMigrationHandler handler;


    @Before
    public void setUp() throws Exception {
        handler = new CliMigrationHandler();
    }

    @Test
    public void testCreateOptions() throws Exception {

        final Group option = handler.createOption();
        Assert.assertNotNull(option);

    }

    @Test
    public void testHandler() throws Exception {

        handler.handle(TestUtils.testArguments);
    }
}
