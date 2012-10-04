package com.nuodb.tools.migration.cli;


import com.nuodb.tools.migration.cli.parse.Group;
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
        final String[] arguments = new String[]{
                "dump",
                "--source.driver=com.mysql.jdbc.Driver",
                "--source.url=jdbc:mysql://localhost:3306/test",
                "--source.username=root",
                "--output.type=cvs",
                "--output.path=/tmp/"};

        handler.handle(arguments);
    }
}
