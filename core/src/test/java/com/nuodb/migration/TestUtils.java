package com.nuodb.migration;

import com.nuodb.migration.spec.DriverConnectionSpec;

public class TestUtils {

    public static DriverConnectionSpec createTestNuoDBConnectionSpec() {
        final DriverConnectionSpec connectionSpec = new DriverConnectionSpec();
        connectionSpec.setUrl("jdbc:com.nuodb://localhost/test");
        connectionSpec.setUsername("dba");
        connectionSpec.setPassword("goalie");
        connectionSpec.setDriverClassName("com.nuodb.jdbc.Driver");
        return connectionSpec;
    }
}
