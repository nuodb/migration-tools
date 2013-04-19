package com.nuodb.migrator;

import com.nuodb.migrator.spec.DriverConnectionSpec;

public class TestUtils {

    public static DriverConnectionSpec createConnectionSpec() {
        final DriverConnectionSpec connectionSpec = new DriverConnectionSpec();
        connectionSpec.setUrl("jdbc:com.nuodb://localhost/test");
        connectionSpec.setUsername("dba");
        connectionSpec.setPassword("goalie");
        connectionSpec.setDriverClassName("com.nuodb.jdbc.Driver");
        return connectionSpec;
    }
}
