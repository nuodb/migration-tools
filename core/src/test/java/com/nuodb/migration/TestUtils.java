package com.nuodb.migration;

import com.nuodb.migration.spec.JdbcConnectionSpec;

public class TestUtils {

    public static JdbcConnectionSpec createConnectionSpec() {
        final JdbcConnectionSpec connectionSpec = new JdbcConnectionSpec();
        connectionSpec.setUrl("jdbc:com.nuodb://localhost/test");
        connectionSpec.setUsername("dba");
        connectionSpec.setPassword("goalie");
        connectionSpec.setDriverClassName("com.nuodb.jdbc.Driver");
        return connectionSpec;
    }
}
