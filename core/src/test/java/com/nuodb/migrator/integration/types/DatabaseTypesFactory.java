package com.nuodb.migrator.integration.types;

public class DatabaseTypesFactory {

    public DatabaseTypes getDatabaseTypes(String driverClassName) {
        if (driverClassName.toLowerCase().contains("mysql")) {
            return new MySQLTypes();
        } else if (driverClassName.toLowerCase().contains("nuodb")) {
            return new NuoDBTypes();
        } else if (driverClassName.toLowerCase().contains("jtds")) {
            return new SQLServerTypes();
        } else if (driverClassName.toLowerCase().contains("postgresql")) {
            return new PostgreSQLTypes();
        } else if (driverClassName.toLowerCase().contains("oracle")) {
            return new OracleTypes();
        }
        return null;
    }
}
