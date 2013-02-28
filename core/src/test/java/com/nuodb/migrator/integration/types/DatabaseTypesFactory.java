package com.nuodb.migrator.integration.types;

public class DatabaseTypesFactory {

	public DatabaseTypes getDatabaseTypes(String driverClassName) {
		if (driverClassName.toLowerCase().contains("mysql")) {
			return new MySQLTypes();
		}
		return null;
	}
}
