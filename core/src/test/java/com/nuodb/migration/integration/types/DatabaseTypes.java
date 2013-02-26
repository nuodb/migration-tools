package com.nuodb.migration.integration.types;

public interface DatabaseTypes {

	public JDBCGetMethod[] getJDBCTypes(String type);

	public boolean isCaseSensitive();

}
