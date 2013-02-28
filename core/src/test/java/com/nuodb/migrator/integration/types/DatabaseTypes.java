package com.nuodb.migrator.integration.types;

public interface DatabaseTypes {

	public JDBCGetMethod[] getJDBCTypes(String type);

	public boolean isCaseSensitive();

}
