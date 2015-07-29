NUODB_SERVER="${NUODB_SERVER:-localhost}"
NUODB_DATABASE="${NUODB_DATABASE:-test}"
export NUODB_HOME
export NUODB_DRIVER=com.nuodb.jdbc.Driver
export NUODB_URL=jdbc:com.nuodb://${NUODB_SERVER}:28004/${NUODB_DATABASE}
export NUODB_USERNAME=test
export NUODB_PASSWORD=test
export NUODB_SCHEMA=test
export NUODB_JDBCJAR=${JDBC_DRIVERS}/nuodb/nuodbjdbc.jar
export SOURCE_JDBCJAR
export CLASSPATH=${SOURCE_JDBCJAR}:${NUODB_JDBCJAR}

