NUODB_SERVER="${NUODB_SERVER:-localhost}"
NUODB_DATABASE="${NUODB_DATABASE:-test}"
export NUODB_HOME
export NUODB_DRIVER=com.nuodb.jdbc.Driver
export NUODB_URL=jdbc:com.nuodb://${NUODB_SERVER}:28004/${NUODB_DATABASE}
export NUODB_USERNAME=test
export NUODB_PASSWORD=test
export NUODB_SCHEMA=test
export NUODB_JDBCJAR=${NUODB_MIGRATOR_ROOT}/build/common/nuodbjdbc.jar
export SOURCE_JDBCJAR=../core/mysql.jar
export CLASSPATH=${SOURCE_JDBCJAR}:${NUODB_JDBCJAR}

