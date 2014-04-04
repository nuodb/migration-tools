NUODB_HOME="${NUODB_HOME:-/opt/nuodb}"
NUODB_SERVER="${NUODB_SERVER:-localhost}"
NUODB_DATABASE="${NUODB_DATABASE:-test}"
export NUODB_HOME
export NUODB_MIGRATION_ROOT=${NUODB_HOME}/tools/migrator
export NUODB_DRIVER=com.nuodb.jdbc.Driver
export NUODB_URL=jdbc:com.nuodb://${NUODB_SERVER}/${NUODB_DATABASE}
export NUODB_USERNAME=dba
export NUODB_PASSWORD=goalie
export NUODB_SCHEMA=nuodbtest
export NUODB_JDBCJAR=${NUODB_HOME}/jar/nuodbjdbc.jar
export CLASSPATH=${SOURCE_JDBCJAR}:${NUODB_JDBCJAR}

