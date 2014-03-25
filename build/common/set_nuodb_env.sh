export NUODB_ROOT=/opt/nuodb
export NUODB_DRIVER=com.nuodb.jdbc.Driver
export NUODB_URL=jdbc:com.nuodb://localhost/test
export NUODB_USERNAME=dba
export NUODB_PASSWORD=goalie
export NUODB_SCHEMA=HOCKEY
export NUODB_JDBCJAR=/opt/nuodb/jar/nuodbjdbc.jar
export CLASSPATH=${SOURCE_JDBCJAR}:${NUODB_JDBCJAR}

