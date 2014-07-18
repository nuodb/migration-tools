mvn -f ${NUODB_MIGRATOR_ROOT}/pom.xml clean install

export JAVA_OPTS=-Dlog4j.configuration=file:///${CUR_DIR}/${BASE_DIR}/common/log4j.properties
export CLASSPATH=${SOURCE_JDBCJAR}:${NUODB_JDBCJAR}
export NUODB_MIGRATOR_HOME=${NUODB_MIGRATOR_ROOT}/assembly/target/nuodb-migrator