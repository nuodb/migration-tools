sqlplus ${SOURCE_PASSWORD}/${SOURCE_PASSWORD}@//${ORACLE_HOST}:1521/${SOURCE_SID} @${NUODB_MIGRATOR_ROOT}/core/src/test/resources/oracle/nuodbtest.sql
sqlplus ${SOURCE_PASSWORD}/${SOURCE_PASSWORD}@//${ORACLE_HOST}:1521/${SOURCE_SID} @${NUODB_MIGRATOR_ROOT}/core/src/test/resources/oracle/datatypes.sql
sleep 1

