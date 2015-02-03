#psql -d ${SOURCE_SID} -U  ${SOURCE_USERNAME} -h ${POSTGRESQL_HOST} -f @${NUODB_MIGRATOR_ROOT}/core/src/test/resources/postgresql/nuodbtest.sql
#psql -d ${SOURCE_SID} -U  ${SOURCE_USERNAME} -h ${POSTGRESQL_HOST} -f @${NUODB_MIGRATOR_ROOT}/core/src/test/resources/postgresql/datatypes.sql

psql -d ${SOURCE_SID} -f @${NUODB_MIGRATOR_ROOT}/core/src/test/resources/postgresql/nuodbtest.sql
psql -d ${SOURCE_SID} -f @${NUODB_MIGRATOR_ROOT}/core/src/test/resources/postgresql/datatypes.sql
sleep 1

