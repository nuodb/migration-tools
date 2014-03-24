${NUODB_MIGRATION_ROOT}/bin/nuodb-migrator schema --source.driver=${SOURCE_DRIVER} --source.url=${SOURCE_URL}  --source.catalog=nuodbtest --source.username=${SOURCE_USERNAME} --output.path=${WORK_FOLDER}/dump/dump.sql  >> ${WORK_FOLDER}/run.log 2>&1

${NUODB_MIGRATION_ROOT}/bin/nuodb-migrator dump --time.zone=EST --source.driver=${SOURCE_DRIVER} --source.url=${SOURCE_URL}  --source.catalog=nuodbtest --source.username=${SOURCE_USERNAME} --output.type=${ARG_FORMAT} --output.path=${WORK_FOLDER}/dump/dump.cat  >> ${WORK_FOLDER}/run.log 2>&1

${NUODB_ROOT}/bin/nuosql test@localhost --user ${NUODB_USERNAME} --password ${NUODB_PASSWORD} --schema ${NUODB_SCHEMA} < ${WORK_FOLDER}/dump/dump.sql  >> ${WORK_FOLDER}/run.log 2>&1

${NUODB_MIGRATION_ROOT}/bin/nuodb-migrator load --time.zone=EST --target.url=${NUODB_URL} --target.username=${NUODB_USERNAME} --target.password=${NUODB_PASSWORD} --target.schema ${NUODB_SCHEMA} --input.path=${WORK_FOLDER}/dump/dump.cat  >> ${WORK_FOLDER}/run.log 2>&1

