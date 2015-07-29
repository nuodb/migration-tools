echo "Running schema command"
${NUODB_MIGRATOR_HOME}/bin/nuodb-migrator schema --source.driver=${SOURCE_DRIVER} --source.url=${SOURCE_URL}  --source.catalog=${SOURCE_CATALOG} --source.username=${SOURCE_USERNAME} --source.password=${SOURCE_PASSWORD}  --output.path=${WORK_DIR}/dump/dump.sql

echo "Running dump command"
${NUODB_MIGRATOR_HOME}/bin/nuodb-migrator dump --time.zone=EST --source.driver=${SOURCE_DRIVER} --source.url=${SOURCE_URL}  --source.catalog=${SOURCE_CATALOG} --source.username=${SOURCE_USERNAME} --source.password=${SOURCE_PASSWORD}   --output.type=${FORMAT} --output.path=${WORK_DIR}/dump/dump.cat

echo "Loading schema"
docker exec nuodb-cdmt-cont /opt/nuodb/bin/nuosql ${NUODB_DATABASE}@${NUODB_SERVER} --user ${NUODB_USERNAME} --password ${NUODB_PASSWORD} --schema ${NUODB_SCHEMA} < ${WORK_DIR}/dump/dump.sql

echo "Running load command"
${NUODB_MIGRATOR_HOME}/bin/nuodb-migrator load --time.zone=EST --target.url=${NUODB_URL} --target.username=${NUODB_USERNAME} --target.password=${NUODB_PASSWORD} --target.schema ${NUODB_SCHEMA} --input.path=${WORK_DIR}/dump/dump.cat
