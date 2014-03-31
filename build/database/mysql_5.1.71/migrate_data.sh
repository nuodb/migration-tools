#!/bin/bash

set -e
set -o pipefail

${NUODB_MIGRATION_ROOT}/bin/nuodb-migrator schema --source.driver=${SOURCE_DRIVER} --source.url=${SOURCE_URL}  --source.catalog=${SOURCE_CATALOG} --source.username=${SOURCE_USERNAME} --output.path=${WORK_FOLDER}/dump/dump.sql  >> ${WORK_FOLDER}/run.log 2>&1

echo "Schema dumped" >>  ${WORK_FOLDER}/run.log

${NUODB_MIGRATION_ROOT}/bin/nuodb-migrator dump --time.zone=EST --source.driver=${SOURCE_DRIVER} --source.url=${SOURCE_URL}  --source.catalog=${SOURCE_CATALOG} --source.username=${SOURCE_USERNAME} --output.type=${ARG_FORMAT} --output.path=${WORK_FOLDER}/dump/dump.cat  >> ${WORK_FOLDER}/run.log 2>&1

echo "Data dumped" >>  ${WORK_FOLDER}/run.log

${NUODB_HOME}/bin/nuosql ${NUODB_DATABASE}@${NUODB_SERVER} --user ${NUODB_USERNAME} --password ${NUODB_PASSWORD} --schema ${NUODB_SCHEMA} < ${WORK_FOLDER}/dump/dump.sql  >> ${WORK_FOLDER}/run.log 2>&1

echo "Schema created on nuodb" >>  ${WORK_FOLDER}/run.log

${NUODB_MIGRATION_ROOT}/bin/nuodb-migrator load --time.zone=EST --target.url=${NUODB_URL} --target.username=${NUODB_USERNAME} --target.password=${NUODB_PASSWORD} --target.schema ${NUODB_SCHEMA} --input.path=${WORK_FOLDER}/dump/dump.cat  >> ${WORK_FOLDER}/run.log 2>&1

echo "Data loaded into nuodb" >>  ${WORK_FOLDER}/run.log
