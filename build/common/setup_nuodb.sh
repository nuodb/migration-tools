#!/bin/bash

set -e
set -o pipefail

echo -e "drop schema ${NUODB_SCHEMA} cascade;\ncreate schema ${NUODB_SCHEMA};" | ${NUODB_HOME}/bin/nuosql ${NUODB_DATABASE}@${NUODB_SERVER} --user ${NUODB_USERNAME} --password ${NUODB_PASSWORD} >> ${WORK_FOLDER}/run.log 2>&1
