#echo -e "drop schema ${NUODB_SCHEMA} cascade;" | ${NUODB_HOME}/bin/nuosql ${NUODB_DATABASE}@${NUODB_SERVER} --user ${NUODB_USERNAME} --password ${NUODB_PASSWORD}
#echo -e "create schema ${NUODB_SCHEMA};" | ${NUODB_HOME}/bin/nuosql ${NUODB_DATABASE}@${NUODB_SERVER} --user ${NUODB_USERNAME} --password ${NUODB_PASSWORD}
