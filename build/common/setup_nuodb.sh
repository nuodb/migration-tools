echo -e "drop schema ${NUODB_SCHEMA} cascade;\ncreate schema ${NUODB_SCHEMA};" | ${NUODB_ROOT}/bin/nuosql test@localhost --user ${NUODB_USERNAME} --password ${NUODB_PASSWORD} >> ${WORK_FOLDER}/run.log 2>&1

