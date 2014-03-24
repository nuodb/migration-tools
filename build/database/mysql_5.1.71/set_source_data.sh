mysql -u${SOURCE_USERNAME} -e 'drop database nuodbtest;' >> ${WORK_FOLDER}/run.log 2>&1
mysql -u${SOURCE_USERNAME} -e 'create database nuodbtest;' >> ${WORK_FOLDER}/run.log 2>&1
mysql -u${SOURCE_USERNAME} nuodbtest < ${NUODB_MIGRATION_SOURCE}/core/src/test/resources/mysql/nuodbtest.sql  >> ${WORK_FOLDER}/run.log 2>&1
mysql -u${SOURCE_USERNAME} nuodbtest < ${NUODB_MIGRATION_SOURCE}/core/src/test/resources/mysql/precision.sql  >> ${WORK_FOLDER}/run.log 2>&1
sleep 1

