mysql -u${SOURCE_USERNAME} -e 'drop database ${SOURCE_CATALOG};'
mysql -u${SOURCE_USERNAME} -e 'create database ${SOURCE_CATALOG};'
mysql -u${SOURCE_USERNAME} ${SOURCE_CATALOG} < ${NUODB_MIGRATION_SOURCE}/core/src/test/resources/mysql/nuodbtest.sql
mysql -u${SOURCE_USERNAME} ${SOURCE_CATALOG} < ${NUODB_MIGRATION_SOURCE}/core/src/test/resources/mysql/precision.sql
sleep 1

