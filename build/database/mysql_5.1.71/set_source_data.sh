#!/bin/bash

set -e
set -o pipefail

mysql -u${SOURCE_USERNAME} -e 'drop database ${SOURCE_CATALOG};' >> ${WORK_FOLDER}/run.log 2>&1
mysql -u${SOURCE_USERNAME} -e 'create database ${SOURCE_CATALOG};' >> ${WORK_FOLDER}/run.log 2>&1
mysql -u${SOURCE_USERNAME} ${SOURCE_CATALOG} < ${NUODB_MIGRATION_SOURCE}/core/src/test/resources/mysql/nuodbtest.sql  >> ${WORK_FOLDER}/run.log 2>&1
mysql -u${SOURCE_USERNAME} ${SOURCE_CATALOG} < ${NUODB_MIGRATION_SOURCE}/core/src/test/resources/mysql/precision.sql  >> ${WORK_FOLDER}/run.log 2>&1
sleep 1

