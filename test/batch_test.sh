#!/bin/sh -e
. ./test/.travis_env
mysql -u$SOURCE_USERNAME -e 'drop database if exists nuodbtest;'
mysql -u$SOURCE_USERNAME -e 'create database nuodbtest;'
mysql -u$SOURCE_USERNAME nuodbtest < core/src/test/resources/mysql/nuodbtest.sql
mysql -u$SOURCE_USERNAME nuodbtest < core/src/test/resources/mysql/precision.sql
mysql -u$SOURCE_USERNAME nuodbtest < core/src/test/resources/mysql/datatypes.sql
${NUODB_MIGRATOR_HOME}/bin/nuodb-migrator dump --time.zone=EST --source.driver=${SOURCE_DRIVER} --source.url=${SOURCE_URL}  --source.catalog=nuodbtest --source.username=${SOURCE_USERNAME} --output.type=bson --output.path=/var/tmp/dump.cat

if [ "$TABLE_LOCK" = "true" ]; then
    echo "Enforcing table locks"
    enforceLocks
fi

load()
{
    ${NUODB_MIGRATOR_HOME}/bin/nuodb-migrator load --time.zone=EST --target.url=${NUODB_URL} --target.username=${NUODB_USERNAME} --target.password=${NUODB_PASSWORD} --input.path=/var/tmp/dump.cat $@
}

# test default commit strategy
nuodrop ${NUODB_SCHEMA}
load
mvn -Pmysql-integration-tests test

# test single commit strategy
nuodrop ${NUODB_SCHEMA}
load --commit.strategy=single
mvn -Pmysql-integration-tests test

# test batch commit strategy
nuodrop ${NUODB_SCHEMA}
load --commit.strategy=batch
mvn -Pmysql-integration-tests test

# test batch 1 commit strategy
nuodrop ${NUODB_SCHEMA}
load --commit.strategy=batch --commit.batch.size=1
mvn -Pmysql-integration-tests test
