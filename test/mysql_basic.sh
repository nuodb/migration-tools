#!/bin/bash -e
. ./test/.travis_env
mysql -u$SOURCE_USERNAME -e 'drop database if exists nuodbtest;'
mysql -u$SOURCE_USERNAME -e 'create database nuodbtest;'
mysql -u$SOURCE_USERNAME nuodbtest < core/src/test/resources/mysql/nuodbtest.sql
mysql -u$SOURCE_USERNAME nuodbtest < core/src/test/resources/mysql/precision.sql
nuodrop ${NUODB_SCHEMA}
${NUODB_MIGRATOR_HOME}/bin/nuodb-migrator dump --time.zone=EST --source.driver=${SOURCE_DRIVER} --source.url=${SOURCE_URL}  --source.catalog=nuodbtest --source.username=${SOURCE_USERNAME} --output.type=bson --output.path=/var/tmp/dump.cat
${NUODB_MIGRATOR_HOME}/bin/nuodb-migrator load --time.zone=EST --target.url=${NUODB_URL} --target.username=${NUODB_USERNAME} --target.password=${NUODB_PASSWORD} --input.path=/var/tmp/dump.cat
mvn -Pmysql-integration-tests test