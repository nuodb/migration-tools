#!/bin/bash

set -e
set -o pipefail

mvn -f ${BASEDIR}/../pom.xml clean >> ${WORK_FOLDER}/run.log 2>&1
mvn -f ${BASEDIR}/../pom.xml -Pmysql-integration-tests test  >> ${WORK_FOLDER}/run.log 2>&1

