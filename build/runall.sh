#!/bin/bash

set -e
set -o pipefail

BASE_DIR=$(dirname $0)

if [ ! -d "$JDBC_DRIVERS" ]; then
      echo "The JDBC_DRIVERS environment variable is not defined correctly"
      echo "NB: JDBC_DRIVERS should point to a directory containing the jdbc jar files used by this script"
      exit 1
fi

echo "Setup Target NuoDB Database Docker ";
. ${BASE_DIR}/docker/docker-nuodb/nuodb_docker.sh

echo "Setup MySQL Docker"
. ${BASE_DIR}/docker/docker-mysql/mysql_docker.sh

${BASE_DIR}/run.sh bson --database mysql_5.1.71 --driver ${JDBC_DRIVERS}/mysql/mysql-connector-java-5.1.28-bin.jar
${BASE_DIR}/run.sh xml --database mysql_5.1.71 --driver ${JDBC_DRIVERS}/mysql/mysql-connector-java-5.1.28-bin.jar
${BASE_DIR}/run.sh csv --database mysql_5.1.71 --driver ${JDBC_DRIVERS}/mysql/mysql-connector-java-5.1.28-bin.jar


echo "Setup Postgresql Docker"
. ${BASE_DIR}/docker/docker-postgresql/postgresql_docker.sh

export POSTGRESQL_PATH=${JDBC_DRIVERS}/postgresql/postgresql-9.3-1100.jdbc4.jar;

${BASE_DIR}/run.sh bson --database  postgresql --driver  ${JDBC_DRIVERS}/postgresql/postgresql-9.3-1100.jdbc4.jar
${BASE_DIR}/run.sh xml --database   postgresql --driver  ${JDBC_DRIVERS}/postgresql/postgresql-9.3-1100.jdbc3.jar
${BASE_DIR}/run.sh csv --database   postgresql --driver  ${JDBC_DRIVERS}/postgresql/postgresql-9.3-1100.jdbc3.jar

