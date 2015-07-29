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
