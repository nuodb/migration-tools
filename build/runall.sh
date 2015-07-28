#!/bin/bash

set -e
set -o pipefail

BASE_DIR=$(dirname $0)

if [ ! -d "${BASE_DIR}" ]; then
      echo "The JDBC_DRIVERS environment variable is not defined correctly"
      echo "NB: JDBC_DRIVERS should point to a directory containing the jdbc jar files used by this script"
      exit 1
fi

echo "Setup Target NuoDB Database Docker ";
. ${BASE_DIR}/docker/nuodb_docker.sh

echo "Setup MySQL Docker"
. ${BASE_DIR}/docker/docker-mysql-5.1/mysql_docker.sh

# Geting mysql jar from maven repo
wget  -O ../core/mysql.jar http://central.maven.org/maven2/mysql/mysql-connector-java/5.1.35/mysql-connector-java-5.1.35.jar

${BASE_DIR}/run.sh bson --database mysql_5.1.71 --driver ../core/mysql.jar
${BASE_DIR}/run.sh xml --database mysql_5.1.71 --driver  ../core/mysql.jar
${BASE_DIR}/run.sh csv --database mysql_5.1.71 --driver  ../core/mysql.jar
