#!/bin/bash

set -e
set -o pipefail

BASE_DIR=$(dirname $0)

if [ ! -d "$JDBC_DRIVERS" ]; then
      echo "The JDBC_DRIVERS environment variable is not defined correctly"
      echo "NB: JDBC_DRIVERS should point to a directory containing the jdbc jar files used by this script"
      exit 1
fi

${BASE_DIR}/run.sh bson --database mysql_5.1.71 --driver ${JDBC_DRIVERS}/mysql/mysql-connector-java-5.1.28-bin.jar
${BASE_DIR}/run.sh xml --database mysql_5.1.71 --driver ${JDBC_DRIVERS}/mysql/mysql-connector-java-5.1.28-bin.jar
${BASE_DIR}/run.sh csv --database mysql_5.1.71 --driver ${JDBC_DRIVERS}/mysql/mysql-connector-java-5.1.28-bin.jar

${BASE_DIR}/run.sh bson --database oracle_11.2.0.2.0 --driver ${JDBC_DRIVERS}/oracle/ojdbc6.jar
${BASE_DIR}/run.sh xml --database oracle_11.2.0.2.0 --driver ${JDBC_DRIVERS}/oracle/ojdbc6.jar
${BASE_DIR}/run.sh csv --database oracle_11.2.0.2.0 --driver ${JDBC_DRIVERS}/oracle/ojdbc6.jar

${BASE_DIR}/run.sh bson --database postgresql-9.2-1002 --driver ${JDBC_DRIVERS}/postgresql/postgresql-9.2-1002.jdbc4.jar
${BASE_DIR}/run.sh xml --database postgresql-9.2-1002 --driver ${JDBC_DRIVERS}/postgresql/postgresql-9.2-1002.jdbc4.jar
${BASE_DIR}/run.sh csv --database postgresql-9.2-1002 --driver ${JDBC_DRIVERS}/postgresql/postgresql-9.2-1002.jdbc4.jar

${BASE_DIR}/run.sh bson --database db2_1.0 --driver ${JDBC_DRIVERS}/db2_1.0/db2jcc.jar
${BASE_DIR}/run.sh xml --database db2_1.0 --driver ${JDBC_DRIVERS}/db2_1.0/db2jcc.jar
${BASE_DIR}/run.sh csv --database db2_1.0 --driver ${JDBC_DRIVERS}/db2_1.0/db2jcc.jar

