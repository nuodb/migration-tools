#!/bin/bash

set -e
set -o pipefail

BASEDIR=$(dirname $0)

${BASEDIR}/run.sh bson --database mysql_5.1.71 --driver /home/build/migratortest/drivers/mysql/mysql-connector-java-5.1.28-bin.jar --jdk /home/build/migratortest/jdks/jdk1.6.0_45
${BASEDIR}/run.sh xml --database mysql_5.1.71 --driver /home/build/migratortest/drivers/mysql/mysql-connector-java-5.1.28-bin.jar --jdk /home/build/migratortest/jdks/jdk1.6.0_45
${BASEDIR}/run.sh csv --database mysql_5.1.71 --driver /home/build/migratortest/drivers/mysql/mysql-connector-java-5.1.28-bin.jar --jdk /home/build/migratortest/jdks/jdk1.6.0_45

${BASEDIR}/run.sh bson --database mysql_5.1.71 --driver /home/build/migratortest/drivers/mysql/mysql-connector-java-5.1.28-bin.jar --jdk /home/build/migratortest/jdks/jdk1.7.0_51
${BASEDIR}/run.sh xml --database mysql_5.1.71 --driver /home/build/migratortest/drivers/mysql/mysql-connector-java-5.1.28-bin.jar --jdk /home/build/migratortest/jdks/jdk1.7.0_51
${BASEDIR}/run.sh csv --database mysql_5.1.71 --driver /home/build/migratortest/drivers/mysql/mysql-connector-java-5.1.28-bin.jar --jdk /home/build/migratortest/jdks/jdk1.7.0_51

${BASEDIR}/run.sh bson --database mysql_5.1.71 --driver /home/build/migratortest/drivers/mysql/mysql-connector-java-5.1.28-bin.jar --jdk /home/build/migratortest/jdks/openjdk-1.7.0.51
${BASEDIR}/run.sh xml --database mysql_5.1.71 --driver /home/build/migratortest/drivers/mysql/mysql-connector-java-5.1.28-bin.jar --jdk /home/build/migratortest/jdks/openjdk-1.7.0.51
${BASEDIR}/run.sh csv --database mysql_5.1.71 --driver /home/build/migratortest/drivers/mysql/mysql-connector-java-5.1.28-bin.jar --jdk /home/build/migratortest/jdks/openjdk-1.7.0.51
