#!/bin/bash

START_TS=`date +%Y%m%d_%H%M%S`
START_TS="20140321_004922"
export START_TS
export START_SOURCE_FOLDER=/migrator-local/work/${START_TS}/source/migration-tools
if [ ! -d $START_SOURCE_FOLDER ]
  then
    mkdir -p ${START_SOURCE_FOLDER}
    git clone git://github.com/nuodb/migration-tools.git ${START_SOURCE_FOLDER}
fi

./run.sh bson --database mysql_5.1.71 --driver mysql-connector-java-5.1.28-bin.jar --jdk jdk1.6.0_45 --migrator_from_git
./run.sh xml --database mysql_5.1.71 --driver mysql-connector-java-5.1.28-bin.jar --jdk jdk1.6.0_45 --migrator_from_git
./run.sh csv --database mysql_5.1.71 --driver mysql-connector-java-5.1.28-bin.jar --jdk jdk1.6.0_45 --migrator_from_git

./run.sh bson --database mysql_5.1.71 --driver mysql-connector-java-5.1.28-bin.jar --jdk jdk1.7.0_51 --migrator_from_git
./run.sh xml --database mysql_5.1.71 --driver mysql-connector-java-5.1.28-bin.jar --jdk jdk1.7.0_51 --migrator_from_git
./run.sh csv --database mysql_5.1.71 --driver mysql-connector-java-5.1.28-bin.jar --jdk jdk1.7.0_51 --migrator_from_git

./run.sh bson --database mysql_5.1.71 --driver mysql-connector-java-5.1.28-bin.jar --jdk openjdk-1.7.0.51 --migrator_from_git
./run.sh xml --database mysql_5.1.71 --driver mysql-connector-java-5.1.28-bin.jar --jdk openjdk-1.7.0.51 --migrator_from_git
./run.sh csv --database mysql_5.1.71 --driver mysql-connector-java-5.1.28-bin.jar --jdk openjdk-1.7.0.51 --migrator_from_git
