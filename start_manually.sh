#!/bin/bash

export JAVA_HOME=/home/build/migratortest/jdks/jdk1.7.0_51 
export JDBC_DRIVERS=/home/build/migratortest/drivers
export  SOURCE_JDBCJAR=/home/build/migratortest/drivers

cd build

./runall.sh

echo "Done";
