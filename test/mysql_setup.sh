#!/bin/sh -e
. ./test/.travis_env
wget -q http://repo1.maven.org/maven2/mysql/mysql-connector-java/5.1.44/mysql-connector-java-5.1.44.jar --output-document=${SOURCE_JDBCJAR}
