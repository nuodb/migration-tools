mvn -f ${BASEDIR}/../pom.xml clean
mvn -f ${BASEDIR}/../pom.xml -Pmysql-integration-tests test

