#!/bin/bash 

set -e
set -o pipefail

if [ ! -x "$JAVA_HOME"/bin/java -o ! -x "$JAVA_HOME"/bin/jdb -o ! -x "$JAVA_HOME"/bin/javac ]; then
      echo "The JAVA_HOME environment variable is not defined correctly"
      echo "NB: JAVA_HOME should point to a JDK not a JRE"
      exit 1
fi

export PATH=.:${JAVA_HOME}/bin:${PATH}

FORMAT=$1
args=$(getopt -l "database:,driver:,jdk:" -- "$@")
eval set -- "$args"

while [ $# -ge 1 ]; do
    case "$1" in
        --database)
            DATABASE_NAME=$2
            shift ;;
        --driver)
            SOURCE_JDBCJAR=$2
            shift;;
        --) shift ; break ;;
    esac
    shift
done

if [ ! -f ${SOURCE_JDBCJAR} ]
    then
    echo "Missing source driver"
    exit 1
fi

CUR_DIR=`pwd`
BASE_DIR=$(dirname $0)
JAVA_NAME=`basename ${JAVA_HOME}`
DRIVER_NAME=`basename ${SOURCE_JDBCJAR}`
WORK_DIR=${BASE_DIR}/tests/${JAVA_NAME}/${DATABASE_NAME}/${DRIVER_NAME}/${FORMAT}
NUODB_MIGRATOR_ROOT=${BASE_DIR}/..

echo "Running tests in ${WORK_DIR}"
rm -rf ${WORK_DIR}
mkdir -p ${WORK_DIR}
touch ${WORK_DIR}/run.log

echo "Using java $JAVA_NAME"
echo "Using database $DATABASE_NAME"
echo "Using driver $SOURCE_JDBCJAR"
echo "Using format $FORMAT"

echo "Setup env"
. ${BASE_DIR}/common/setup_env.sh
. ${BASE_DIR}/database/${DATABASE_NAME}/setup_env.sh

echo "Setup migrator"
. ${BASE_DIR}/common/setup_migrator.sh

echo "Setup source database"
. ${BASE_DIR}/database/${DATABASE_NAME}/setup_source.sh

echo "Setup target database"
. ${BASE_DIR}/common/setup_nuodb.sh

echo "Running migration"
. ${BASE_DIR}/database/${DATABASE_NAME}/migrate_data.sh

echo "Running integration tests"
. ${BASE_DIR}/database/${DATABASE_NAME}/run_integration_tests.sh