#!/bin/bash 

set -e
set -o pipefail

if [ ! -x "$JAVA_HOME"/bin/java -o ! -x "$JAVA_HOME"/bin/jdb -o ! -x "$JAVA_HOME"/bin/javac ]; then
      echo "The JAVA_HOME environment variable is not defined correctly"
      echo "NB: JAVA_HOME should point to a JDK not a JRE"
      exit 1
fi

export PATH=.:${JAVA_HOME}/bin:${PATH}

ARG_FORMAT=$1
args=$(getopt -l "database:,driver:,jdk:" -- "$@")
eval set -- "$args"

while [ $# -ge 1 ]; do
    case "$1" in
        --database)
            ARG_DATABASE=$2
            shift ;;
        --driver)
            ARG_DRIVER=$2
            shift;;
        --) shift ; break ;;
    esac
    shift
done

if [ ! -f ${ARG_DRIVER} ]
    then
    echo "Driver missing -  ${ARG_DRIVER}"
    exit 1
fi
BASEDIR=$(dirname $0)
JDK_NAME=`basename ${JAVA_HOME}`
DRIVER_NAME=`basename ${ARG_DRIVER}`
export WORK_FOLDER=${BASEDIR}/tests/${JDK_NAME}/${ARG_DATABASE}/${DRIVER_NAME}/${ARG_FORMAT}
rm -rf ${WORK_FOLDER}
mkdir -p ${WORK_FOLDER}
touch ${WORK_FOLDER}/run.log
echo "Running tests: ${WORK_FOLDER}"

exec 1>> ${WORK_FOLDER}/run.log 2>&1

echo "ARG_DATABASE = $ARG_DATABASE"
echo "ARG_DRIVER = $ARG_DRIVER"
echo "ARG_FORMAT = $ARG_FORMAT"
echo "JDK = $JDK_NAME"

. ${BASEDIR}/database/${ARG_DATABASE}/set_source_env.sh
. ${BASEDIR}/common/set_nuodb_env.sh
echo "Env variables set up complete"
. ${BASEDIR}/common/setup_nuodb.sh
echo "nuodb set up complete"
#. ${BASEDIR}/database/${ARG_DATABASE}/set_source_data.sh
echo "migration start"
. ${BASEDIR}/database/${ARG_DATABASE}/migrate_data.sh
echo "migration complete"
echo "integration_tests start"
. ${BASEDIR}/database/${ARG_DATABASE}/run_integration_tests.sh
echo "integration_tests complete"
