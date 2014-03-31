#!/bin/bash 

set -e
set -o pipefail

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
            shift ;;
        --jdk)
            ARG_JDK=$2
            shift ;;
        --) shift ; break ;;
    esac
    shift
done

if [ ! -d ${ARG_JDK} ]
    then
    echo "JDK missing -  ${ARG_JDK}"
    exit 1
fi
if [ ! -f ${ARG_DRIVER} ]
    then
    echo "Driver missing -  ${ARG_DRIVER}"
    exit 1
fi
BASEDIR=$(dirname $0)
JDK_NAME=`basename ${ARG_JDK}`
DRIVER_NAME=`basename ${ARG_DRIVER}`
export WORK_FOLDER=${BASEDIR}/tests/${JDK_NAME}/${ARG_DATABASE}/${DRIVER_NAME}/${ARG_FORMAT}
rm -rf ${WORK_FOLDER}
mkdir -p ${WORK_FOLDER}
touch ${WORK_FOLDER}/run.log
echo "Running tests: ${WORK_FOLDER}"
echo "ARG_DATABASE = $ARG_DATABASE" >> ${WORK_FOLDER}/run.log
echo "ARG_DRIVER = $ARG_DRIVER" >> ${WORK_FOLDER}/run.log
echo "ARG_JDK = $ARG_JDK" >> ${WORK_FOLDER}/run.log
echo "ARG_FORMAT = $ARG_FORMAT" >> ${WORK_FOLDER}/run.log

. ${BASEDIR}/common/set_java.sh
. ${BASEDIR}/database/${ARG_DATABASE}/set_source_env.sh
. ${BASEDIR}/common/set_nuodb_env.sh
echo "Env variables set up complete" >> ${WORK_FOLDER}/run.log
. ${BASEDIR}/common/setup_nuodb.sh
echo "nuodb set up complete" >> ${WORK_FOLDER}/run.log
#. ${BASEDIR}/database/${ARG_DATABASE}/set_source_data.sh
echo "migration start" >> ${WORK_FOLDER}/run.log
. ${BASEDIR}/database/${ARG_DATABASE}/migrate_data.sh
echo "migration complete" >> ${WORK_FOLDER}/run.log
echo "integration_tests start" >> ${WORK_FOLDER}/run.log
. ${BASEDIR}/database/${ARG_DATABASE}/run_integration_tests.sh
echo "integration_tests complete" >> ${WORK_FOLDER}/run.log
