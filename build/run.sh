#!/bin/bash


ARG_FORMAT=$1
args=$(getopt -l "database:,driver:,jdk:,migrator_from_git" -- "$@")
eval set -- "$args"

ARG_MIGRATOR_FROM_GIT=0
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
        --migrator_from_git) 
            ARG_MIGRATOR_FROM_GIT=1
            shift ;;
        --) shift ; break ;;
    esac
    shift
done

#echo "ARG_DATABASE = $ARG_DATABASE"
#echo "ARG_DRIVER = $ARG_DRIVER"
#echo "ARG_JDK = $ARG_JDK"
#echo "ARG_MIGRATOR_FROM_GIT = $ARG_MIGRATOR_FROM_GIT"


if [ -z "$START_TS" ]
  then
  START_TS=`date +%Y%m%d_%H%M%S`
fi
export BUILD_FOLDER=/migrator-local/work/${START_TS}/${ARG_JDK}
export WORK_FOLDER=/migrator-local/work/${START_TS}/${ARG_JDK}/${ARG_DATABASE}/${ARG_DRIVER}/${ARG_FORMAT}
rm -rf ${WORK_FOLDER}
mkdir -p ${WORK_FOLDER}
touch ${WORK_FOLDER}/run.log 2>&1
scriptFile=${WORK_FOLDER}/runtests.sh

echo "#!/bin/sh" > ${scriptFile}
echo "export START_SOURCE_FOLDER=${START_SOURCE_FOLDER}" >> ${scriptFile}
echo "export WORK_FOLDER=${WORK_FOLDER}" >> ${scriptFile}
echo "export ARG_FORMAT=${ARG_FORMAT}" >> ${scriptFile}
echo "export ARG_DATABASE=${ARG_DATABASE}" >> ${scriptFile}
echo "export ARG_DRIVER=${ARG_DRIVER}" >> ${scriptFile}
echo "export ARG_JDK=${ARG_JDK}" >> ${scriptFile}
echo "export ARG_MIGRATOR_FROM_GIT=${ARG_MIGRATOR_FROM_GIT}" >> ${scriptFile}
cat common/set_java.sh >> ${scriptFile}
cat database/${ARG_DATABASE}/set_source_env.sh >> ${scriptFile}
cat common/set_nuodb_env.sh >> ${scriptFile}
cat common/set_source_folders.sh >> ${scriptFile}
cat common/build_source.sh >> ${scriptFile}
cat common/setup_nuodb.sh >> ${scriptFile}
#cat database/${ARG_DATABASE}/set_source_data.sh >> ${scriptFile}
cat database/${ARG_DATABASE}/migrate_data.sh >> ${scriptFile}
cat database/${ARG_DATABASE}/run_integration_tests.sh >> ${scriptFile}
cat common/analyze_log.sh >> ${scriptFile}
chmod +x ${scriptFile}
${scriptFile}
