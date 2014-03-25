cd ${NUODB_MIGRATION_SOURCE}
if [ ! -s "${BUILD_FOLDER}/compile.log" ]
  then
  touch ${BUILD_FOLDER}/compile.log 2>&1
  mvn clean install >> ${BUILD_FOLDER}/compile.log 2>&1
  echo "Building source in ${BUILD_FOLDER}"
fi
count=`grep "BUILD SUCCESS" ${BUILD_FOLDER}/compile.log | wc -l`
if [ "$count" -lt 1 ]; then
  tail -100 ${BUILD_FOLDER}/compile.log > ${BUILD_FOLDER}/tail.log
  mail -s "Build Failed ${BUILD_FOLDER}" kdhandapani@nuodb.com < ${BUILD_FOLDER}/tail.log
  exit 1;
fi

