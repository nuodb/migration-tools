count=`grep "BUILD SUCCESS" ${WORK_FOLDER}/run.log | wc -l`
if [ "$count" -lt 1 ]; then
  tail -100 ${WORK_FOLDER}/run.log > ${WORK_FOLDER}/tail.log
  mail -s "Integration Tests Failed ${WORK_FOLDER}" kdhandapani@nuodb.com < ${WORK_FOLDER}/tail.log
   else
   echo "Tests Completed for ${WORK_FOLDER}"
fi

