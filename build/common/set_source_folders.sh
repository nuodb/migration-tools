if [ ! -s "${BUILD_FOLDER}/compile.log" ]
  then
  if [ -d $START_SOURCE_FOLDER ]
    then
      cp -r ${START_SOURCE_FOLDER} ${BUILD_FOLDER}
    else
      git clone git://github.com/nuodb/migration-tools.git ${BUILD_FOLDER}/migration-tools
  fi
fi

export NUODB_MIGRATION_SOURCE=${BUILD_FOLDER}/migration-tools
export NUODB_MIGRATION_ROOT=${NUODB_MIGRATION_SOURCE}/assembly/target/nuodb-migrator

