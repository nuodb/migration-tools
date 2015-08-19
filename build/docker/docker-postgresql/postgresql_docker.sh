#!/bin/bash
#  Author: Masthaka Team 


CONTAINER='postgresql-cdmt-cont'
IMAGE='postgresql-cdmt-img'
pwd
BASE_DIR=$(dirname $0)

echo "Postgresql Docker Process Starting"
pwd
cd ../core/src/test/resources/postgresql/ && db_mount_path=`pwd` && cd -

cp -r ../core/src/test/resources/postgresql/datatypes.sql  ${BASE_DIR}/docker/docker-postgresql/
cp -r ../core/src/test/resources/postgresql/nuodbtest.sql  ${BASE_DIR}/docker/docker-postgresql/

docker build -t $IMAGE ${BASE_DIR}/docker/docker-postgresql/
echo "postgresql Image Creation is done.";
 
docker rm -f $CONTAINER > /dev/null || true
docker run  -d   --name="postgresql-cdmt-cont"  -p 54322:5432 $IMAGE 


echo "OK - $CONTAINER is running.";
echo "Postgresql Image Building & Container Creation Process Is Done";
