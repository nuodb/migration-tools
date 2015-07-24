#!/bin/bash
#  Author: Masthaka Team 

CONTAINER='postgresql-cdmt-cont'
IMAGE='postgresql-cdmt-img'


echo "Postgresql Docker Process Starting"
cd ../core/src/test/resources/postgresql/ &&  db_mount_path=`pwd` && cd -

rm -rf postgresql_docker
git clone https://github.com/ljavvadi/postgresql_docker.git


cp ../core/src/test/resources/postgresql/datatypes.sql  postgresql_docker/
cp ../core/src/test/resources/postgresql/nuodbtest.sql  postgresql_docker/


docker build -t $IMAGE ./postgresql_docker/
 
docker rm -f $CONTAINER > /dev/null || true
docker run  -d   --name="postgresql-cdmt-cont"  -p 54322:5432 $IMAGE 

echo "OK - $CONTAINER is running.";
echo "Postgresql Image Building & Container Creation Process Is Done";
