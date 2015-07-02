#!/bin/bash
#  Author: Masthaka Team 


CONTAINER='mysql-cdmt-cont'
IMAGE='mysql-cdmt-img'

echo "Mysql Docker Process Starting"
touch ../core/src/test/resources/mysql/mysql_init.sh

rm -rf docker-mysql-5.1

git clone https://github.com/bkmukund/docker-mysql-5.1.git
docker build -t $IMAGE ./docker-mysql-5.1/ # building docker image

rm -rf docker-mysql-5.1

echo "mysql -u root -proot test < /db_mount_path/datatypes.sql" >  ../core/src/test/resources/mysql/mysql_init.sh
echo "mysql -u root -proot test < /db_mount_path/nuodbtest.sql" >> ../core/src/test/resources/mysql/mysql_init.sh
echo "mysql -u root -proot test < /db_mount_path/precision.sql" >> ../core/src/test/resources/mysql/mysql_init.sh

echo "MySQL Image Creation is done.";

cd ../core/src/test/resources/mysql

db_mount_path=`pwd`

cd -

docker rm -f $CONTAINER > /dev/null || true
docker run --name="mysql-cdmt-cont"  -d  -v $db_mount_path:/db_mount_path/ -p 33306:3306 $IMAGE
  
STARTED=$(docker inspect --format="{{ .State.StartedAt }}" $CONTAINER)

echo "OK - $CONTAINER is running.  StartedAt: $STARTED";
echo "MySQL Image Building & Container Creation Process Is Done";
