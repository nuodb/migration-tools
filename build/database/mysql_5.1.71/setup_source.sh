docker exec mysql-cdmt-cont mysql -u${SOURCE_USERNAME} -p${SOURCE_PASSWORD} -e "drop database if exists ${SOURCE_CATALOG};"
docker exec mysql-cdmt-cont mysql -u${SOURCE_USERNAME} -p${SOURCE_PASSWORD} -e "create database ${SOURCE_CATALOG};"
docker exec mysql-cdmt-cont chmod 770 /db_mount_path/mysql_init.sh
docker exec mysql-cdmt-cont sh /db_mount_path/mysql_init.sh
sleep 1

