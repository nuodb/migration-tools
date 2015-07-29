mysql -u root -proot -e "drop database if exists test;"
mysql -u root -proot -e "create database test;"

mysql -u root -proot test < /migsql/datatypes.sql
mysql -u root -proot test < /migsql/nuodbtest.sql
mysql -u root -proot test < /migsql/precision.sql
