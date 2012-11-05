# Read help on the available command-line options prior to any migration
./nuodb-migration --help

# Dump data from MySQL example database from table1 & table2
./nuodb-migration dump \
    --source.driver=com.mysql.jdbc.Driver --source.url=jdbc:mysql://localhost:3306/test --source.username=root \
    --output.type=csv --output.path=/tmp/test/dump.cat --table=table1 --table=table2

# Load comma separated values to the corresponding tables in NuoDB from /tmp/example/dump.cat
./nuodb-migration load \
    --target.url=jdbc:com.nuodb://localhost/test --target.username=dba --target.password=goalie \
    --input.path=/tmp/test/dump.cat