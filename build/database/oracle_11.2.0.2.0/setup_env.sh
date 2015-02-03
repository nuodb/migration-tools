export ORA_HOME=/home/build/migratortest/dbclients/oracle_11.2.0.4.0
export LD_LIBRARY_PATH=${ORA_HOME}:${LD_LIBRARY_PATH}
export PATH=${ORA_HOME}:${PATH}
export ORACLE_HOST=p66.hurley.nuodb.com

export SOURCE_DRIVER=oracle.jdbc.driver.OracleDriver
export SOURCE_SID=XE
export SOURCE_CATALOG=
export SOURCE_URL=jdbc:oracle:thin:MTEST/MTEST@p66.hurley.nuodb.com:1521
export SOURCE_USERNAME=MTEST
export SOURCE_PASSWORD=MTEST
