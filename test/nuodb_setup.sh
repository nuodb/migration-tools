#!/bin/sh
. ./test/.travis_env

wget -q http://download.nuohub.org/nuodb_2.1.1.10_amd64.deb --output-document=/var/tmp/nuodb.deb
sudo dpkg -i /var/tmp/nuodb.deb

/opt/nuodb/etc/nuoagent stop
/opt/nuodb/etc/nuorestsvc stop
/opt/nuodb/etc/nuowebconsole stop

sudo cp ./test/default.properties /opt/nuodb/etc/.

/opt/nuodb/etc/nuoagent start
/opt/nuodb/etc/nuorestsvc start
/opt/nuodb/etc/nuowebconsole start

${NUODB_HOME}/bin/nuodbmgr --broker localhost --password bird --command "start process sm host localhost database test archive /var/tmp/nuodb initialize true"
${NUODB_HOME}/bin/nuodbmgr --broker localhost --password bird --command "start process te host localhost database test options '--dba-user ${NUODB_USERNAME} --dba-password ${NUODB_PASSWORD}'"
