#!/bin/sh
. ./test/.travis_env

wget -q http://download.nuohub.org/nuodb-2.0.4.linux.x64.deb --output-document=/var/tmp/nuodb.deb
sudo dpkg -i /var/tmp/nuodb.deb
${NUODB_HOME}/bin/nuodbmgr --broker localhost --password bird --command "start process sm host localhost database test archive /var/tmp/nuodb initialize true"
${NUODB_HOME}/bin/nuodbmgr --broker localhost --password bird --command "start process te host localhost database test options '--dba-user ${NUODB_USERNAME} --dba-password ${NUODB_PASSWORD}'"
