#!/bin/sh
. ./test/.travis_env

[ -n "$NUO_VERSION" ] || { echo "Missing version"; exit 1; }

# Ensure THP is not on
echo madvise | sudo tee /sys/kernel/mm/transparent_hugepage/enabled >/dev/null
echo madvise | sudo tee /sys/kernel/mm/transparent_hugepage/defrag >/dev/null

wget -q "http://download.nuohub.org/nuodb-ce_${NUO_VERSION}_amd64.deb" --output-document=/var/tmp/nuodb.deb
sudo dpkg -i /var/tmp/nuodb.deb

/opt/nuodb/etc/nuoagent stop
/opt/nuodb/etc/nuorestsvc stop
/opt/nuodb/etc/nuowebconsole stop

sudo cp ./test/default.properties "$NUODB_HOME/etc/."

sudo /opt/nuodb/etc/nuoagent start
sudo /opt/nuodb/etc/nuorestsvc start
sudo /opt/nuodb/etc/nuowebconsole start

"$NUODB_HOME/bin/nuodbmgr" --broker localhost --password bird --command "start process sm host localhost database test archive /var/tmp/nuodb initialize true"
"$NUODB_HOME/bin/nuodbmgr" --broker localhost --password bird --command "start process te host localhost database test options '--dba-user $NUODB_USERNAME --dba-password $NUODB_PASSWORD'"
