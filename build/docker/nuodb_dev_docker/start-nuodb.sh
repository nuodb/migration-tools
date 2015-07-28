# Start ssh
/etc/init.d/ssh start

echo "Starting nuoagent:"
/opt/nuodb/etc/nuoagent start

echo "Starting nuodb rest service:"
/opt/nuodb/etc/nuorestsvc start

echo "Tailing agent logs keep container running!!!"
tail -n 1000 -f /opt/nuodb/var/log/agent.log

