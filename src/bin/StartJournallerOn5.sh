#!/bin/bash
set -o nounset
export JAVA_HOME=/usr/lib/jvm/java

lwesLogs=/home/gxetl/lwesLogs
if [ -f /etc/sysconfig/lwes-journaller ]; then
  . /etc/sysconfig/lwes-journaller
else
  echo "WARN: /etc/sysconfig/lwes-journaller not found. Exiting"
  exit 1
fi

if [ -n "${1:-}" ] && [ -n "${2:-}" ];then
	LWES_ADDRESS=$1
	LWES_PORT=$2
else
	LWES_ADDRESS=10.245.63.143
	LWES_PORT=9999
	echo "Using default address and port: $LWES_ADDRESS, $LWES_PORT"
fi

if [ ! -d $LWES_JOURNALLER_HOME ];then
	echo "Journaller home directory does Not exist, check: $LWES_JOURNALLER_HOME"
	exit 1
fi


# Start journaller
## Due to the math used calculate a complete bucket, log must be started at a 5 minute mark
echo "LWES Logs files: $LWES_JOURNAL_FILE"
min=$(date +%M)

while [ ! $(( min % 5 )) -eq 0 ]
do
        min=$(date +%M)
        sleep 10s
        echo "$(date):Waiting to start logs at a 5 minute mark"
done
echo "JAVAHOME = $JAVA_HOME"
PROG_CMD="$LWES_JOURNALLER_HOME/bin/lwes-journaller \
 -a $LWES_ADDRESS -p $LWES_PORT \
 -f $LWES_JOURNAL_FILE $LWES_JOURNAL_FORMAT"
echo "CMD= $PROG_CMD"

$PROG_CMD

[[ ! $? -eq 0 ]] && echo "ERROR starting journaller"
