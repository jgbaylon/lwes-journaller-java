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

if [ ! -d $LWES_JOURNALLER_HOME ];then
	echo "Journaller home directory does Not exist, check: $LWES_JOURNALLER_HOME"
	exit 1
fi


# Start journaller
## Due to the math used calculate a complete bucket, log must be started at a 5 minute mark
min=$(date +%M)
while [ ! $(( min % 5 )) -eq 0 ]
do
        sleep 10s
        echo "$(date):Waiting to start logs at a 5 minute mark"
        min=$(date +%M)
done
PROG_CMD="$LWES_JOURNALLER_HOME/bin/lwes-journaller \
 -a $LWES_ADDRESS -p $LWES_PORT \
 -f $LWES_JOURNAL_FILE $LWES_JOURNAL_FORMAT"

$PROG_CMD

[[ ! $? -eq 0 ]] && echo "ERROR starting journaller"
