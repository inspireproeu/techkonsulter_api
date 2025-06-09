#!/bin/bash
PROCESS=$1
PROJECTPATH=$2
RUNNING=$(pm2 pid $PROCESS)0
if [ "${RUNNING}" -eq 0 ]; then
cd $PROJECTPATH
pm2 delete $PROCESS || true
pm2 start "npm run ${3}" --name "${PROCESS}"
fi;