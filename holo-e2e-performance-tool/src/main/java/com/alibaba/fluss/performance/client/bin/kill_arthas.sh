#!/bin/bash

PIDS=$(ps -ef | grep java | grep arthas-boot.jar | grep -v grep | awk '{print $2}')
for PID in $PIDS; do
    kill -9 $PID
done