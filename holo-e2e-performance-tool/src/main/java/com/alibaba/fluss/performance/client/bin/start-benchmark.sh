#!/bin/bash

source ./env.sh

if [ -z "$1" ]; then
  echo "Usage: $0 (insert|update|partial_update|get｜binlog|binlog_skip_prepare|failover)"
  exit 1
fi

SERVER_COUNTS=1
if [ -n "$2" ]; then
  SERVER_COUNTS=$2
fi

echo "server count $SERVER_COUNTS"

BENCH_TYPE=$1

if [ "$BENCH_TYPE" != "binlog_skip_prepare" ]; then
  echo "restart fluss cluster"
  ./stop-cluster.sh
  ./start-cluster.sh $SERVER_COUNTS
fi


start_profiler() {
  SERVER=$1
  PID=$2
  PROFILE_DURATION=$4
#  SLEEP_DURATION=$5
  sleep 20
	ssh "$1" "export JAVA_HOME=$3 &&
	export PATH=\$JAVA_HOME/bin:\$PATH &&
	nohup sh -c 'java -jar /root/$ARTHAS_JAR_NAME $2 <<< \"profiler start --duration $PROFILE_DURATION\"' &> t.log &"
}

profile_servers() {
  PROFILE_DURATION=$1
  SLEEP_DURATION=$2
  for SERVER in "${SERVER_ADDRESSES[@]}"; do
    COMMAND="ps -ef | grep java | grep \"TabletServer\" | grep -v grep | awk '{print \$2}'"
    PIDS=$(ssh "$SERVER" "$COMMAND")
    echo $PIDS
    if [ -z "$PIDS" ]; then
    	echo "No process TabletServer while begin arthas profiler"
    else
    	for PID in $PIDS; do
    	  echo $JAVA_HOME
    	  start_profiler "$SERVER" "$PID" "$JAVA_HOME" "$PROFILE_DURATION" &
        echo "profiler start for process $PID in $SERVER, last $PROFILE_DURATION secondes"
        break
    	done
    fi
  done
}

if [ "$BENCH_TYPE" != "failover" ]; then
  profile_servers 90 20
fi



profile_client() {
  PROFILE_DURATION=$1
  SLEEP_DURATION=$2
  for CLIENT_SERVER in "${KV_CLIENT_ADDRESSES[@]}"; do
    while true; do
      COMMAND="ps -ef | grep java | grep holo-e2e-performance-tool | grep -v grep | awk '{print \$2}'"
      PID=$(ssh "$CLIENT_SERVER" "$COMMAND")
      if [ -z "$PID" ]; then
        echo "No process holo-e2e-performance-tool while begin arthas profiler. Wait for a while"
        sleep 1
      else
        start_profiler "$CLIENT_SERVER" "$PID" "$KV_CLIENT_JAVA_HOME" "$PROFILE_DURATION"  &
        echo "profiler start in $CLIENT_SERVER, last 120 secondes"
        break
      fi
    done
  done
}

profile_client 90 20 &



## start kv-benchmark-client
for CLIENT_SERVER in "${KV_CLIENT_ADDRESSES[@]}"; do
  ssh "$CLIENT_SERVER" << EOF
     cd $KV_INSTALL_PATH
     case "$BENCH_TYPE" in
       insert)
         java -jar $KV_FRAMEWORK_JAR_NAME conf/test_insert.conf INSERT
         ;;
       update)
         java -jar $KV_FRAMEWORK_JAR_NAME conf/test_insert.conf INSERT
         java -jar $KV_FRAMEWORK_JAR_NAME conf/test_update.conf INSERT
         ;;
       partial_update)
         java -jar $KV_FRAMEWORK_JAR_NAME conf/test_insert.conf INSERT
         java -jar $KV_FRAMEWORK_JAR_NAME conf/test_update_part.conf INSERT
         ;;
       get)
         echo "Running PREPARE_GET_DATA..."
         java -jar $KV_FRAMEWORK_JAR_NAME conf/test_get_sync.conf PREPARE_GET_DATA
         echo "Running GET..."
         java -jar $KV_FRAMEWORK_JAR_NAME conf/test_get_sync.conf GET
         ;;
       binlog)
         echo "Running PREPARE_BINLOG_DATA..."
         java -jar $KV_FRAMEWORK_JAR_NAME conf/test_binlog.conf PREPARE_BINLOG_DATA
         echo "Running BINLOG..."
         java -jar $KV_FRAMEWORK_JAR_NAME conf/test_binlog.conf BINLOG
         ;;
       binlog_skip_prepare)
         echo "Running BINLOG..."
         java -jar $KV_FRAMEWORK_JAR_NAME conf/test_binlog.conf BINLOG
         ;;
       failover)
         echo "Running failover..."
         java -jar $KV_FRAMEWORK_JAR_NAME conf/test_failover.conf INSERT
         ;;
       *)
         echo "Unknown $BENCH_TYPE, using default configuration."
         ;;
     esac
EOF
done

#if [ "$BENCH_TYPE" = "failover" ]; then
#  echo "start to test fail over"
#  echo "stop current tablet server."
#  cd $KV_INSTALL_PATH
#  ./bin/tablet-server.sh stop
#
#  # then profile servers
#  profile_servers 20 1
#
#  echo "start to sleep"
#  sleep 25
#  # then ssh to the other tablet servers to grep the recover cost times
#  for SERVER in "${SERVER_ADDRESSES[@]}"; do
#    LINE=$(ssh "$SERVER" "grep ' finish, cost' $FLUSS_INSTALL_PATH/log/*tablet-server*.log")
#    echo "$SERVER: $LINE"
#  done
#fi

if [ "$BENCH_TYPE" = "failover" ]; then
  echo "start to test fail over"
  echo "stop current tablet server."
#  cd $KV_INSTALL_PATH
#  ./bin/tablet-server.sh stop

  # then profile servers
#  profile_servers 20 1

  echo "start to sleep"
  sleep 25
  # then ssh to the other tablet servers to grep the recover cost times
  for SERVER in "${SERVER_ADDRESSES[@]}"; do
    ssh "$SERVER" "$FLUSS_INSTALL_PATH/bin/tablet-server.sh stop"
#    LINE=$(ssh "$SERVER" "grep ' finish, cost' $FLUSS_INSTALL_PATH/log/*tablet-server*.log")
#    echo "$SERVER: $LINE"
  done
fi


# upload the result into oss, the file name begin with date.
# copy running result to result dir.
rm -rf result/*
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
if [ ! -d "result" ]; then
	mkdir -p result
fi
mkdir -p result/$TIMESTAMP

for SERVER in "${SERVER_ADDRESSES[@]}"; do
	mkdir result/$TIMESTAMP/$SERVER
	scp -r $SERVER:$FLUSS_INSTALL_PATH/arthas-output/* result/$TIMESTAMP/$SERVER/
	scp -r $SERVER:$FLUSS_INSTALL_PATH/log/*.log result/$TIMESTAMP/$SERVER/
done

for CLIENT in "${KV_CLIENT_ADDRESSES[@]}"; do
	mkdir result/$TIMESTAMP/$CLIENT
	scp -r $CLIENT:$KV_INSTALL_PATH/arthas-output/* result/$TIMESTAMP/$CLIENT/
#	scp -r $CLIENT:$KV_INSTALL_PATH/*.log result/$TIMESTAMP/$CLIENT/
	scp -r $CLIENT:$KV_INSTALL_PATH/conf/result.csv result/$TIMESTAMP/$CLIENT/
done

# upload to oss
ossutil cp -r result/$TIMESTAMP $OSS_RESULT_DIR/$TIMESTAMP
