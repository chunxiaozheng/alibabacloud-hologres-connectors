#!/bin/bash

source ./env.sh

# download zkCli package
if [ ! -d "$ZKCLI_DIR" ]; then
	echo "zkCli not exist. Download and Creating..."
       	mkdir "$ZKCLI_DIR"

	cd "$ZKCLI_DIR"
	echo "Downloading $ZKCLI_FILE_URL..."
	wget "$ZKCLI_FILE_URL"
	if [ $? -ne 0 ]; then
		echo "Failed to download the file from $FILE_URL."
		exit 1
	fi

	# un-zip
	tar -zvxf "$ZKCLI_TAR_FILE"
	mv "$ZKCLI_EXTRACTED_DIR" "$ZKCLI_TARGET_DIR"
	cd ..
else
	echo "zkCli exist. do nothing"
fi
echo

PROCESS=TabletServer

# kill tablet server
for server in "${SERVER_ADDRESSES[@]}"; do
	# kill process
	echo "$server:"
	COMMAND="ps -ef | grep java | grep \"$PROCESS\" | grep -v grep | awk '{print \$2}'"
	PIDS=$(ssh "$server" "$COMMAND")
	if [ -z "$PIDS" ]; then
	    echo "No process $PROCESS"
	else
	  for PID in $PIDS; do
	    ssh "$server" "kill -9 $PID"
	    echo "killed process $PROCESS"
	  done
	  sleep 1
	fi

	# delete log & data dir
	ssh "$server" << EOF
	cd $FLUSS_INSTALL_PATH
	rm -rf ./log/*
	rm -rf "${FLUSS_DATA_PATH}-"*
	rm -rf ${FLUSS_INSTALL_PATH}/arthas-output/*
EOF

done

for CLIENT_SERVER in "${KV_CLIENT_ADDRESSES[@]}"; do
  ssh "$CLIENT_SERVER" << EOF
    rm -rf $KV_INSTALL_PATH/arthas-output/*
    rm -rf $KV_INSTALL_PATH/conf/result.csv
EOF
done

# 等待所有后台任务完成
wait


# 在本地机器上执行命令
PID=$(ps -ef | grep java | grep CoordinatorServer | grep -v grep | awk '{print $2}')
if [ -n "$PID" ]; then
    echo "killing process CoordinatorServer"
    kill -9 $PID
fi

# delete node in zk to avoid data conflict.
delete_node() {
	local base_path=$1

	echo "Deleting zk node /$base_path"
	echo "deleteall /$base_path" | ./$ZKCLI_DIR/$ZKCLI_TARGET_DIR/bin/zkCli.sh -server $ZK_SERVER_ADDRESS
}

delete_node "$ZK_PATH"
