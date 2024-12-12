#!/bin/bash

USAGE="Usage: start-clusters.sh [server_count] ]"

source ./env.sh

DEFAULT_SERVER_COUNT=1

SERVER_COUNTS=$1

# first download jar form oss and scp to different node.
echo "download jar from oss"
yes | ossutil cp $OSS_JAR_DIR/$FLUSS_SERVER_JAR_NAME /root
yes | ossutil cp $OSS_JAR_DIR/$KV_FRAMEWORK_JAR_NAME /root

if [ ! -f "$ARTHAS_INSTALL_PATH" ];then
	yes | ossutil cp $OSS_JAR_DIR/$ARTHAS_JAR_NAME $ARTHAS_INSTALL_PATH
else
	echo "arthas exists, will not download from oss"
fi

echo "jar hava downloaded from oss, scp to different node"
for SERVER in "${SERVER_ADDRESSES[@]}"; do
	yes | scp /root/$FLUSS_SERVER_JAR_NAME $SERVER:$FLUSS_INSTALL_PATH/lib/
	yes | scp $ARTHAS_INSTALL_PATH $SERVER:$ARTHAS_INSTALL_PATH
done

for CLIENT in "${KV_CLIENT_ADDRESSES[@]}"; do
	yes | scp /root/$KV_FRAMEWORK_JAR_NAME $CLIENT:$KV_INSTALL_PATH/$KV_FRAMEWORK_JAR_NAME
	yes | scp $ARTHAS_INSTALL_PATH $CLIENT:$ARTHAS_INSTALL_PATH
done



# 在远程服务器上执行启动命令的函数
start_remote_servers() {
    local server_index=$1
    local server=$2
    echo "Starting $SERVER_COUNTS tablet servers on $server..."

    echo "start server index $server_index"

    # copy server.yaml to remote servers
    yes | scp $FLUSS_INSTALL_PATH/conf/server.yaml $server:$FLUSS_INSTALL_PATH/conf/server.yaml

    ssh "$server" << EOF
        cd $FLUSS_INSTALL_PATH
        for ((i=1; i<=$SERVER_COUNTS; i++)); do
            yes | cp ./conf/server.yaml ./conf/server\$i.yaml

            # modify some values in sever yaml
            sed -i "s|tablet-server.id: 0|tablet-server.id: $server_index\$i|" ./conf/server\$i.yaml
            sed -i "s|tablet-server.host: fluss-kv-server1|tablet-server.host: $server|" ./conf/server\$i.yaml
            sed -i "s|data.dir: datadir|data.dir: $FLUSS_DATA_PATH-\$i|" ./conf/server\$i.yaml
            ./bin/tablet-server.sh start -D configFile=./conf/server\$i.yaml
        done
EOF
    echo "$server tablet servers started on $server."
}


#sed -i.bak 's|\("$JAVA_RUN"\)|\1 --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED|g' ./bin/fluss-daemon.sh




echo "begin to start fluss clusters"

cp ./server.yaml $FLUSS_INSTALL_PATH/conf/server.yaml
cd $FLUSS_INSTALL_PATH
sed -i "s|zookeeper.path.root: zkpath|zookeeper.path.root: $ZK_PATH|" conf/server.yaml
./bin/coordinator-server.sh start


echo "start tablet servers"
for ((i=0; i<${#SERVER_ADDRESSES[@]}; i++)); do
    ((index=i+1))
    echo "start server index $index"
    start_remote_servers $index ${SERVER_ADDRESSES[$i]}
done





