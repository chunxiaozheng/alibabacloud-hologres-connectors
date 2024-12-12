export ARTHAS_INSTALL_PATH="/root/arthas-boot.jar"

export FLUSS_INSTALL_PATH="/root/fluss-kv-benchmark/fluss"
export FLUSS_DATA_PATH="/mnt/fluss-kv-bench"

# zkCli related
export ZKCLI_DIR="zk-console"
export ZKCLI_FILE_URL="https://mirrors.tuna.tsinghua.edu.cn/apache/zookeeper/zookeeper-3.7.2/apache-zookeeper-3.7.2-bin.tar.gz"
export ZKCLI_TAR_FILE="apache-zookeeper-3.7.2-bin.tar.gz"
export ZKCLI_EXTRACTED_DIR="apache-zookeeper-3.7.2-bin"
export ZKCLI_TARGET_DIR="apache-zookeeper"

export ZK_SERVER_ADDRESS="mse-xxxxx-zk.mse.aliyuncs.com"
export ZK_PATH="fluss-kv-bench-new"

export SERVER_ADDRESSES=("fluss-kv-server1" "fluss-kv-server2" "fluss-kv-server3" "fluss-kv-server4" "fluss-kv-server5" "fluss-kv-server6")
export KV_CLIENT_ADDRESSES=("kv-client")
export KV_CLIENT_BENCHMARK_EXECUTOR="kv-client"
export KV_INSTALL_PATH="/root/fluss-kv-bench-new"
export KV_CLIENT_JAVA_HOME="/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.412.b08-1.el7_9.x86_64"

# jar related
export FLUSS_SERVER_JAR_NAME="fluss-server-0.2-SNAPSHOT.jar"
export KV_FRAMEWORK_JAR_NAME="holo-e2e-performance-tool-1.0.1.jar"
export ARTHAS_JAR_NAME="arthas-boot.jar"

# oss related
export OSS_JAR_DIR="oss://fluss-daily/fluss-kv-benchmark/jar"
export OSS_RESULT_DIR="oss://fluss-daily/fluss-kv-benchmark/result"