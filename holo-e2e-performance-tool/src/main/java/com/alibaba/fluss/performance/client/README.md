# 执行步骤
1. 先修改 env.sh 中的各种环境变量
2. 需要在执行本脚本的 server 上先下载 ossutil，并配置好 oss 地址，参考 https://help.aliyun.com/zh/oss/developer-reference/install-ossutil?spm=a2c4g.11186623.0.i1#concept-303829
    - 同时，上传新修改的 jar 包到 oss： https://oss.console.aliyun.com/bucket/oss-cn-hangzhou/yunxie-vvp/object?path=fluss-log-benchmark%2Fjar%2F
        - 需要上传的 jar 包为：
            1. fluss/fluss-server/target/fluss-server-0.2-SNAPSHOT.jar
            2. omb/benchmark_framework/target/benchmark-framework-0.0.1-SNAPSHOT-jar-with-dependencies.jar
3. 执行 start-benchmark.sh <ENGINE_NAME> <CONFIG_YAML> <WORKLOAD_YAML>
    - ENGINE_NAME 可以是 "fluss" 或者 "kafka"
    - CONFIG_YAML 是 fluss/kafka 的配置文件，配置 fluss 和 kafka 执行的 client 参数
        - 需要特别关注的是  projectFields: 0/2/4/6/8， 表示裁剪 value，保留 0, 2, 4, 6, 8 列
    - WORKLOAD_YAML 是 OMB workload file，会发送给 OMB 执行
        - 需要特别关注的是
            - rowTypeString: "int-int-string-string-int-string" 。表示产生数据的字段类型，用 "-" 分隔
            - inColumnFormat: true 。表示输入数据是列还是行
    - 例如：./start-benchmark.sh fluss config/fluss.yaml workload/max-rate-1-topic-1-partition-4p-1c-1kb.yaml，会启动 fluss 集群，并执行 fluss benchmark
# 各个脚本说明
1. env.sh : 各种环境变量，包括 kafka，fluss，zookeeper，以及 zkCli 下载地址， oss 地址等。
2. clear-env.sh : 清除上次执行的结果，包括停止 fluss/kafka server, 删除产生的数据，日志等；停止OMB worker，并删除日志；删除 zookeeper 中的 fluss/kafka 节点信息。 可以单独执行。
3. start-cluster.sh :将最新的 fluss-server 包，omb jar 包下载到本地，并分发给不同的机器， 启动 kafka/fluss server, OMB worker
4. start-benchmark.sh : 启动脚本，会先执行 clear-env.sh, start-cluster.sh, 然后给 OMB 集群提交测试作业，启动 arthas 采集火焰图，并把最后的结果和火焰图上传到 oss
    - 执行结果会输出到 oss 中的 result 文件夹下，目录名是日期 + 时间： 如 20240701_140835/
# 文件夹说明
1. config
    - config 文件夹下包含了 OMB 运行需要的配置文件，运行前需要按需修改
2. workloads
    - workloads 文件夹下包含了 OMB 运行需要的 workload 文件，目前添加了两个，分别对应 1 topic-1 partition 和 1 topic-100 partitions