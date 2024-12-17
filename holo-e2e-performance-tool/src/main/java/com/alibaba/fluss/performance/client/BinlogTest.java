package com.alibaba.fluss.performance.client;

import com.alibaba.hologres.client.model.HoloVersion;
import com.alibaba.hologres.client.utils.ConfLoader;
import com.alibaba.hologres.client.utils.Metrics;
import com.alibaba.hologres.com.codahale.metrics.Histogram;
import com.alibaba.hologres.com.codahale.metrics.Meter;
import com.alibaba.hologres.performace.client.Reporter;
import com.alibaba.hologres.performace.client.Util;
import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.scanner.log.LogScan;
import com.alibaba.fluss.client.scanner.log.LogScanner;
import com.alibaba.fluss.client.scanner.log.ScanRecords;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static java.lang.Math.min;

public class BinlogTest {

    public static final Logger LOG = LoggerFactory.getLogger(BinlogTest.class);
    public static final String METRICS_BINLOG_PERF_RPS = "binlog_perf_rps";
    public static final String METRICS_BINLOG_PERF_LATENCY = "binlog_perf_latency";

    private static final long POLL_TIMEOUT_MS = 10000L;

    private String confName;
    private long targetTime;
    private List<List<Integer>> shardListPerThread = new ArrayList<>();
    PrepareBinlogDataConf prepareBinlogDataConf = new PrepareBinlogDataConf();
    BinlogTestConf binlogConf = new BinlogTestConf();
    ClientConf clientConf = new ClientConf();

    private int shardCount = 0;
    private static long memoryUsage = 0;
    private CyclicBarrier barrier = null;

    private Connection connection;
    private TablePath tablePath;
    private Schema schema;
    private AtomicLong totalRecords;
    private int[] projectFields;

    public void run(String confName) throws Exception {
        LOG.info("confName:{}", confName);
        this.confName = confName;
        ConfLoader.load(confName, "binlog.", binlogConf);
        ConfLoader.load(confName, "client.", clientConf);
        ConfLoader.load(confName, "prepareBinlogData.", prepareBinlogDataConf);
        totalRecords = new AtomicLong();

        Configuration flussConf = new Configuration();
        flussConf.setString(ConfigOptions.BOOTSTRAP_SERVERS.key(),
                this.clientConf.bootstrapServers);

        Reporter reporter = new Reporter(confName);


        this.tablePath = TablePath.of("default", binlogConf.tableName);
        this.connection = ConnectionFactory.createConnection(flussConf);

        Admin admin = this.connection.getAdmin();
        TableInfo tableInfo = admin.getTable(this.tablePath).get();
        this.schema = tableInfo.getTableDescriptor().getSchema();

        if (schema.getColumnNames().size() < binlogConf.scanColumns) {
            throw new IllegalArgumentException("scanColumns must less than schema columns");
        }
        projectFields = IntStream.range(0, binlogConf.scanColumns).toArray();


        shardCount = tableInfo.getTableDescriptor()
                .getTableDistribution().get()
                .getBucketCount().get();

        binlogConf.threadSize = min(binlogConf.threadSize, shardCount);
        barrier = new CyclicBarrier(binlogConf.threadSize, ()->{
            memoryUsage = Util.getMemoryStat();
            Util.dumpHeap(confName);
        });
        for(int i = 0; i < shardCount; i++) {
            int index = i % binlogConf.threadSize;
            if (shardListPerThread.size() <= index) {
                shardListPerThread.add(new ArrayList<>());
            }
            shardListPerThread.get(index).add(i);
        }
        targetTime = System.currentTimeMillis() + binlogConf.testTime;

        Thread[] threads = new Thread[binlogConf.threadSize];
        Metrics.startSlf4jReporter(60L, TimeUnit.SECONDS);
        for (int i = 0; i < threads.length; ++i) {
            threads[i] = new Thread(new Job(i));
            threads[i].start();
        }

        reporter.start(new HoloVersion(1, 1, 1));

        for (Thread thread : threads) {
            thread.join();
        }

        Metrics.reporter().report();
        {
            Meter meter = Metrics.registry().meter(METRICS_BINLOG_PERF_RPS);
            Histogram hist = Metrics.registry().histogram(METRICS_BINLOG_PERF_LATENCY);
            reporter.report(meter.getCount(), meter.getOneMinuteRate(), meter.getFiveMinuteRate(),
                    meter.getFifteenMinuteRate(), hist.getSnapshot().getMean(),
                    hist.getSnapshot().get99thPercentile(), hist.getSnapshot().get999thPercentile(),
                    memoryUsage);
        }

        if (this.binlogConf.deleteTableAfterDone) {
            admin.deleteTable(this.tablePath, false).get();
        }
        admin.close();
        this.connection.close();
    }

    class Job implements Runnable {
        int id;

        public Job(int id) {
            this.id = id;
        }

        @Override
        public void run() {
            try {
                List<Integer> shardList = shardListPerThread.get(id);
                ConfLoader.load(confName, "client.", clientConf);

                Meter meter = Metrics.registry().meter(METRICS_BINLOG_PERF_RPS);
                Histogram hist = Metrics.registry().histogram(METRICS_BINLOG_PERF_LATENCY);

                try (Table table = connection.getTable(tablePath)) {
                    LogScan logScan = new LogScan();
                    logScan.withProjectedFields(projectFields);
//                    for (Integer shardId : shardList) {
//                        logScan.addBucketWithOffset(new BucketAndOffset(shardId, 0));
//                    }

                    LogScanner logScanner = table.getLogScanner(logScan);
                    for (Integer shardId : shardList) {
                        logScanner.subscribe(shardId, 0);
                    }
                    int i = 0;
                    long startNano = System.nanoTime();

                    ScanRecords scanRecords;
                    while (true) {
                        scanRecords = logScanner.poll(Duration.ofMillis(POLL_TIMEOUT_MS));
                        if (++i % 1000 == 0 && System.currentTimeMillis() > targetTime) {
                            break;
                        }
                        meter.mark(scanRecords.count());
                        long endNano = System.nanoTime();
                        if (!scanRecords.isEmpty()) {
                            hist.update(((endNano - startNano)) / 1_000L);
                        }
                        startNano = endNano;

                        if (totalRecords.addAndGet(scanRecords.count())
                                == prepareBinlogDataConf.rowNumber) {
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}


class BinlogTestConf {
    public int threadSize = 10;
    public long testTime = 600000;
    public String tableName = "fluss_perf";
    public boolean deleteTableAfterDone = false;
    public boolean dumpMemoryStat = false;

    public int scanColumns = 20;
}