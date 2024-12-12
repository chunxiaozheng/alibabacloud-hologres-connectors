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
import com.alibaba.fluss.client.scanner.ScanRecord;
import com.alibaba.fluss.client.scanner.snapshot.SnapshotScan;
import com.alibaba.fluss.client.scanner.snapshot.SnapshotScanner;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.snapshot.BucketSnapshotInfo;
import com.alibaba.fluss.client.table.snapshot.KvSnapshotInfo;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static java.lang.Math.min;

public class ScanTest {

    public static final Logger LOG = LoggerFactory.getLogger(BinlogTest.class);
    public static final String METRICS_BINLOG_PERF_RPS = "scan_perf_rps";
    public static final String METRICS_BINLOG_PERF_LATENCY = "scan_perf_latency";

    private static final long POLL_TIMEOUT_MS = 10000L;

    private String confName;
    private long targetTime;
    private final List<List<Integer>> shardListPerThread = new ArrayList<>();
    PrepareScanDataConf prepareScanDataConf = new PrepareScanDataConf();
    ScanTestConf scanTestConf = new ScanTestConf();
    ClientConf clientConf = new ClientConf();


    private int shardCount = 0;
    private static long memoryUsage = 0;
    private CyclicBarrier barrier = null;

    private Connection connection;
    private TablePath tablePath;
    private Schema schema;
    private AtomicLong totalRecords;
    private InternalRow.FieldGetter[] projectFieldGetter;
    private int[] projectFields;

    private KvSnapshotInfo tableSnapshotInfo;


    public void run(String confName) throws Exception {
        LOG.info("confName:{}", confName);
        this.confName = confName;
        ConfLoader.load(confName, "scan.", scanTestConf);
        ConfLoader.load(confName, "client.", clientConf);
        ConfLoader.load(confName, "prepareScanData.", prepareScanDataConf);
        totalRecords = new AtomicLong();

        Configuration flussConf = new Configuration();
        flussConf.setString(ConfigOptions.BOOTSTRAP_SERVERS.key(),
                this.clientConf.bootstrapServers);

        Reporter reporter = new Reporter(confName);

        this.tablePath = TablePath.of("default", scanTestConf.tableName);
        this.connection = ConnectionFactory.createConnection(flussConf);

        Admin admin = this.connection.getAdmin();
        TableInfo tableInfo = admin.getTable(this.tablePath).get();
        this.schema = tableInfo.getTableDescriptor().getSchema();

        if (schema.getColumnNames().size() < scanTestConf.scanColumns) {
            throw new IllegalArgumentException("scanColumns must less than schema columns");
        }
        projectFields = IntStream.range(0, scanTestConf.scanColumns).toArray();
        projectFieldGetter = new InternalRow.FieldGetter[projectFields.length];
        RowType rowType = schema.toRowType();
        for (int i = 0; i < projectFields.length; i++) {
            projectFieldGetter[i] =
                    InternalRow.createFieldGetter(
                            rowType.getTypeAt(i),
                            projectFields[i]
                    );
        }

        shardCount = tableInfo.getTableDescriptor()
                .getTableDistribution().get()
                .getBucketCount().get();

        scanTestConf.threadSize = min(scanTestConf.threadSize, shardCount);
        barrier = new CyclicBarrier(scanTestConf.threadSize, ()->{
            memoryUsage = Util.getMemoryStat();
            Util.dumpHeap(confName);
        });
        for(int i = 0; i < shardCount; i++) {
            int index = i % scanTestConf.threadSize;
            if (shardListPerThread.size() <= index) {
                shardListPerThread.add(new ArrayList<>());
            }
            shardListPerThread.get(index).add(i);
        }
        targetTime = System.currentTimeMillis() + scanTestConf.testTime;

        // get snapshot
        tableSnapshotInfo = admin.getKvSnapshot(tablePath).get();

        Thread[] threads = new Thread[scanTestConf.threadSize];
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

        if (this.scanTestConf.deleteTableAfterDone) {
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
                    for (int shard : shardList) {
                        BucketSnapshotInfo bucketSnapshotInfo
                                = tableSnapshotInfo.getBucketsSnapshots().getBucketSnapshotInfo(shard)
                                .get();
                        // TODO: fixme, should have a correct table_id
                        SnapshotScan snapshotScan =
                                new SnapshotScan(new TableBucket(1, shard), bucketSnapshotInfo.getSnapshotFiles(),
                                        schema, null);
                        try(SnapshotScanner snapshotScanner
                                 = table.getSnapshotScanner(snapshotScan)) {
                            while (true) {
                                Iterator<ScanRecord> scanRecordIterator
                                        =  snapshotScanner.poll(Duration.ofMillis(POLL_TIMEOUT_MS));
                                if (scanRecordIterator == null) {
                                    break;
                                }
                                while (scanRecordIterator.hasNext()) {
                                    ScanRecord scanRecord = scanRecordIterator.next();
                                    InternalRow internalRow
                                            = scanRecord.getRow();
                                    for (int projectField : projectFields) {
                                        projectFieldGetter[projectField].getFieldOrNull(internalRow);
                                    }
                                    meter.mark();
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

class ScanTestConf {
    public int threadSize = 10;
    public long testTime = 600000;
    public String tableName = "fluss_perf";
    public boolean deleteTableAfterDone = false;
    public boolean dumpMemoryStat = false;

    public int scanColumns = 20;
}
