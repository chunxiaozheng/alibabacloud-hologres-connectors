/*
 * Decompiled with CFR 0.153-SNAPSHOT (d6f6758-dirty).
 */
package com.alibaba.fluss.performance.client;

import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.compacted.CompactedRow;
import com.alibaba.fluss.row.encode.RowEncoder;
import com.alibaba.fluss.types.DataTypeRoot;
import com.alibaba.hologres.client.model.HoloVersion;
import com.alibaba.hologres.client.utils.ConfLoader;
import com.alibaba.hologres.client.utils.Metrics;
import com.alibaba.hologres.com.codahale.metrics.Histogram;
import com.alibaba.hologres.com.codahale.metrics.Meter;
import com.alibaba.hologres.performace.client.InsertTest;
import com.alibaba.hologres.performace.client.Reporter;
import com.alibaba.hologres.performace.client.Util;
import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.writer.UpsertWrite;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.RowType;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PutTest {
    public static final Logger LOG = LoggerFactory.getLogger(InsertTest.class);
    private AtomicLong tic = new AtomicLong(0);
    protected String confName;
    protected final PutTestConf putConf = new PutTestConf();
    private final ClientConf clientConf = new ClientConf();
    protected long targetTime;
    private String prefix;
    private byte[] prefixBytes;
    protected AtomicLong totalCount = new AtomicLong(0L);
    private Connection connection;
    private TablePath tablePath;
    private Schema schema;

    protected static long memoryUsage = 0;
    protected CyclicBarrier barrier = null;

    public void run(String confName) throws Exception {
        int i;
        LOG.info("confName:{}", confName);
        this.confName = confName;
        this.init();
        Configuration flussConf = new Configuration();
        flussConf.setString(ConfigOptions.BOOTSTRAP_SERVERS.key(), this.clientConf.bootstrapServers);
        flussConf.setString(ConfigOptions.CLIENT_WRITER_BATCH_SIZE.key(), this.clientConf.batchSize);
        flussConf.setString(ConfigOptions.CLIENT_WRITER_REQUEST_MAX_SIZE.key(), this.clientConf.requestMaxSize);
        flussConf.setString(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE.key(), this.clientConf.bufferMemorySize);
        flussConf.setString(ConfigOptions.CLIENT_WRITER_BATCH_TIMEOUT.key(), this.clientConf.batchTimeOut);
        Reporter reporter = new Reporter(confName);
        this.connection = ConnectionFactory.createConnection(flussConf);
        Admin admin = this.connection.getAdmin();
        reporter.start(new HoloVersion(1, 1, 2));
        this.tablePath = TablePath.of("default", this.putConf.tableName);
        if (this.putConf.deleteTableBeforeRun) {
            admin.deleteTable(this.tablePath, true).get();
            Thread.sleep(1000L);
        }
        if (this.putConf.createTableBeforeRun) {
            TableDescriptor tableDescriptor = TableUtil.createTablePathAndDescriptor(this.putConf.columnCount, this.putConf.shardCount);
            this.schema = tableDescriptor.getSchema();
            admin.createDatabase("default", true).get();
            admin.createTable(this.tablePath, tableDescriptor, false).get();
        } else {
            this.schema = admin.getTable(this.tablePath).get().getTableDescriptor().getSchema();
        }
        StringBuilder sb = new StringBuilder();
        for (i = 0; i < this.putConf.columnSize; ++i) {
            sb.append("a");
        }
        this.prefix = sb.toString();
        this.prefixBytes = this.prefix.getBytes();

        barrier = new CyclicBarrier(putConf.threadSize, ()->{
            memoryUsage = Util.getMemoryStat();
            Util.dumpHeap(confName);
        });

        this.targetTime = System.currentTimeMillis() + this.putConf.testTime;
        Thread[] threads = new Thread[this.putConf.threadSize];
        Metrics.startSlf4jReporter(60L, TimeUnit.SECONDS);
        for (i = 0; i < threads.length; ++i) {
            threads[i] = new Thread(new InsertJob(i));
            threads[i].start();
        }
        for (i = 0; i < threads.length; ++i) {
            threads[i].join();
        }
        LOG.info("finished, {} rows has written", this.totalCount.get());
        Metrics.reporter().report();
        Meter meter = Metrics.registry().meter("write_rps");
        Histogram hist = Metrics.registry().histogram("write_latency");
        reporter.report(meter.getCount(),
                meter.getOneMinuteRate(),
                meter.getFiveMinuteRate(),
                meter.getFifteenMinuteRate(),
                hist.getSnapshot().getMean(),
                hist.getSnapshot().get99thPercentile(),
                hist.getSnapshot().get999thPercentile(),
                memoryUsage);
        if (this.putConf.deleteTableAfterDone) {
            admin.deleteTable(this.tablePath, false).get();
        }
        admin.close();
        this.connection.close();
    }

    void init() throws Exception {
        ConfLoader.load(this.confName, "put.", this.putConf);
        ConfLoader.load(this.confName, "client.", this.clientConf);
    }

    class InsertJob implements Runnable {
        final int id;
        final DataType[] dataTypes;
        final RowType rowType;

        public InsertJob(int id) {
            this.id = id;
            this.dataTypes = PutTest.this.schema.toRowType().getChildren().toArray(new DataType[0]);
            this.rowType = PutTest.this.schema.toRowType();
        }

        @Override
        public void run() {
            try {
                Meter meter = Metrics.registry().meter("write_rps");
                Meter bpsMeter = Metrics.registry().meter("write_bps");
                UpsertWrite upsertWrite = new UpsertWrite();
                // plus 1 is for the pk
                int writeColumns = dataTypes.length;
                if (putConf.writeColumnCount > 0) {
                    writeColumns = putConf.writeColumnCount + 1;
                    upsertWrite = upsertWrite.withPartialUpdate(IntStream.range(0, writeColumns).toArray());
                }
                try (Table table = PutTest.this.connection.getTable(PutTest.this.tablePath)) {
                    UpsertWriter upsertWriter = table.getUpsertWriter(upsertWrite);
                    RowEncoder rowEncoder = RowEncoder.create(table.getDescriptor().getKvFormat(), this.dataTypes);
                    int i = 0;
                    while (true) {
                        long pk = PutTest.this.tic.incrementAndGet();
                        ++i;
                        if (PutTest.this.putConf.testByTime) {
                            if (i % 1000 == 0 && System.currentTimeMillis() > PutTest.this.targetTime) {
                                LOG.info("test time reached");
                                PutTest.this.totalCount.addAndGet(i - 1);
                                break;
                            }
                        } else if (pk > PutTest.this.putConf.rowNumber) {
                            LOG.info("insert write : {}", i - 1);
                            PutTest.this.totalCount.addAndGet(i - 1);
                            break;
                        }
                        InternalRow row = this.newRow(pk, this.dataTypes, rowEncoder);
                        upsertWriter.upsert(row);
                        meter.mark();
                        if (row instanceof IndexedRow) {
                            bpsMeter.mark(((IndexedRow) row).getSizeInBytes());
                        } else if (row instanceof CompactedRow) {
                            bpsMeter.mark(((CompactedRow) row).getSizeInBytes());
                        }
                    }
                    upsertWriter.flush();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private InternalRow newRow(long id, DataType[] dataTypes, RowEncoder rowEncoder) {
            rowEncoder.startNewRow();
            for (int i = 0; i < dataTypes.length; i++) {
                rowEncoder.encodeField(i, encodeFieldValue(id, dataTypes[i].getTypeRoot()));
            }
            return rowEncoder.finishRow();
        }

        private Object encodeFieldValue(long id, DataTypeRoot typeRoot) {
            switch (typeRoot) {
                case BIGINT:
                    return id;
                case STRING:
                    return BinaryString.fromBytes(PutTest.this.prefixBytes);
                default:
                    throw new RuntimeException("UnSupport data type: " + typeRoot);
            }
        }
    }
}

