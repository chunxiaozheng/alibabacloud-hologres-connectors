/*
 * Decompiled with CFR 0.153-SNAPSHOT (d6f6758-dirty).
 */
package com.alibaba.fluss.performance.client;

import com.alibaba.hologres.client.model.HoloVersion;
import com.alibaba.hologres.client.utils.ConfLoader;
import com.alibaba.hologres.client.utils.Metrics;
import com.alibaba.hologres.com.codahale.metrics.Histogram;
import com.alibaba.hologres.com.codahale.metrics.Meter;
import com.alibaba.hologres.performace.client.Reporter;
import com.alibaba.hologres.performace.client.Util;
import com.alibaba.hologres.performace.params.ParamsProvider;
import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.table.LookupResult;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.*;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.row.indexed.IndexedRowWriter;
import com.alibaba.fluss.types.DataType;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetTest {
    public static final Logger LOG = LoggerFactory.getLogger(GetTest.class);
    public static final String METRICS_GET_PERF_RPS = "get_perf_rps";
    public static final String METRICS_GET_PERF_LATENCY = "get_perf_latency";
    private long targetTime;
    private final GetTestConf getConf = new GetTestConf();
    private final ClientConf clientConf = new ClientConf();
    ParamsProvider provider;
    private Connection connection;
    private TablePath tablePath;
    private Schema schema;
    private static long memoryUsage = 0;
    private CyclicBarrier barrier = null;

    public void run(String confName) throws Exception {
        int i;
        LOG.info("confName:{}", (Object)confName);
        ConfLoader.load(confName, "get.", this.getConf);
        ConfLoader.load(confName, "client.", this.clientConf);
        Configuration flussConf = new Configuration();
        flussConf.setString(ConfigOptions.BOOTSTRAP_SERVERS.key(), this.clientConf.bootstrapServers);
        this.provider = new ParamsProvider(this.getConf.keyRangeParams);
        Reporter reporter = new Reporter(confName);
        reporter.start(new HoloVersion(1, 1, 1));
        this.tablePath = TablePath.of("default", this.getConf.tableName);
        this.connection = ConnectionFactory.createConnection(flussConf);
        Admin admin = this.connection.getAdmin();
        TableInfo tableInfo = admin.getTable(this.tablePath).get();
        this.schema = tableInfo.getTableDescriptor().getSchema();
        if (this.schema.getPrimaryKeyIndexes().length != this.provider.size()) {
            throw new Exception("table has " + this.schema.getPrimaryKeyIndexes().length + " pk columns, but test.params only has " + this.provider.size() + " columns");
        }

        barrier = new CyclicBarrier(this.getConf.threadSize, ()->{
            memoryUsage = Util.getMemoryStat();
            Util.dumpHeap(confName);
        });

        this.targetTime = System.currentTimeMillis() + this.getConf.testTime;
        Thread[] threads = new Thread[this.getConf.threadSize];
        Metrics.startSlf4jReporter(60L, TimeUnit.SECONDS);
        for (i = 0; i < threads.length; ++i) {
            threads[i] = new Thread(new Job(i));
            threads[i].start();
        }
        for (i = 0; i < threads.length; ++i) {
            threads[i].join();
        }
        Metrics.reporter().report();
        Meter meter = Metrics.registry().meter(METRICS_GET_PERF_RPS);
        Histogram hist = Metrics.registry().histogram(METRICS_GET_PERF_LATENCY);
        reporter.report(meter.getCount(),
                meter.getOneMinuteRate(),
                meter.getFiveMinuteRate(),
                meter.getFifteenMinuteRate(),
                hist.getSnapshot().getMean(),
                hist.getSnapshot().get99thPercentile(),
                hist.getSnapshot().get999thPercentile(),
                memoryUsage);
        if (this.getConf.deleteTableAfterDone) {
            admin.deleteTable(this.tablePath, false).get();
        }
        admin.close();
        this.connection.close();
    }

    class Job implements Runnable {
        int id;
        final DataType[] pkDataTypes;
        final IndexedRowWriter pkRowWriter;
        private final IndexedRowWriter.FieldWriter[] pkRowFieldWriters;

        public Job(int id) {
            int i;
            this.id = id;
            DataType[] allDataTypes = GetTest.this.schema.toRowType().getChildren().toArray(new DataType[0]);
            int[] pkIndic = GetTest.this.schema.getPrimaryKeyIndexes();
            this.pkDataTypes = new DataType[pkIndic.length];
            for (i = 0; i < pkIndic.length; ++i) {
                this.pkDataTypes[i] = allDataTypes[pkIndic[i]];
            }
            this.pkRowWriter = new IndexedRowWriter(this.pkDataTypes);
            this.pkRowFieldWriters = new IndexedRowWriter.FieldWriter[this.pkDataTypes.length];
            for (i = 0; i < this.pkDataTypes.length; ++i) {
                this.pkRowFieldWriters[i] = IndexedRowWriter.createFieldWriter(this.pkDataTypes[i]);
            }
        }

        @Override
        public void run() {
            try {
                Meter meter = Metrics.registry().meter(GetTest.METRICS_GET_PERF_RPS);
                Histogram hist = Metrics.registry().histogram(GetTest.METRICS_GET_PERF_LATENCY);
                try (Table table = GetTest.this.connection.getTable(GetTest.this.tablePath);){
                    int i = 0;
                    CompletableFuture<LookupResult> future = null;
                    while (++i % 1000 != 0 || System.currentTimeMillis() <= GetTest.this.targetTime) {
                        Object[] pks = new Object[GetTest.this.schema.getPrimaryKeyIndexes().length];
                        for (int j = 0; j < pks.length; ++j) {
                            pks[j] = GetTest.this.provider.get(j);
                        }
                        InternalRow keyRow = this.newInternalRow(pks);
                        long startNano = System.nanoTime();
                        future = table.lookup(keyRow);
                        if (GetTest.this.getConf.async) {
                            future = future.thenApply(r -> {
                                long endNano = System.nanoTime();
                                hist.update((endNano - startNano) / 1000000L);
                                meter.mark();
                                return r;
                            });
                        } else {
                            future.get();
                            meter.mark();
                            long endNano = System.nanoTime();
                            hist.update((endNano - startNano) / 1000L);
                        }
                    }
                    if (GetTest.this.getConf.async && future != null) {
                        future.get();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        InternalRow newInternalRow(Object[] pkFields) {
            pkRowWriter.reset();
            for (int i = 0; i < pkFields.length; i++) {
                this.pkRowFieldWriters[i].writeField(pkRowWriter, i, pkFields[i]);
            }
            IndexedRow indexedRow = new IndexedRow(pkDataTypes);
            indexedRow.pointTo(pkRowWriter.segment(), 0, pkRowWriter.position());
            return indexedRow;
        }
    }
}

