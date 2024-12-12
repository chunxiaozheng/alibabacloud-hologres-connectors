/*
 * Decompiled with CFR 0.153-SNAPSHOT (d6f6758-dirty).
 */
package com.alibaba.fluss.performance.client;

class PutTestConf {
    public int threadSize = 10;
    public long testTime = 600000L;
    public long rowNumber = 1000000L;
    public boolean testByTime = true;
    public String tableName = "fluss_perf";
    public int columnSize = 10;
    public int columnCount = 20;
    public int shardCount = -1;
    public int writeColumnCount = -1;
    public boolean deleteTableBeforeRun = true;
    public boolean createTableBeforeRun = true;
    public boolean deleteTableAfterDone = true;

    PutTestConf() {
    }
}

