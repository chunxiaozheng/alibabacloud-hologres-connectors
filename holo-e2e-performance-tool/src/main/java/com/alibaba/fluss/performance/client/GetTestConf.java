/*
 * Decompiled with CFR 0.153-SNAPSHOT (d6f6758-dirty).
 */
package com.alibaba.fluss.performance.client;

class GetTestConf {
    public int threadSize = 10;
    public long testTime = 600000L;
    public String tableName = "fluss_perf";
    public String keyRangeParams;
    public boolean async = true;
    public boolean deleteTableAfterDone = false;

    GetTestConf() {
    }
}

