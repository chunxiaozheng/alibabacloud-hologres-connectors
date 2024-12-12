package com.alibaba.fluss.performance.client;

import com.alibaba.hologres.client.utils.ConfLoader;

public class PrepareScanData extends PutTest {

    ScanTestConf scanTestConf = new ScanTestConf();
    PrepareScanDataConf prepareScanDataConf = new PrepareScanDataConf();


    @Override
    void init() throws Exception {
        super.init();

        ConfLoader.load(confName, "scan.", scanTestConf);
        ConfLoader.load(confName, "prepareScanData.", prepareScanDataConf);

        putConf.testByTime = false;
        putConf.rowNumber = prepareScanDataConf.rowNumber;
        putConf.tableName = scanTestConf.tableName;
        putConf.shardCount = prepareScanDataConf.shardCount;
        putConf.deleteTableAfterDone = false;
    }
}


class PrepareScanDataConf {
    public long rowNumber = 1000000;
    public int shardCount = 40;
}