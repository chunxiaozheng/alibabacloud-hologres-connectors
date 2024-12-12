package com.alibaba.fluss.performance.client;

import com.alibaba.hologres.client.utils.ConfLoader;

public class PrepareBinlogData extends PutTest {
    BinlogTestConf binlogTestConf = new BinlogTestConf();
    PrepareBinlogDataConf prepareBinlogDataConf = new PrepareBinlogDataConf();


    @Override
    void init() throws Exception {
        super.init();

        ConfLoader.load(confName, "binlog.", binlogTestConf);
        ConfLoader.load(confName, "prepareBinlogData.", prepareBinlogDataConf);

        putConf.testByTime = false;
        putConf.rowNumber = prepareBinlogDataConf.rowNumber;
        putConf.tableName = binlogTestConf.tableName;
        putConf.shardCount = prepareBinlogDataConf.shardCount;
        putConf.deleteTableAfterDone = false;
    }
}


class PrepareBinlogDataConf {
    public long rowNumber = 1000000;
    public int shardCount = 40;
}