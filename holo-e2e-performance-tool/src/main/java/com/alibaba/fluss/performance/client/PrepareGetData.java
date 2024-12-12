/*
 * Decompiled with CFR 0.153-SNAPSHOT (d6f6758-dirty).
 */
package com.alibaba.fluss.performance.client;

import com.alibaba.hologres.client.utils.ConfLoader;

public class PrepareGetData
extends PutTest {
    GetTestConf getTestConf = new GetTestConf();
    PrepareGetDataConf prepareGetDataConf = new PrepareGetDataConf();

    @Override
    void init() throws Exception {
        super.init();
        ConfLoader.load(this.confName, "get.", this.getTestConf);
        ConfLoader.load(this.confName, "prepareGetData.", this.prepareGetDataConf);
        this.putConf.rowNumber = this.prepareGetDataConf.rowNumber;
        this.putConf.tableName = this.getTestConf.tableName;
        this.putConf.testByTime = false;
        this.putConf.deleteTableAfterDone = false;
    }
}

