/*
 * Decompiled with CFR 0.153-SNAPSHOT (d6f6758-dirty).
 */
package com.alibaba.fluss.performance.client;

import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.types.DataTypes;

public class TableUtil {
    public static TableDescriptor createTablePathAndDescriptor(int columnCount, int shardCount) {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("id", DataTypes.INT());
        schemaBuilder.primaryKey("id");
        for (int i = 0; i < columnCount; ++i) {
            schemaBuilder.column("name" + i, DataTypes.STRING());
        }
        TableDescriptor.Builder tableDescriptorBuilder = TableDescriptor.builder();
        tableDescriptorBuilder.schema(schemaBuilder.build());
        if (shardCount > 0) {
            tableDescriptorBuilder.distributedBy(shardCount, "id");
        }
        return tableDescriptorBuilder.build();
    }
}

