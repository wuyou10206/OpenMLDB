package com._4paradigm.openmldb.rtidb_client.test;

import com._4paradigm.openmldb.rtidb_client.common.OpenMLDBTest;
import com._4paradigm.openmldb.rtidb_client.util.RtidbUtil;
import com._4paradigm.rtidb.common.Common;
import com._4paradigm.rtidb.ns.NS;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;
import shade.guava.collect.Lists;

import java.util.*;
@Slf4j
public class TestCompress extends OpenMLDBTest {
    // 创建表-put数据-添加列-在新列上添加索引-put数据-make snapshot-add replica-change leader-get scan
    @Test
    public void testAddColumnAddIndex() throws Exception {
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        Common.ColumnDesc columnDesc1 = Common.ColumnDesc.newBuilder().setName("card").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc columnDesc2 = Common.ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("int32").build();
        Common.ColumnDesc columnDesc3 = Common.ColumnDesc.newBuilder().setName("sal").setAddTsIdx(false).setType("double").build();
        Common.ColumnDesc columnDesc4 = Common.ColumnDesc.newBuilder().setName("ts1").setIsTsCol(true).setType("int64").build();
        Common.ColumnKey columnKey1 = Common.ColumnKey.newBuilder().setIndexName("card_ck").addColName("card").addTsName("ts1").build();
        NS.TableInfo tableInfo = NS.TableInfo.newBuilder()
                .setName(tableName)
                .addColumnDescV1(columnDesc1).addColumnDescV1(columnDesc2).addColumnDescV1(columnDesc3).addColumnDescV1(columnDesc4)
                .addColumnKey(columnKey1)
                .setReplicaNum(1).setPartitionNum(1)
                .setCompressType(NS.CompressType.kSnappy)
                .build();
        log.info("table info:"+tableInfo);
        boolean result = RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo);
        Assert.assertTrue(result);
        tableNameList.add(tableName);

        long timestamp = System.currentTimeMillis();
        Set<String> putSet = new HashSet<>();
        for(int i=1;i<10;i++) {
            Object[] row = new Object[]{"card"+i, i, 1.0, timestamp+i};
            Assert.assertTrue(masterTableSyncClient.put(tableName, row));
            putSet.add(Arrays.toString(row));
        }
        Object[] row = new Object[]{"card1",1, 1.0, timestamp};
        Assert.assertTrue(masterTableSyncClient.put(tableName, row));
        putSet.add(Arrays.toString(row));
        //add column and check schema
        String columnName1 = "c1";
        String columnType1 = "string";
        String columnName2 = "c2";
        String columnType2 = "int32";
        Assert.assertTrue(masterNsc.addTableField(tableName,columnName1,columnType1));
        Assert.assertTrue(masterNsc.addTableField(tableName,columnName2,columnType2));
        masterClusterClient.refreshRouteTable();
        // add index
        String indexName = "c1_ck";
        Map<String,String> cols = new HashMap<>();
        cols.put("c1","string");
        boolean addIndexOk = masterNsc.addIndex(tableName,indexName, Lists.newArrayList("ts1"), cols);
        Assert.assertTrue(addIndexOk);
        boolean flag = RtidbUtil.checkOPStatus(masterNsc,tableName);
        Assert.assertTrue(flag);
        List<String> indexNames = RtidbUtil.getIndexName(masterNsc,tableName);
        Assert.assertTrue(indexNames.contains(indexName));
        masterClusterClient.refreshRouteTable();

        long timestamp2 = System.currentTimeMillis();
//        Set<String> putSet2 = new HashSet<>();
        for(int i=1;i<10;i++) {
            Object[] row2 = new Object[]{"card2"+i, 100+i, 1.0, timestamp2+i,"c1"+i,i};
            Assert.assertTrue(masterTableSyncClient.put(tableName, row2));
            putSet.add(Arrays.toString(row2));
        }
        Object[] row2 = new Object[]{"card21",101, 1.0, timestamp2,"c11",1};
        Assert.assertTrue(masterTableSyncClient.put(tableName, row2));
        putSet.add(Arrays.toString(row2));
        // make snapshot

        // add replica

        // change leader

        // get scan

    }
}
