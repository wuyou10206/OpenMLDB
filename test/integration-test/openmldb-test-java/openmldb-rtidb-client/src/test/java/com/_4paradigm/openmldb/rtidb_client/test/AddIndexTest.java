package com._4paradigm.openmldb.rtidb_client.test;

import com._4paradigm.openmldb.rtidb_client.common.OpenMLDBTest;

import com._4paradigm.openmldb.rtidb_client.common.RTIDBClient;
import com._4paradigm.openmldb.rtidb_client.util.KvIteratorUtil;
import com._4paradigm.openmldb.rtidb_client.util.RtidbUtil;
import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBGlobalVar;
import com._4paradigm.rtidb.client.KvIterator;
import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.common.Common;
import com._4paradigm.rtidb.ns.NS;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;
import shade.guava.collect.Lists;
import shade.guava.collect.Maps;

import java.util.*;

/**
 * @author zhaowei
 * @date 2020/4/15 下午8:45
 */
@Slf4j
public class AddIndexTest extends OpenMLDBTest {
    // 添加索引成功
    @Test
    public void addIndexShowSchema(){
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
                .build();
        log.info("table info:"+tableInfo);
        boolean result = RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo);
        Assert.assertTrue(result);
        tableNameList.add(tableName);
        String indexName = "mcc_ck";
        Map<String,String> cols = new HashMap<>();
        cols.put("mcc","int32");
        boolean addIndexOk = masterNsc.addIndex(tableName,indexName, Lists.newArrayList("ts1"), cols);
        Assert.assertTrue(addIndexOk);
        boolean flag = RtidbUtil.checkOPStatus(masterNsc,tableName);
        Assert.assertTrue(flag);
        List<String> indexNames = RtidbUtil.getIndexName(masterNsc,tableName);
        Assert.assertTrue(indexNames.contains(indexName));
    }
    // SSD  HDD  不能添加索引
    @Test(dataProvider = "storageModeDataNoMemory")
    public void testSSDAndHDD(Common.StorageMode storageMode){
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        Common.ColumnDesc columnDesc1 = Common.ColumnDesc.newBuilder().setName("card").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc columnDesc2 = Common.ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("int32").build();
        Common.ColumnDesc columnDesc3 = Common.ColumnDesc.newBuilder().setName("sal").setAddTsIdx(false).setType("double").build();
        Common.ColumnDesc columnDesc4 = Common.ColumnDesc.newBuilder().setName("ts1").setIsTsCol(true).setType("int64").build();
        Common.ColumnKey columnKey1 = Common.ColumnKey.newBuilder().setIndexName("card_ck").addColName("card").addTsName("ts1").build();
        NS.TableInfo tableInfo = NS.TableInfo.newBuilder()
                .setName(tableName).setStorageMode(storageMode)
                .addColumnDescV1(columnDesc1).addColumnDescV1(columnDesc2).addColumnDescV1(columnDesc3).addColumnDescV1(columnDesc4)
                .addColumnKey(columnKey1)
                .build();
        log.info("table info:"+tableInfo);
        boolean result = RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo);
        Assert.assertTrue(result);
        tableNameList.add(tableName);
        String indexName = "mcc_ck";
        Map<String,String> cols = new HashMap<>();
        cols.put("mcc","int32");
        boolean addIndexOk = masterNsc.addIndex(tableName,indexName, Lists.newArrayList("ts1"), cols);
        Assert.assertFalse(addIndexOk);
    }
    // 创建内存表且不是用column_key创建索引，增加索引
    @Test
    public void testMemoryIndexTableByClient(){
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        Common.ColumnDesc columnDesc1 = Common.ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        Common.ColumnDesc columnDesc2 = Common.ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("int32").build();
        Common.ColumnDesc columnDesc3 = Common.ColumnDesc.newBuilder().setName("sal").setAddTsIdx(false).setType("double").build();
        Common.ColumnDesc columnDesc4 = Common.ColumnDesc.newBuilder().setName("ts1").setIsTsCol(false).setType("int64").build();
        NS.TableInfo tableInfo = NS.TableInfo.newBuilder()
                .setName(tableName)
                .addColumnDescV1(columnDesc1).addColumnDescV1(columnDesc2).addColumnDescV1(columnDesc3).addColumnDescV1(columnDesc4)
                .build();
        log.info("table info:"+tableInfo);
        boolean result = RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo);
        Assert.assertTrue(result);
        tableNameList.add(tableName);
        String indexName = "mcc_ck";
        Map<String,String> cols = new HashMap<>();
        cols.put("mcc","int32");
        boolean addIndexOk = masterNsc.addIndex(tableName,indexName, Lists.newArrayList("ts1"), cols);
        Assert.assertTrue(addIndexOk);
        boolean flag = RtidbUtil.checkOPStatus(masterNsc,tableName);
        Assert.assertTrue(flag);
        List<String> indexNames = RtidbUtil.getIndexName(masterNsc,tableName);
        Assert.assertTrue(indexNames.contains(indexName));
    }
    @Test
    public void addDeleteAdd(){
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
                .build();
        log.info("table info:"+tableInfo);
        boolean result = RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo);
        Assert.assertTrue(result);
        tableNameList.add(tableName);
        List<String> list = null;
        String indexName = "mcc_ck";
        Map<String,String> cols = new HashMap<>();
        cols.put("mcc","int32");
        boolean addIndexOk = masterNsc.addIndex(tableName,indexName, Lists.newArrayList("ts1"), cols);
        Assert.assertTrue(addIndexOk);
        boolean flag = RtidbUtil.checkOPStatus(masterNsc,tableName);
        Assert.assertTrue(flag);
        List<String> indexNames = RtidbUtil.getIndexName(masterNsc,tableName);
        Assert.assertTrue(indexNames.contains(indexName));
//        String command = String.format("deleteindex %s %s", tableName,indexName);
//        list = RtidbCommandUtil.runNoInteractive(masterInfo,command);
//        Assert.assertEquals(list.get(0),"delete index ok");
//        indexNames = RtidbCommandUtil.getIndexName(masterInfo,tableName);
//        Assert.assertFalse(indexNames.contains("mcc_ck"));
//        RtidbClusterUtil.gc(masterInfo,tableName,2);
//        command = String.format("addindex %s %s %s %s", tableName,indexName,"mcc","ts1");
//        list = RtidbCommandUtil.runNoInteractive(masterInfo,command);
//        Assert.assertEquals(list.get(0),"addindex ok");
//        flag = RtidbJavaUtil.checkOPStatus(tableName);
//        Assert.assertTrue(flag);
//        indexNames = RtidbCommandUtil.getIndexName(masterInfo,tableName);
//        Assert.assertTrue(indexNames.contains(indexName));
    }
    // failed: 表名错误 不存在的列  double列 float列 已经存在的索引
    @Test
    public void testException(){
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        Common.ColumnDesc columnDesc1 = Common.ColumnDesc.newBuilder().setName("card").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc columnDesc2 = Common.ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("int32").build();
        Common.ColumnDesc columnDesc3 = Common.ColumnDesc.newBuilder().setName("sal").setAddTsIdx(false).setType("double").build();
        Common.ColumnDesc columnDesc4 = Common.ColumnDesc.newBuilder().setName("ts1").setIsTsCol(true).setType("int64").build();
        Common.ColumnDesc columnDesc5 = Common.ColumnDesc.newBuilder().setName("sal2").setAddTsIdx(false).setType("float").build();
        Common.ColumnKey columnKey1 = Common.ColumnKey.newBuilder().setIndexName("card_ck").addColName("card").addTsName("ts1").build();
        NS.TableInfo tableInfo = NS.TableInfo.newBuilder()
                .setName(tableName)
                .addColumnDescV1(columnDesc1).addColumnDescV1(columnDesc2).addColumnDescV1(columnDesc3).addColumnDescV1(columnDesc4).addColumnDescV1(columnDesc5)
                .addColumnKey(columnKey1)
                .build();
        log.info("table info:"+tableInfo);
        boolean result = RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo);
        Assert.assertTrue(result);
        tableNameList.add(tableName);
        String indexName = "mcc_ck";
        Map<String,String> cols = new HashMap<>();
        cols.put("mcc","int32");
        boolean addIndexOk = masterNsc.addIndex(tableName+"1",indexName, Lists.newArrayList("ts1"), cols);
        Assert.assertFalse(addIndexOk);
        cols = new HashMap<>();
        cols.put("mcc1","int32");
        addIndexOk = masterNsc.addIndex(tableName,indexName, Lists.newArrayList("ts1"), cols);
        Assert.assertTrue(addIndexOk);
        boolean flag = RtidbUtil.checkOPStatus(masterNsc,tableName);
        Assert.assertTrue(flag);
        List<String> indexNames = RtidbUtil.getIndexName(masterNsc,tableName);
        Assert.assertTrue(indexNames.contains(indexName));
        indexName = "sal_ck";
        cols = new HashMap<>();
        cols.put("sal","double");
        addIndexOk = masterNsc.addIndex(tableName,indexName, Lists.newArrayList("ts1"), cols);
        Assert.assertFalse(addIndexOk);
        indexName = "sal2_ck";
        cols = new HashMap<>();
        cols.put("sal2","float");
        addIndexOk = masterNsc.addIndex(tableName,indexName, Lists.newArrayList("ts1"), cols);
        Assert.assertFalse(addIndexOk);
        indexName = "card_ck";
        cols = new HashMap<>();
        cols.put("mcc","int32");
        addIndexOk = masterNsc.addIndex(tableName,indexName, Lists.newArrayList("ts1"), cols);
        Assert.assertFalse(addIndexOk);
    }
    @Test
    public void addIndexSameName(){
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
                .build();
        log.info("table info:"+tableInfo);
        boolean result = RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo);
        Assert.assertTrue(result);
        tableNameList.add(tableName);
        String indexName = "mcc";
        Map<String,String> cols = new HashMap<>();
        cols.put("mcc","int32");
        boolean addIndexOk = masterNsc.addIndex(tableName,indexName, Lists.newArrayList("ts1"), cols);
        Assert.assertTrue(addIndexOk);
        boolean flag = RtidbUtil.checkOPStatus(masterNsc,tableName);
        Assert.assertTrue(flag);
        List<String> indexNames = RtidbUtil.getIndexName(masterNsc,tableName);
        Assert.assertTrue(indexNames.contains(indexName));
    }
    @Test
    public void addIndexOnlyHasIndexName(){
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        Common.ColumnDesc columnDesc1 = Common.ColumnDesc.newBuilder().setName("card").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc columnDesc2 = Common.ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("int32").build();
        Common.ColumnDesc columnDesc3 = Common.ColumnDesc.newBuilder().setName("sal").setAddTsIdx(false).setType("double").build();
        Common.ColumnKey columnKey1 = Common.ColumnKey.newBuilder().setIndexName("card_ck").addColName("card").build();
        NS.TableInfo tableInfo = NS.TableInfo.newBuilder()
                .setName(tableName)
                .addColumnDescV1(columnDesc1).addColumnDescV1(columnDesc2).addColumnDescV1(columnDesc3)
                .addColumnKey(columnKey1)
                .build();
        log.info("table info:"+tableInfo);
        boolean result = RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo);
        Assert.assertTrue(result);
        tableNameList.add(tableName);
        String indexName = "mcc";
        Map<String,String> cols = new HashMap<>();
        cols.put("mcc","int32");
        boolean addIndexOk = masterNsc.addIndex(tableName,indexName, Lists.newArrayList(), Maps.newHashMap());
        Assert.assertTrue(addIndexOk);
        boolean flag = RtidbUtil.checkOPStatus(masterNsc,tableName);
        Assert.assertTrue(flag);
        List<String> indexNames = RtidbUtil.getIndexName(masterNsc,tableName);
        Assert.assertTrue(indexNames.contains(indexName));
        indexName = "mcc_ck";
        addIndexOk = masterNsc.addIndex(tableName,indexName, Lists.newArrayList(), Maps.newHashMap());
        Assert.assertFalse(addIndexOk);
    }
    @Test
    public void noTs(){
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        Common.ColumnDesc columnDesc1 = Common.ColumnDesc.newBuilder().setName("card").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc columnDesc2 = Common.ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("int32").build();
        Common.ColumnDesc columnDesc3 = Common.ColumnDesc.newBuilder().setName("sal").setAddTsIdx(false).setType("int64").build();
        Common.ColumnKey columnKey1 = Common.ColumnKey.newBuilder().setIndexName("card_ck").addColName("card").build();
        NS.TableInfo tableInfo = NS.TableInfo.newBuilder()
                .setName(tableName)
                .addColumnDescV1(columnDesc1).addColumnDescV1(columnDesc2).addColumnDescV1(columnDesc3)
                .addColumnKey(columnKey1)
                .build();
        log.info("table info:"+tableInfo);
        boolean result = RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo);
        Assert.assertTrue(result);
        tableNameList.add(tableName);
        String indexName = "mcc";
        Map<String,String> cols = new HashMap<>();
        cols.put("mcc","int32");
        boolean addIndexOk = masterNsc.addIndex(tableName,indexName, Lists.newArrayList(), cols);
        Assert.assertTrue(addIndexOk);
        boolean flag = RtidbUtil.checkOPStatus(masterNsc,tableName);
        Assert.assertTrue(flag);
        List<String> indexNames = RtidbUtil.getIndexName(masterNsc,tableName);
        Assert.assertTrue(indexNames.contains(indexName));
        indexName = "mcc_ck";
        cols = new HashMap<>();
        cols.put("mcc","int32");
        addIndexOk = masterNsc.addIndex(tableName,indexName, Lists.newArrayList("sal"), cols);
        Assert.assertTrue(addIndexOk);
        flag = RtidbUtil.checkOPStatus(masterNsc,tableName);
        Assert.assertTrue(flag);
        indexNames = RtidbUtil.getIndexName(masterNsc,tableName);
        Assert.assertTrue(indexNames.contains(indexName));

    }
    @Test
    public void hasTs(){
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
                .build();
        log.info("table info:"+tableInfo);
        boolean result = RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo);
        Assert.assertTrue(result);
        tableNameList.add(tableName);
        String indexName = "mcc_ck";
        Map<String,String> cols = new HashMap<>();
        cols.put("mcc","int32");
        boolean addIndexOk = masterNsc.addIndex(tableName,indexName, Lists.newArrayList(), cols);
        Assert.assertTrue(addIndexOk);
        boolean flag = RtidbUtil.checkOPStatus(masterNsc,tableName);
        Assert.assertTrue(flag);
        List<String> indexNames = RtidbUtil.getIndexName(masterNsc,tableName);
        Assert.assertTrue(indexNames.contains(indexName));
    }
    @Test
    public void testNormal(){
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        Common.ColumnDesc columnDesc1 = Common.ColumnDesc.newBuilder().setName("card").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc columnDesc2 = Common.ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("int32").build();
        Common.ColumnDesc columnDesc3 = Common.ColumnDesc.newBuilder().setName("sal").setAddTsIdx(false).setType("double").build();
        Common.ColumnDesc columnDesc4 = Common.ColumnDesc.newBuilder().setName("ts1").setIsTsCol(true).setType("int64").build();
        Common.ColumnDesc columnDesc5 = Common.ColumnDesc.newBuilder().setName("ts2").setIsTsCol(true).setType("int64").build();
        Common.ColumnKey columnKey1 = Common.ColumnKey.newBuilder().setIndexName("card_ck").addColName("card").addTsName("ts1").build();
        NS.TableInfo tableInfo = NS.TableInfo.newBuilder()
                .setName(tableName)
                .addColumnDescV1(columnDesc1).addColumnDescV1(columnDesc2).addColumnDescV1(columnDesc3).addColumnDescV1(columnDesc4).addColumnDescV1(columnDesc5)
                .addColumnKey(columnKey1)
                .build();
        log.info("table info:"+tableInfo);
        boolean result = RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo);
        Assert.assertTrue(result);
        tableNameList.add(tableName);
        String indexName = "mcc_ck";
        Map<String,String> cols = new HashMap<>();
        cols.put("mcc","int32");
        boolean addIndexOk = masterNsc.addIndex(tableName,indexName, Lists.newArrayList("ts1","ts2"), cols);
        Assert.assertFalse(addIndexOk);

        indexName = "mcc_card";
        cols = new HashMap<>();
        cols.put("card","string");
        cols.put("mcc","int32");
        addIndexOk = masterNsc.addIndex(tableName,indexName, Lists.newArrayList("ts1"), cols);
        Assert.assertTrue(addIndexOk);
        boolean flag = RtidbUtil.checkOPStatus(masterNsc,tableName);
        Assert.assertTrue(flag);
        List<String> indexNames = RtidbUtil.getIndexName(masterNsc,tableName);
        Assert.assertTrue(indexNames.contains(indexName));

        indexName = "mcc_card2";
        cols = new HashMap<>();
        cols.put("card","string");
        cols.put("mcc","int32");
        addIndexOk = masterNsc.addIndex(tableName,indexName, Lists.newArrayList("ts1","ts2"), cols);
        Assert.assertFalse(addIndexOk);

        indexName = "mcc_card3";
        cols = new HashMap<>();
        cols.put("card","string");
        cols.put("mcc","int32");
        addIndexOk = masterNsc.addIndex(tableName,indexName, Lists.newArrayList("ts1"), cols);
        Assert.assertFalse(addIndexOk);

        indexName = "mcc4";
        cols = new HashMap<>();
        cols.put("mcc","int32");
        addIndexOk = masterNsc.addIndex(tableName,indexName, Lists.newArrayList("ts1"), cols);
        Assert.assertTrue(addIndexOk);
        flag = RtidbUtil.checkOPStatus(masterNsc,tableName);
        Assert.assertTrue(flag);
        indexNames = RtidbUtil.getIndexName(masterNsc,tableName);
        Assert.assertTrue(indexNames.contains(indexName));

        indexName = "mcc5";
        cols = new HashMap<>();
        cols.put("mcc","int32");
        addIndexOk = masterNsc.addIndex(tableName,indexName, Lists.newArrayList("ts2"), cols);
        Assert.assertTrue(addIndexOk);
        flag = RtidbUtil.checkOPStatus(masterNsc,tableName);
        Assert.assertTrue(flag);
        indexNames = RtidbUtil.getIndexName(masterNsc,tableName);
        Assert.assertTrue(indexNames.contains(indexName));

        indexName = "mcc4";
        cols = new HashMap<>();
        cols.put("mcc","int32");
        addIndexOk = masterNsc.addIndex(tableName,indexName, Lists.newArrayList("ts1"), cols);
        Assert.assertFalse(addIndexOk);

    }
    // 创建表-put数据-添加索引-根据新索引get/scan/traverse-在put数据-根据新索引get/scan/traverse
    @Test
    public void addIndexOperation() throws Exception {
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
        String indexName = "mcc_ck";
        Map<String,String> cols = new HashMap<>();
        cols.put("mcc","int32");
        boolean addIndexOk = masterNsc.addIndex(tableName,indexName, Lists.newArrayList("ts1"), cols);
        Assert.assertTrue(addIndexOk);
        boolean flag = RtidbUtil.checkOPStatus(masterNsc,tableName);
        Assert.assertTrue(flag);
        List<String> indexNames = RtidbUtil.getIndexName(masterNsc,tableName);
        Assert.assertTrue(indexNames.contains(indexName));
        masterClusterClient.refreshRouteTable();

        Object[] actualRow = masterTableSyncClient.getRow(tableName, "1", indexName,0);
        Assert.assertEquals(String.valueOf(actualRow[1]),"1");
        KvIterator scanIterator = masterTableSyncClient.scan(tableName, "1", indexName, 0,0);
        List<Object[]> scanActualRows = KvIteratorUtil.kvIteratorToListForSchema(scanIterator);
        for(Object[] scanRow:scanActualRows) {
            Assert.assertEquals(String.valueOf(scanRow[1]), "1");
        }
        Assert.assertEquals(scanActualRows.size(),2);
        KvIterator it = masterTableSyncClient.traverse(tableName, indexName);
        Set<String>  traverseResultSet = new HashSet<>();
        while (it.valid()) {
            // 一次迭代只能调用一次getDecodedValue
            row = it.getDecodedValue();
            traverseResultSet.add(Arrays.toString(row));
            it.next();
        }
        Assert.assertEquals(traverseResultSet,putSet);

        long timestamp2 = System.currentTimeMillis();
//        Set<String> putSet2 = new HashSet<>();
        for(int i=1;i<10;i++) {
            Object[] row2 = new Object[]{"card2"+i, 100+i, 1.0, timestamp2+i};
            Assert.assertTrue(masterTableSyncClient.put(tableName, row2));
            putSet.add(Arrays.toString(row2));
        }
        Object[] row2 = new Object[]{"card21",101, 1.0, timestamp2};
        Assert.assertTrue(masterTableSyncClient.put(tableName, row2));
        putSet.add(Arrays.toString(row2));
        Object[] actualRow2 = masterTableSyncClient.getRow(tableName, "101", indexName,0);
        Assert.assertEquals(String.valueOf(actualRow2[1]),"101");
        KvIterator scanIterator2 = masterTableSyncClient.scan(tableName, "101", indexName, 0,0);
        List<Object[]> scanActualRows2 = KvIteratorUtil.kvIteratorToListForSchema(scanIterator2);
        for(Object[] scanRow:scanActualRows2) {
            Assert.assertEquals(String.valueOf(scanRow[1]), "101");
        }
        Assert.assertEquals(scanActualRows2.size(),2);
        KvIterator it2 = masterTableSyncClient.traverse(tableName, indexName);
        Set<String>  traverseResultSet2 = new HashSet<>();
        while (it2.valid()) {
            // 一次迭代只能调用一次getDecodedValue
            row2 = it2.getDecodedValue();
            traverseResultSet2.add(Arrays.toString(row2));
            it2.next();
        }
        Assert.assertEquals(traverseResultSet2,putSet);
    }
    // put-delete pk-add index-根据新索引get 删除的pk
    @Test
    public void testDeletePK() throws Exception{
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        Common.ColumnDesc columnDesc1 = Common.ColumnDesc.newBuilder().setName("card").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc columnDesc2 = Common.ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("int32").build();
        Common.ColumnDesc columnDesc3 = Common.ColumnDesc.newBuilder().setName("sal").setAddTsIdx(false).setType("int32").build();
        Common.ColumnDesc columnDesc4 = Common.ColumnDesc.newBuilder().setName("ts1").setIsTsCol(true).setType("int64").build();
        Common.ColumnKey columnKey1 = Common.ColumnKey.newBuilder().setIndexName("card_ck").addColName("card").addTsName("ts1").build();
        Common.ColumnKey columnKey2 = Common.ColumnKey.newBuilder().setIndexName("mcc_ck").addColName("mcc").addTsName("ts1").build();
        NS.TableInfo tableInfo = NS.TableInfo.newBuilder()
                .setName(tableName)
                .addColumnDescV1(columnDesc1).addColumnDescV1(columnDesc2).addColumnDescV1(columnDesc3).addColumnDescV1(columnDesc4)
                .addColumnKey(columnKey1).addColumnKey(columnKey2)
                .build();
        log.info("table info:"+tableInfo);
        boolean result = RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo);
        Assert.assertTrue(result);
        tableNameList.add(tableName);

        long timestamp = System.currentTimeMillis();
        for(int i=1;i<10;i++) {
            Object[] row = new Object[]{"card"+i, i, i, timestamp+i};
            Assert.assertTrue(masterTableSyncClient.put(tableName, row));
        }

        boolean deleteOk = masterTableSyncClient.delete(tableName, "card1", "card_ck");
        Assert.assertTrue(deleteOk);
        deleteOk = masterTableSyncClient.delete(tableName, "1", "mcc_ck");
        Assert.assertTrue(deleteOk);
        deleteOk = masterTableSyncClient.delete(tableName, "2", "mcc_ck");
        Assert.assertTrue(deleteOk);

        String indexName = "sal_ck";
        Map<String,String> cols = new HashMap<>();
        cols.put("sal","int32");
        boolean addIndexOk = masterNsc.addIndex(tableName,indexName, Lists.newArrayList("ts1"), cols);
        Assert.assertTrue(addIndexOk);
        boolean flag = RtidbUtil.checkOPStatus(masterNsc,tableName);
        Assert.assertTrue(flag);
        List<String> indexNames = RtidbUtil.getIndexName(masterNsc,tableName);
        Assert.assertTrue(indexNames.contains(indexName));

        Object[] actualRow = masterTableSyncClient.getRow(tableName, "1", indexName,0);
        Assert.assertEquals(actualRow,null);
        actualRow = masterTableSyncClient.getRow(tableName, "2", indexName,0);
        Assert.assertEquals(actualRow,null);
    }
    // 从节点读取
    @Test
    public void followerGetAndScan() throws Exception {
        RTIDBClient rtidbClient = new RTIDBClient(OpenMLDBGlobalVar.mainInfo.getZk_cluster(),OpenMLDBGlobalVar.mainInfo.getZk_root_path(), TableHandler.ReadStrategy.kReadFollower);
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
                .build();
        log.info("table info:"+tableInfo);
        boolean result = RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo);
        rtidbClient.getClusterClient().refreshRouteTable();
        Assert.assertTrue(result);
        tableNameList.add(tableName);

        long timestamp = System.currentTimeMillis();
        Set<String> putSet = new HashSet<>();
        for(int i=1;i<10;i++) {
            Object[] row = new Object[]{"card"+i, i, 1.0, timestamp+i};
            Assert.assertTrue(rtidbClient.getTableSyncClient().put(tableName, row));
            putSet.add(Arrays.toString(row));
        }

        String indexName = "mcc_ck";
        Map<String,String> cols = new HashMap<>();
        cols.put("mcc","int32");
        boolean addIndexOk = masterNsc.addIndex(tableName,indexName, Lists.newArrayList("ts1"), cols);
        Assert.assertTrue(addIndexOk);
        boolean flag = RtidbUtil.checkOPStatus(masterNsc,tableName);
        Assert.assertTrue(flag);
        List<String> indexNames = RtidbUtil.getIndexName(masterNsc,tableName);
        Assert.assertTrue(indexNames.contains(indexName));
        masterClusterClient.refreshRouteTable();

        long timestamp2 = System.currentTimeMillis();
        for(int i=1;i<10;i++) {
            Object[] row2 = new Object[]{"card2"+i, 100+i, 1.0, timestamp2+i};
            Assert.assertTrue(rtidbClient.getTableSyncClient().put(tableName, row2));
            putSet.add(Arrays.toString(row2));
        }
        Object[] row2 = new Object[]{"card21",101, 1.0, timestamp2};
        Assert.assertTrue(rtidbClient.getTableSyncClient().put(tableName, row2));
        putSet.add(Arrays.toString(row2));
        Object[] getRow = rtidbClient.getTableSyncClient().getRow(tableName,"1",indexName);
        Assert.assertEquals(getRow[1],1);
        getRow = rtidbClient.getTableSyncClient().getRow(tableName,"101",indexName);
        Assert.assertEquals(getRow[1],101);

        KvIterator it = rtidbClient.getTableSyncClient().scan(tableName,"1",indexName,0,0);
        while(it.valid()){
            Object[] scanRow = it.getDecodedValue();
            Assert.assertEquals(scanRow[1], 1);
            it.next();
        }
        Assert.assertEquals(it.getCount(),1);
        it = rtidbClient.getTableSyncClient().scan(tableName,"101",indexName,0,0);
        while(it.valid()){
            Object[] scanRow = it.getDecodedValue();
            Assert.assertEquals(scanRow[1], 101);
            it.next();
        }
        Assert.assertEquals(it.getCount(),2);

        KvIterator it2 = rtidbClient.getTableSyncClient().traverse(tableName, indexName);
        Set<String>  traverseResultSet = new HashSet<>();
        while (it2.valid()) {
            // 一次迭代只能调用一次getDecodedValue
            row2 = it2.getDecodedValue();
            traverseResultSet.add(Arrays.toString(row2));
            it2.next();
        }
        Assert.assertEquals(traverseResultSet,putSet);
    }
    // null put-add index-新索引 get/scan- put-新索引 get/scan/count
    @Test(dataProvider = "formatVersion")
    public void addIndexByNull(int formatVersion) throws Exception {
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        Common.ColumnDesc columnDesc1 = Common.ColumnDesc.newBuilder().setName("card").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc columnDesc2 = Common.ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc columnDesc3 = Common.ColumnDesc.newBuilder().setName("sal").setAddTsIdx(false).setType("double").build();
        Common.ColumnDesc columnDesc4 = Common.ColumnDesc.newBuilder().setName("ts1").setIsTsCol(true).setType("int64").build();
        Common.ColumnKey columnKey1 = Common.ColumnKey.newBuilder().setIndexName("card_ck").addColName("card").addTsName("ts1").build();
        NS.TableInfo tableInfo = NS.TableInfo.newBuilder()
                .setName(tableName).setFormatVersion(formatVersion)
                .addColumnDescV1(columnDesc1).addColumnDescV1(columnDesc2).addColumnDescV1(columnDesc3).addColumnDescV1(columnDesc4)
                .addColumnKey(columnKey1)
                .build();
        log.info("table info:"+tableInfo);
        boolean result = RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo);
        Assert.assertTrue(result);
        tableNameList.add(tableName);

        long timestamp = System.currentTimeMillis();
        for(int i=1;i<10;i++) {
            Object[] row = new Object[]{"card"+i, i+"", 1.0, timestamp+i};
            Assert.assertTrue(masterTableSyncClient.put(tableName, row));
        }
        Object[] row = new Object[]{"card1",null, 2.0, timestamp};
        Assert.assertTrue(masterTableSyncClient.put(tableName, row));
        int count = masterTableSyncClient.count(tableName, "card1", "card_ck", 0,0);
        Assert.assertEquals(count,2);
        String indexName = "mcc_ck";
        Map<String,String> cols = new HashMap<>();
        cols.put("mcc","string");
        boolean addIndexOk = masterNsc.addIndex(tableName,indexName, Lists.newArrayList("ts1"), cols);
        Assert.assertTrue(addIndexOk);
        boolean flag = RtidbUtil.checkOPStatus(masterNsc,tableName);
        Assert.assertTrue(flag);
        List<String> indexNames = RtidbUtil.getIndexName(masterNsc,tableName);
        Assert.assertTrue(indexNames.contains(indexName));

        Object[] getRow = masterTableSyncClient.getRow(tableName,null,indexName,0);
        Assert.assertEquals(getRow[1],null);
        KvIterator scanIterator = masterTableSyncClient.scan(tableName, null, indexName, 0,0);
        List<Object[]> scanActualRows = KvIteratorUtil.kvIteratorToListForSchema(scanIterator);
        for(Object[] scanRow:scanActualRows) {
            Assert.assertEquals(scanRow[1], null);
        }
        row = new Object[]{"card1",null, 3.0, timestamp+1};
        Assert.assertTrue(masterTableSyncClient.put(tableName, row));
        count = masterTableSyncClient.count(tableName, null, indexName, 0,0);
        Assert.assertEquals(count,2);
        getRow = masterTableSyncClient.getRow(tableName,null,indexName,0);
        Assert.assertEquals(getRow[1],null);
        scanIterator = masterTableSyncClient.scan(tableName, null, indexName, 0,0);
        scanActualRows = KvIteratorUtil.kvIteratorToListForSchema(scanIterator);
        for(Object[] scanRow:scanActualRows) {
            Assert.assertEquals(scanRow[1], null);
        }
        Assert.assertEquals(scanActualRows.size(),2);
        Object[] ret = masterTableSyncClient.getRow(tableName,null, indexName, 0);
        Assert.assertEquals(ret[1],null);
        List<Object[]> scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tableName,null,indexName,0,0));
        Assert.assertEquals(scanList.size(),2);
        for(Object[] scanRow:scanList) {
            Assert.assertEquals(scanRow[1], null);
        }
        count = masterTableSyncClient.count(tableName,null,indexName);
        Assert.assertEquals(count,2);
    }
    @Test(dataProvider = "formatVersion")
    public void addIndexByEmptyString(int formatVersion) throws Exception {
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        Common.ColumnDesc columnDesc1 = Common.ColumnDesc.newBuilder().setName("card").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc columnDesc2 = Common.ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc columnDesc3 = Common.ColumnDesc.newBuilder().setName("sal").setAddTsIdx(false).setType("double").build();
        Common.ColumnDesc columnDesc4 = Common.ColumnDesc.newBuilder().setName("ts1").setIsTsCol(true).setType("int64").build();
        Common.ColumnKey columnKey1 = Common.ColumnKey.newBuilder().setIndexName("card_ck").addColName("card").addTsName("ts1").build();
        NS.TableInfo tableInfo = NS.TableInfo.newBuilder()
                .setName(tableName).setFormatVersion(formatVersion)
                .addColumnDescV1(columnDesc1).addColumnDescV1(columnDesc2).addColumnDescV1(columnDesc3).addColumnDescV1(columnDesc4)
                .addColumnKey(columnKey1)
                .build();
        log.info("table info:"+tableInfo);
        boolean result = RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo);
        Assert.assertTrue(result);
        tableNameList.add(tableName);

        long timestamp = System.currentTimeMillis();
        for(int i=1;i<10;i++) {
            Object[] row = new Object[]{"card"+i, i+"", 1.0, timestamp+i};
            Assert.assertTrue(masterTableSyncClient.put(tableName, row));
        }
        Object[] row = new Object[]{"card1","", 2.0, timestamp};
        Assert.assertTrue(masterTableSyncClient.put(tableName, row));
        int count = masterTableSyncClient.count(tableName,"card1","card_ck");
        Assert.assertEquals(count,2);
        String indexName = "mcc_ck";
        Map<String,String> cols = new HashMap<>();
        cols.put("mcc","string");
        boolean addIndexOk = masterNsc.addIndex(tableName,indexName, Lists.newArrayList("ts1"), cols);
        Assert.assertTrue(addIndexOk);
        boolean flag = RtidbUtil.checkOPStatus(masterNsc,tableName);
        Assert.assertTrue(flag);
        List<String> indexNames = RtidbUtil.getIndexName(masterNsc,tableName);
        Assert.assertTrue(indexNames.contains(indexName));

        Object[] getRow = masterTableSyncClient.getRow(tableName,"",indexName,0);
        Assert.assertEquals(getRow[1],"");
        KvIterator scanIterator = masterTableSyncClient.scan(tableName, "", indexName, 0,0);
        List<Object[]> scanActualRows = KvIteratorUtil.kvIteratorToListForSchema(scanIterator);
        for(Object[] scanRow:scanActualRows) {
            Assert.assertEquals(scanRow[1], "");
        }
        row = new Object[]{"card1","", 3.0, timestamp};
        Assert.assertTrue(masterTableSyncClient.put(tableName, row));
        count = masterTableSyncClient.count(tableName,"",indexName);
        Assert.assertEquals(count,2);

        getRow = masterTableSyncClient.getRow(tableName,"",indexName,0);
        Assert.assertEquals(getRow[1],"");
        scanIterator = masterTableSyncClient.scan(tableName, "", indexName, 0,0);
        scanActualRows = KvIteratorUtil.kvIteratorToListForSchema(scanIterator);
        for(Object[] scanRow:scanActualRows) {
            Assert.assertEquals(scanRow[1], "");
        }
        Assert.assertEquals(scanActualRows.size(),2);
        Object[] ret = masterTableSyncClient.getRow(tableName,"", indexName, 0);
        Assert.assertEquals(ret[1],"");
        List<Object[]> scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tableName, "", indexName, 0,0));;
        Assert.assertEquals(scanList.size(),2);
        for(Object[] scanRow:scanList) {
            Assert.assertEquals(scanRow[1], "");
        }
        count = masterTableSyncClient.count(tableName,"",indexName);
        Assert.assertEquals(count,2);
    }
    @Test
    public void addIndexByOnePartition(){
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        Common.ColumnDesc columnDesc1 = Common.ColumnDesc.newBuilder().setName("card").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc columnDesc2 = Common.ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("int32").build();
        Common.ColumnDesc columnDesc3 = Common.ColumnDesc.newBuilder().setName("sal").setAddTsIdx(false).setType("double").build();
        Common.ColumnDesc columnDesc4 = Common.ColumnDesc.newBuilder().setName("ts1").setIsTsCol(true).setType("int64").build();
        Common.ColumnKey columnKey1 = Common.ColumnKey.newBuilder().setIndexName("card_ck").addColName("card").addTsName("ts1").build();
        NS.TableInfo tableInfo = NS.TableInfo.newBuilder()
                .setName(tableName).setPartitionNum(1)
                .addColumnDescV1(columnDesc1).addColumnDescV1(columnDesc2).addColumnDescV1(columnDesc3).addColumnDescV1(columnDesc4)
                .addColumnKey(columnKey1)
                .build();
        log.info("table info:"+tableInfo);
        boolean result = RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo);
        Assert.assertTrue(result);
        tableNameList.add(tableName);
        String indexName = "mcc_ck";
        Map<String,String> cols = new HashMap<>();
        cols.put("mcc","string");
        boolean addIndexOk = masterNsc.addIndex(tableName,indexName, Lists.newArrayList("ts1"), cols);
        Assert.assertTrue(addIndexOk);
        boolean flag = RtidbUtil.checkOPStatus(masterNsc,tableName);
        Assert.assertTrue(flag);
        List<String> indexNames = RtidbUtil.getIndexName(masterNsc,tableName);
        Assert.assertTrue(indexNames.contains(indexName));
    }

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
                .setReplicaNum(2).setPartitionNum(2)
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
