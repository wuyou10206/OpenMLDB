package com._4paradigm.openmldb.rtidb_client.test;

import com._4paradigm.openmldb.rtidb_client.common.OpenMLDBTest;
import com._4paradigm.openmldb.rtidb_client.util.AssertUtil;
import com._4paradigm.openmldb.rtidb_client.util.KvIteratorUtil;
import com._4paradigm.openmldb.rtidb_client.util.ResultConvert;
import com._4paradigm.openmldb.rtidb_client.util.RtidbUtil;
import com._4paradigm.rtidb.client.KvIterator;
import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.common.Common.ColumnDesc;
import com._4paradigm.rtidb.common.Common.ColumnKey;
import com._4paradigm.rtidb.common.Common.StorageMode;
import com._4paradigm.rtidb.ns.NS.TableInfo;
import com._4paradigm.rtidb.tablet.Tablet.GetType;
import lombok.extern.log4j.Log4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Log4j
public class AddColumn extends OpenMLDBTest {
    @Test(dataProvider = "storageModeData")
    public void testAddColumn(StorageMode storageMode) throws Exception{
        //create table
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        ColumnDesc columnDesc1 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        ColumnDesc columnDesc2 = ColumnDesc.newBuilder().setName("name").setAddTsIdx(true).setType("string").build();
        ColumnDesc columnDesc3 = ColumnDesc.newBuilder().setName("ts1").setIsTsCol(true).setType("int64").build();
        ColumnDesc columnDesc4 = ColumnDesc.newBuilder().setName("ts2").setIsTsCol(true).setType("int64").build();

        ColumnKey columnKey1 = ColumnKey.newBuilder().setIndexName("card").addColName("card").addTsName("ts1").build();
        ColumnKey columnKey2 = ColumnKey.newBuilder().setIndexName("card_name").addColName("card").addColName("name").addTsName("ts1").build();
        TableInfo tableInfo = TableInfo.newBuilder()
                .setName(tableName)
                .setStorageMode(storageMode)
                .setTtl(1)
                .addColumnDescV1(columnDesc1).addColumnDescV1(columnDesc2).addColumnDescV1(columnDesc3).addColumnDescV1(columnDesc4)
                .addColumnKey(columnKey1).addColumnKey(columnKey2)
                .build();
        log.info("table info:"+tableInfo);
        Assert.assertTrue(RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo));
        tableNameList.add(tableName);

        //先put一部分数据，添加列后再get，检查历史数据
        long timeStamp1 = System.currentTimeMillis();
        log.info("timeStamp1:"+timeStamp1);
        long timeStamp2 = timeStamp1+1000;
        log.info("timeStamp2:"+timeStamp2);
        String key1 = "card1";
        String key2 = "name1";

//        Object[] row22 = new Object[]{key1,key2,timeStamp1+2,timeStamp2+2};
//        Assert.assertTrue(masterTableSyncClient.put(tableName,row22));

        Object[] row1 = new Object[]{key1,key2,timeStamp1+1,timeStamp2+1};
        Object[] expectResult1 = new Object[]{key1,key2,timeStamp1+1,timeStamp2+1,null,null};
        Assert.assertTrue(masterTableSyncClient.put(tableName,row1));

        //add column and check schema
        String columnName1 = "c1";
        String columnType1 = "string";
        String columnName2 = "c2";
        String columnType2 = "int32";
        Assert.assertTrue(masterNsc.addTableField(tableName,columnName1,columnType1));
        Assert.assertTrue(masterNsc.addTableField(tableName,columnName2,columnType2));
        masterClusterClient.refreshRouteTable();
        ColumnDesc columnDesc5 = ColumnDesc.newBuilder().setName(columnName1).setAddTsIdx(false).setType(columnType1).build();
        ColumnDesc columnDesc6 = ColumnDesc.newBuilder().setName(columnName2).setAddTsIdx(false).setType(columnType2).build();
        ArrayList<ColumnDesc> expectColumnList = new ArrayList<>(tableInfo.getColumnDescV1List());
        expectColumnList.add(columnDesc5);
        expectColumnList.add(columnDesc6);
        masterClusterClient.refreshRouteTable();
        List<ColumnDesc> actualColumnDescList = ResultConvert.convertGetSchemaRes(masterTableSyncClient.getSchema(tableName));
//        AssertUtil.assertColumnDescV1List(masterNsc.showTable(tableName).get(0).getColumnDescV1List(),expectColumnList);
        AssertUtil.assertColumnDescV1List(actualColumnDescList,expectColumnList);

        //put(String name, Object[] row)
        Object[] row2 = new Object[]{key1,key2,timeStamp1+2,timeStamp2+2l};
        Object[] expectResult2 = new Object[]{key1,key2,timeStamp1+2,timeStamp2+2,null,null};
        Assert.assertTrue(masterTableSyncClient.put(tableName,row2));
        //put(String tname, Map<String, Object> row)
        Map<String, Object> row3 = new HashMap<>();
        row3.put("card",key1);
        row3.put("name",key2);
        row3.put("ts1",timeStamp1+3);
        row3.put("ts2",timeStamp2+3);
        row3.put("c1","c1_v3");
//        row3.put("c2",null);
        Object[] expectResult3 = new Object[]{key1,key2,timeStamp1+3,timeStamp2+3,"c1_v3",null};
        Assert.assertTrue(masterTableSyncClient.put(tableName,row3));
        Object[] row4 = new Object[]{key1,key2,timeStamp1+4,timeStamp2+4,"c1_v4",4};
        Assert.assertTrue(masterTableSyncClient.put(tableName,row4));

        //getRow(String tname, String key, String idxName, long time, String tsName, Tablet.GetType type,long et, Tablet.GetType etType)
        String indexName1 = tableInfo.getColumnKey(0).getIndexName();
        String tsNameOfIndexName1 = tableInfo.getColumnKey(0).getTsName(0);
        Object[] getRow = masterTableSyncClient.getRow(tableName,key1,indexName1,timeStamp1+1,tsNameOfIndexName1,GetType.kSubKeyLe,timeStamp1,GetType.kSubKeyGe);
        Assert.assertEquals(getRow,expectResult1);
        //getRow(String tname, Object[] keyArr, String idxName, long time, String tsName, Tablet.GetType type,long et, Tablet.GetType etType)
        Object[] combinedKey1 = new Object[]{key1,key2};
        String indexName2 = tableInfo.getColumnKey(1).getIndexName();
        String tsNameOfIndexName2 = tableInfo.getColumnKey(1).getTsName(0);
        Object[] row = masterTableSyncClient.getRow(tableName, combinedKey1, indexName2, timeStamp1 + 2, tsNameOfIndexName2, GetType.kSubKeyLe, timeStamp1, GetType.kSubKeyGe);
        System.out.println("row = " + row);
        Assert.assertEquals(row,expectResult2);
        //public Object[] getRow(String tname, Map<String, Object> keyMap, String idxName, long time, String tsName,Tablet.GetType type, long et, Tablet.GetType etType)
        Map<String,Object> combinedKey2 = new HashMap<>();
        combinedKey2.put("card",key1);
        combinedKey2.put("name",key2);
        Assert.assertEquals(masterTableSyncClient.getRow(tableName,combinedKey2,indexName2,timeStamp1+2,tsNameOfIndexName2,GetType.kSubKeyLe,timeStamp1,GetType.kSubKeyGe),expectResult2);

        //scan(String tname, String key, String idxName, long st, long et, String tsName, int limit)
        KvIterator kvIterator = masterTableSyncClient.scan(tableName,key1,indexName1,timeStamp1+3,timeStamp1,tsNameOfIndexName1,2);
        List<Object[]> ret = KvIteratorUtil.kvIteratorToListForSchema(kvIterator);
        ArrayList<Object[]> expectScanRet = new ArrayList<>();
        expectScanRet.add(expectResult3);
        expectScanRet.add(expectResult2);
        AssertUtil.assertList(ret,expectScanRet);

        //traverse(String tname, String idxName, String tsName)
        kvIterator = masterTableSyncClient.traverse(tableName,indexName2,tsNameOfIndexName2);
        ArrayList<Object[]> expectTraverseRet = new ArrayList<>();
        expectTraverseRet.add(row4);
        expectTraverseRet.add(expectResult3);
        expectTraverseRet.add(expectResult2);
        expectTraverseRet.add(expectResult1);
        AssertUtil.assertList(KvIteratorUtil.kvIteratorToListForSchema(kvIterator),expectTraverseRet);

        //count(String tname, String key, String idxName, String tsName, boolean filter_expired_data)
        long invalidTS = timeStamp1 - tableInfo.getTtl()*1000*60*10 - 10000;
        Object[] row5 = new Object[]{key1,key2,invalidTS,invalidTS,null,null};
        masterTableSyncClient.put(tableName,row5);
        Assert.assertEquals(masterTableSyncClient.count(tableName,key1,indexName1,tsNameOfIndexName1,true),4);
        if (tableInfo.getStorageMode().equals(StorageMode.kMemory)){
            Assert.assertEquals(masterTableSyncClient.count(tableName,key1,indexName1,tsNameOfIndexName1,false),5);
        }else {
            Assert.assertEquals(masterTableSyncClient.count(tableName,key1,indexName1,tsNameOfIndexName1,false),5);    //之前是4
        }
    }

    @Test(dataProvider = "storageModeData")
    public void testAddColumnWithoutTS(StorageMode storageMode) throws Exception{
        //create table
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        ColumnDesc columnDesc1 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        ColumnDesc columnDesc2 = ColumnDesc.newBuilder().setName("name").setAddTsIdx(false).setType("string").build();
        TableInfo tableInfo = TableInfo.newBuilder()
                .setName(tableName)
                .setStorageMode(storageMode)
                .addColumnDescV1(columnDesc1).addColumnDescV1(columnDesc2)
                .build();
        log.info("table info:"+tableInfo);
        Assert.assertTrue(RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo));
        tableNameList.add(tableName);

        //add column and check schema
        String columnName1 = "c1";
        String columnType1 = "string";
        Assert.assertTrue(masterNsc.addTableField(tableName,columnName1,columnType1));
        masterClusterClient.refreshRouteTable();

        //put(String name, long time, Object[] row)
        Object[] row1 = new Object[]{"card1","name1","c1_v1"};
        long timeStamp = System.currentTimeMillis();
        Assert.assertTrue(masterTableSyncClient.put(tableName,timeStamp,row1));
        //put(String tname, long time, Map<String, Object> row)
        Map<String,Object> row2 = new HashMap<>();
        row2.put("card","card2");
        row2.put("name","name2");
        row2.put("c1","c1_v2");
        masterTableSyncClient.put(tableName,timeStamp,row2);

        //get check
        Assert.assertEquals(masterTableSyncClient.getRow(tableName,"card1",timeStamp),row1);
        Assert.assertEquals(new HashSet<>(Arrays.asList(masterTableSyncClient.getRow(tableName,"card2",timeStamp))),new HashSet<>(row2.values()));

        //异步put验证
        //put(String name, long time, Object[] row)
        Assert.assertTrue(masterTableAsyncClient.put(tableName,timeStamp+3,row1).get());
        Assert.assertEquals(masterTableSyncClient.getRow(tableName,"card1",timeStamp+3),row1);
        //put(String tname, long time, Map<String, Object> row)
        Assert.assertTrue(masterTableAsyncClient.put(tableName,timeStamp+4,row2).get());
        Assert.assertEquals(new HashSet<>(Arrays.asList(masterTableSyncClient.getRow(tableName,"card2",timeStamp+4))),new HashSet<>(row2.values()));
    }

    @Test(dataProvider = "storageModeData")
    public void testAddColumnWithAsyncClient(StorageMode storageMode) throws Exception{
        //create table
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        ColumnDesc columnDesc1 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        ColumnDesc columnDesc2 = ColumnDesc.newBuilder().setName("name").setAddTsIdx(true).setType("string").build();
        ColumnDesc columnDesc3 = ColumnDesc.newBuilder().setName("ts1").setIsTsCol(true).setType("int64").build();
        ColumnDesc columnDesc4 = ColumnDesc.newBuilder().setName("ts2").setIsTsCol(true).setType("int64").build();
        ColumnKey columnKey1 = ColumnKey.newBuilder().setIndexName("card").addColName("card").addTsName("ts1").build();
        TableInfo tableInfo = TableInfo.newBuilder()
                .setName(tableName)
                .setStorageMode(storageMode)
                .addColumnDescV1(columnDesc1).addColumnDescV1(columnDesc2).addColumnDescV1(columnDesc3).addColumnDescV1(columnDesc4)
                .addColumnKey(columnKey1)
                .build();
        log.info("table info:"+tableInfo);
        Assert.assertTrue(RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo));
        tableNameList.add(tableName);

        //add column
        String columnName1 = "c1";
        String columnType1 = "string";
        String columnName2 = "c2";
        String columnType2 = "int32";
        Assert.assertTrue(masterNsc.addTableField(tableName,columnName1,columnType1));
        Assert.assertTrue(masterNsc.addTableField(tableName,columnName2,columnType2));
        masterClusterClient.refreshRouteTable();

        //put(String name, Object[] row)
        long timeStamp1 = System.currentTimeMillis();
        log.info("timeStamp1:"+timeStamp1);
        long timeStamp2 = timeStamp1+1000;
        log.info("timeStamp2:"+timeStamp2);
        String key1 = "card1";
        String key2 = "name1";
        Object[] row1 = new Object[]{key1,key2,timeStamp1+1,timeStamp2+1,"c1_v1",1};
        Assert.assertTrue(masterTableAsyncClient.put(tableName,row1).get());
        //put(String tname, Map<String, Object> row)
        Map<String, Object> row2 = new HashMap<>();
        row2.put("card",key1);
        row2.put("name",key2);
        row2.put("ts1",timeStamp1+2);
        row2.put("ts2",timeStamp2+2);
        row2.put("c1","c2_v2");
        row2.put("c2",2);
        Assert.assertTrue(masterTableAsyncClient.put(tableName,row2).get());

        //get(String name, String key, String idxName, long time, String tsName, Tablet.GetType type)
        String indexName = tableInfo.getColumnKey(0).getIndexName();
        String tsName2 = tableInfo.getColumnKey(0).getTsName(0);
        Assert.assertEquals(masterTableAsyncClient.get(tableName,key1,indexName,timeStamp1+1,tsName2,GetType.kSubKeyLe).getRow(),row1);
        Assert.assertEquals(masterTableAsyncClient.get(tableName,key1,indexName,timeStamp1+1,tsName2,GetType.kSubKeyLe).getRow(1, TimeUnit.SECONDS),row1);

        //scan(String name, String key, String idxName, long st, long et, String tsName, int limit)
        KvIterator kvIterator = masterTableAsyncClient.scan(tableName,key1,indexName,timeStamp1+1,0,tsName2,1).get();
        ArrayList<Object[]> expectScanRet = new ArrayList<>();
        expectScanRet.add(row1);
        AssertUtil.assertList(KvIteratorUtil.kvIteratorToListForSchema(kvIterator),expectScanRet);
        kvIterator = masterTableAsyncClient.scan(tableName,key1,indexName,timeStamp1+1,0,tsName2,1).get(1, TimeUnit.SECONDS);
        List<Object[]> list = KvIteratorUtil.kvIteratorToListForSchema(kvIterator);
        AssertUtil.assertList(list,expectScanRet);
    }

    //添加字段前，schema字段数小于128；添加字段后，schema字段数大于等于128
    @Test(dataProvider = "storageModeData")
    public void testAddColMoreThan128(StorageMode storageMode) throws Exception{
        //create table
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        ColumnDesc columnDesc1 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        TableInfo.Builder builder = TableInfo.newBuilder();
        builder.addColumnDescV1(columnDesc1);
        String columnNamePrefix = "c";
        for (int i = 2; i <= 127; i++){
            ColumnDesc columnDesc = ColumnDesc.newBuilder().setName(columnNamePrefix+i).setAddTsIdx(false).setType("string").build();
            builder.addColumnDescV1(columnDesc);
        }
        TableInfo tableInfo = builder
                .setName(tableName)
                .setStorageMode(storageMode)
                .build();
        log.info("table info:"+tableInfo);
        Assert.assertTrue(RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo));
        tableNameList.add(tableName);

        //add column
        String columnName1 = "c128";
        String columnType1 = "string";
        String columnName2 = "c129";
        String columnType2 = "string";
        Assert.assertTrue(masterNsc.addTableField(tableName,columnName1,columnType1));
        Assert.assertTrue(masterNsc.addTableField(tableName,columnName2,columnType2));
        masterClusterClient.refreshRouteTable();

        //put
        Map<String,Object> row = new HashMap<>();
        String key = "card1";
        row.put("card",key);
        for (int i = 2; i <= 129; i++){
            row.put(columnNamePrefix+String.valueOf(i),"v"+String.valueOf(i));
        }
        long timeStamp = System.currentTimeMillis();
        masterTableSyncClient.put(tableName,timeStamp,row);

        //get
        Assert.assertEquals(new HashSet<>(Arrays.asList(masterTableSyncClient.getRow(tableName,key,timeStamp))),new HashSet<>(row.values()));

        //scan
        KvIterator kvIterator = masterTableSyncClient.scan(tableName,key,0,0);
        HashSet<Object> expectRet = new HashSet<>();
        expectRet.add(new HashSet<>(row.values()));
        Assert.assertEquals(KvIteratorUtil.kvIteratorToSet(kvIterator),expectRet);

        //traverse
        kvIterator = masterTableSyncClient.traverse(tableName);
        Assert.assertEquals(KvIteratorUtil.kvIteratorToSet(kvIterator),expectRet);

        //count
        Assert.assertEquals(masterTableSyncClient.count(tableName,key),1);
    }

    //添加字段前，schema字段数大于等于128
    @Test(dataProvider = "storageModeData")
    public void testColMoreThan128BeforeAdd(StorageMode storageMode) throws Exception{
        //create table
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        ColumnDesc columnDesc1 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        TableInfo.Builder builder = TableInfo.newBuilder();
        builder.addColumnDescV1(columnDesc1);
        String columnNamePrefix = "c";
        for (int i = 2; i <= 129; i++){
            ColumnDesc columnDesc = ColumnDesc.newBuilder().setName(columnNamePrefix+i).setAddTsIdx(false).setType("string").build();
            builder.addColumnDescV1(columnDesc);
        }
        TableInfo tableInfo = builder
                .setName(tableName)
                .setStorageMode(storageMode)
                .build();
        log.info("table info:"+tableInfo);
        Assert.assertTrue(RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo));
        tableNameList.add(tableName);

        //add column
        String columnName1 = "c130";
        String columnType1 = "string";
        Assert.assertTrue(masterNsc.addTableField(tableName,columnName1,columnType1));
        masterClusterClient.refreshRouteTable();

        //put
        Map<String,Object> row = new HashMap<>();
        String key = "card1";
        row.put("card",key);
        for (int i = 2; i <= 130; i++){
            row.put(columnNamePrefix+String.valueOf(i),"v"+String.valueOf(i));
        }
        long timeStamp = System.currentTimeMillis();
        masterTableSyncClient.put(tableName,timeStamp,row);

        //get
        Assert.assertEquals(new HashSet<>(Arrays.asList(masterTableSyncClient.getRow(tableName,key,timeStamp))),new HashSet<>(row.values()));

        //scan
        KvIterator kvIterator = masterTableSyncClient.scan(tableName,key,0,0);
        HashSet<Object> expectRet = new HashSet<>();
        expectRet.add(new HashSet<>(row.values()));
        Assert.assertEquals(KvIteratorUtil.kvIteratorToSet(kvIterator),expectRet);

        //traverse
        kvIterator = masterTableSyncClient.traverse(tableName);
        Assert.assertEquals(KvIteratorUtil.kvIteratorToSet(kvIterator),expectRet);

        //count
        Assert.assertEquals(masterTableSyncClient.count(tableName,key),1);
    }

    //重复添加列，两种情况，一种是和原始列重复，一种是后添加的列之间重复
    @Test(dataProvider = "storageModeData")
    public void testAddDupColumn(StorageMode storageMode) throws TabletException {
        //create table
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        ColumnDesc columnDesc1 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        ColumnDesc columnDesc2 = ColumnDesc.newBuilder().setName("f1").setAddTsIdx(false).setType("float").build();
        ColumnDesc columnDesc3 = ColumnDesc.newBuilder().setName("ts1").setIsTsCol(true).setType("int64").build();
        TableInfo tableInfo = TableInfo.newBuilder()
                .setName(tableName)
                .setStorageMode(storageMode)
                .addColumnDescV1(columnDesc1).addColumnDescV1(columnDesc2).addColumnDescV1(columnDesc3)
                .build();
        log.info("table info:"+tableInfo);
        Assert.assertTrue(RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo));
        tableNameList.add(tableName);

        //add column,和原始列重复
        String columnName1 = "card";
        String columnType1 = "string";
        //该方法不抛异常，返回的也是布尔值，只能断言返回值
        Assert.assertFalse(masterNsc.addTableField(tableName,columnName1,columnType1));

        //add column,和后添加的列之间重复
        String columnName2 = "c2";
        String columnType2 = "string";
        Assert.assertTrue(masterNsc.addTableField(tableName,columnName2,columnType2));
        //该方法不抛异常，返回的也是布尔值，只能断言返回值
        Assert.assertFalse(masterNsc.addTableField(tableName,columnName2,columnType2));

        //check schema
        masterClusterClient.refreshRouteTable();
        ColumnDesc columnDesc4 = ColumnDesc.newBuilder().setName(columnName2).setAddTsIdx(false).setType(columnType2).build();
        ArrayList<ColumnDesc> expectColumnList = new ArrayList<>(tableInfo.getColumnDescV1List());
        expectColumnList.add(columnDesc4);
        List<ColumnDesc> actualColumnDescList = ResultConvert.convertGetSchemaRes(masterTableSyncClient.getSchema(tableName));
        AssertUtil.assertColumnDescV1List(actualColumnDescList,expectColumnList);
    }

    //添加字段次数不能超过63
    @Test(dataProvider = "storageModeData")
    public void testMaxColumnCanBeAdd(StorageMode storageMode) throws TabletException {
        //create table
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        ColumnDesc columnDesc1 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        TableInfo tableInfo = TableInfo.newBuilder()
                .setName(tableName)
                .setStorageMode(storageMode)
                .addColumnDescV1(columnDesc1)
                .build();
        log.info("table info:"+tableInfo);
        Assert.assertTrue(RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo));
        tableNameList.add(tableName);

        //add column
        ArrayList<ColumnDesc> expectColumnList = new ArrayList<>(tableInfo.getColumnDescV1List());
        for (int i = 1; i <= 63; i++){
            String columnName = "c"+i;
            String columnType = "string";
            Assert.assertTrue(masterNsc.addTableField(tableName,columnName,columnType));
            ColumnDesc columnDesc = ColumnDesc.newBuilder().setName(columnName).setAddTsIdx(false).setType(columnType).build();
            expectColumnList.add(columnDesc);
        }
        masterClusterClient.refreshRouteTable();
        String columnName = "c64";
        String columnType = "string";
        Assert.assertFalse(masterNsc.addTableField(tableName,columnName,columnType));
        //check schema
        List<ColumnDesc> actualColumnDescList = ResultConvert.convertGetSchemaRes(masterTableSyncClient.getSchema(tableName));
        AssertUtil.assertColumnDescV1List(actualColumnDescList,expectColumnList);
    }

    //有添加字段时，实际列数大于更新后的schema字段数或者小于原始schema字段数
//    @Test(dataProvider = "storageModeData")
//    public void testPutColumnNotEqualSchemaAfterAddCol(StorageMode storageMode) throws TimeoutException, TabletException {
//        //create table
//        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
//        ColumnDesc columnDesc1 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
//        ColumnDesc columnDesc2 = ColumnDesc.newBuilder().setName("ts1").setIsTsCol(true).setType("int64").build();
//        ColumnDesc columnDesc3 = ColumnDesc.newBuilder().setName("c1").setAddTsIdx(false).setType("string").build();
//        TableInfo tableInfo = TableInfo.newBuilder()
//                .setName(tableName)
//                .setStorageMode(storageMode)
//                .addColumnDescV1(columnDesc1).addColumnDescV1(columnDesc2).addColumnDescV1(columnDesc3)
//                .build();
//        log.info("table info:"+tableInfo);
//        Assert.assertTrue(RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo));
//        tableNameList.add(tableName);
//
//        //add column
//        String columnName1 = "c2";
//        String columnType1 = "string";
//        Assert.assertTrue(masterNsc.addTableField(tableName,columnName1,columnType1));
//        masterClusterClient.refreshRouteTable();
//
//        //put列小于schema
//        String key = "k1";
//        long timeStamp = System.currentTimeMillis();
//        Object[] row1 = new Object[]{key,timeStamp,"c1","c2"};
//        RtidbUtil.putSchemaLessThanSchema(masterTableSyncClient,tableName,row1,true);
//        Assert.assertEquals(masterTableSyncClient.getRow(tableName,key,timeStamp),null);
//
//        //put列大于schema
//        Object[] row2 = new Object[]{key,timeStamp,"c1_v1","c2_v2","test"};
//        Object[] expectRet = new Object[]{key,timeStamp,"c1_v1","c2_v2"};
//        Assert.assertTrue(masterTableSyncClient.put(tableName,row2));
//        Assert.assertEquals(masterTableSyncClient.getRow(tableName,key,timeStamp),expectRet);
//    }
}
