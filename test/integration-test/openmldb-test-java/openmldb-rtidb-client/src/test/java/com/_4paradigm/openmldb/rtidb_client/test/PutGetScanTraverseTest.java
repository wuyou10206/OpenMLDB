package com._4paradigm.openmldb.rtidb_client.test;

import com._4paradigm.openmldb.rtidb_client.common.OpenMLDBTest;
import com._4paradigm.openmldb.rtidb_client.util.AssertUtil;
import com._4paradigm.openmldb.rtidb_client.util.DataUtil;
import com._4paradigm.openmldb.rtidb_client.util.KvIteratorUtil;
import com._4paradigm.rtidb.client.GetOption;
import com._4paradigm.rtidb.client.KvIterator;
import com._4paradigm.rtidb.client.ScanOption;
import com._4paradigm.rtidb.ns.NS;
import com._4paradigm.rtidb.tablet.Tablet;
import io.qameta.allure.Description;
import lombok.extern.log4j.Log4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.*;

/**
 * Created by zhangguanglin on 2019/9/25.
 */
@Log4j
public class PutGetScanTraverseTest extends OpenMLDBTest {
    @Description("索引表-put-get-scan")
    @Test(dataProvider = "storageModeData")
    public void testIndexTable(String storageMode) throws Exception{
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        String tableDDL = "create table %s(c1 string,c2 smallint,c3 int,c4 bigint,c5 float,c6 double,c7 timestamp,c8 date,c9 bool," +
                "index(key=(c1),ts=c7))options(partitionnum=2,replicanum=3,storage_mode='%s');";
        tableDDL = String.format(tableDDL,tableName,storageMode);
        sdkClient.execute(tableDDL);
        tableNameList.add(tableName);
        List<Object[]> dataList = new ArrayList<>();
        List<Object[]> bbList = new ArrayList<>();
        // put(String tname, Object[] row)
        // getRow(String tname, String key, String idxName)
        // scan(String tname, String key, String idxName, int limit)
        for(int i=0;i<10;i++) {
            short s = 1;
            Date date = Date.valueOf("2020-05-01");
            Timestamp timestamp = new Timestamp(1590738989000L+i);
            Object[] data = new Object[]{"aa",s, 2, 3L, 1.1F, 2.1,timestamp ,date,true};
            masterTableSyncClient.put(tableName,data);
            dataList.add(data);
        }
        Collections.reverse(dataList);
        NS.TableInfo tableInfo = masterNsc.showTable(tableName).get(0);
        String indexName = tableInfo.getColumnKey(0).getIndexName();
        int tid = tableInfo.getTid();
        log.info("indexName:"+indexName);
        log.info("tid:" + tid);
        Object[] actualRow = masterTableSyncClient.getRow(tableName, "aa", indexName);
        Object[] expectRow = dataList.get(0);
        // DateTime -> Timestamp
        DataUtil.convertData(actualRow);
        Assert.assertEquals(actualRow,expectRow);

        KvIterator scanIterator = masterTableSyncClient.scan(tableName, "aa", indexName, 10);
        List<Object[]> scanActualRows = KvIteratorUtil.kvIteratorToListForSchema(scanIterator);
        DataUtil.convertData(scanActualRows);
        AssertUtil.assertList(scanActualRows,dataList);
        // traverse(String tname)
        {
            KvIterator traverseIterator = masterTableSyncClient.traverse(tableName);
            List<Object[]> traverseActualRows = KvIteratorUtil.kvIteratorToListForSchema(traverseIterator);
            DataUtil.convertData(traverseActualRows);
            AssertUtil.assertList(traverseActualRows, dataList);
        }
        // traverse(String tname, String idxName)
        {
            KvIterator traverseIterator = masterTableSyncClient.traverse(tableName,indexName);
            List<Object[]> traverseActualRows = KvIteratorUtil.kvIteratorToListForSchema(traverseIterator);
            DataUtil.convertData(traverseActualRows);
            AssertUtil.assertList(traverseActualRows, dataList);
        }
        // traverse(String tname, String idxName, String tsName)
        {
            KvIterator traverseIterator = masterTableSyncClient.traverse(tableName,indexName,"c7");
            List<Object[]> traverseActualRows = KvIteratorUtil.kvIteratorToListForSchema(traverseIterator);
            DataUtil.convertData(traverseActualRows);
            AssertUtil.assertList(traverseActualRows, dataList);
        }
        // put(String tname, long time, Object[] row)
        // getRow(String tname, String key, String idxName, long time)
        // scan(String tname, String key, String idxName, long st, long et)
        {
            short s = 1;
            Date date = Date.valueOf("2020-05-01");
            Timestamp timestamp = new Timestamp(1590738989000L);
            Object[] data = new Object[]{"bb", s, 2, 3L, 1.1F, 2.1, timestamp, date, true};
            masterTableSyncClient.put(tableName, System.currentTimeMillis(), data);
            bbList.add(data);
            Object[] actualGetRow = masterTableSyncClient.getRow(tableName, "bb", indexName, timestamp.getTime());
            DataUtil.convertData(actualGetRow);
            Assert.assertEquals(actualGetRow, data);

            List<Object[]> scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tableName, "bb", indexName, timestamp.getTime(), timestamp.getTime()-1));
            Assert.assertEquals(scanList.size(),1);
            Object[] actualScanRow = scanList.get(0);
            DataUtil.convertData(actualScanRow);
            Assert.assertEquals(actualScanRow, data);
        }
        // put(String tname, long time, Map<String, Object> row)
        // getRow(String tname, String key, String idxName, long time, Tablet.GetType type)
        // scan(String tname, String key, String idxName, long st, long et, int limit)
        {
            short s = 1;
            Date date = Date.valueOf("2020-05-01");
            Timestamp timestamp = new Timestamp(1590738989001L);
            Map<String,Object> mapData = new HashMap<>();
            mapData.put("c1","bb");
            mapData.put("c2",s);
            mapData.put("c3",2);
            mapData.put("c4",3L);
            mapData.put("c5",1.1F);
            mapData.put("c6",2.1);
            mapData.put("c7",timestamp);
            mapData.put("c8",date);
            mapData.put("c9",true);
            masterTableSyncClient.put(tableName, System.currentTimeMillis(), mapData);
            Object[] actualGetRow = masterTableSyncClient.getRow(tableName, "bb", indexName, timestamp.getTime(), Tablet.GetType.kSubKeyEq);
            DataUtil.convertData(actualGetRow);
            Object[] data = new Object[]{"bb", s, 2, 3L, 1.1F, 2.1, timestamp, date, true};
            bbList.add(data);
            Assert.assertEquals(actualGetRow, data);

            List<Object[]> scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tableName, "bb", indexName, timestamp.getTime(), timestamp.getTime()-1, 2));
            Assert.assertEquals(scanList.size(),1);
            Object[] actualScanRow = scanList.get(0);
            DataUtil.convertData(actualScanRow);
            Assert.assertEquals(actualScanRow, data);
        }
        // put(String tname, Map<String, Object> row)
        // getRow(String tname, String key, String idxName, long time, String tsName, Tablet.GetType type)
        // scan(String tname, String key, String idxName, long st, long et, String tsName, int limit)
        {
            short s = 1;
            Date date = Date.valueOf("2020-05-01");
            Timestamp timestamp = new Timestamp(1590738989002L);
            Map<String,Object> mapData = new HashMap<>();
            mapData.put("c1","bb");
            mapData.put("c2",s);
            mapData.put("c3",2);
            mapData.put("c4",3L);
            mapData.put("c5",1.1F);
            mapData.put("c6",2.1);
            mapData.put("c7",timestamp);
            mapData.put("c8",date);
            mapData.put("c9",true);
            masterTableSyncClient.put(tableName, mapData);
            Object[] actualGetRow = masterTableSyncClient.getRow(tableName, "bb", indexName, timestamp.getTime(), "c7", Tablet.GetType.kSubKeyEq);
            DataUtil.convertData(actualGetRow);
            Object[] data = new Object[]{"bb", s, 2, 3L, 1.1F, 2.1, timestamp, date, true};
            bbList.add(data);
            Assert.assertEquals(actualGetRow, data);

            List<Object[]> scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tableName, "bb", indexName, timestamp.getTime(), timestamp.getTime()-1,"c7", 2));
            Assert.assertEquals(scanList.size(),1);
            Object[] actualScanRow = scanList.get(0);
            DataUtil.convertData(actualScanRow);
            Assert.assertEquals(actualScanRow, data);
        }
        // put(int tid, int pid, long time, Map<String, Object> row)
        // getRow(int tid, int pid, String key, String idxName, long time)
        // scan(int tid, int pid, String key, String idxName, long st, long et)
        {
            short s = 1;
            Date date = Date.valueOf("2020-05-01");
            Timestamp timestamp = new Timestamp(1590738989003L);
            Map<String,Object> mapData = new HashMap<>();
            mapData.put("c1","bb");
            mapData.put("c2",s);
            mapData.put("c3",2);
            mapData.put("c4",3L);
            mapData.put("c5",1.1F);
            mapData.put("c6",2.1);
            mapData.put("c7",timestamp);
            mapData.put("c8",date);
            mapData.put("c9",true);
            masterTableSyncClient.put(tid,0, System.currentTimeMillis(), mapData);
            Object[] actualGetRow = masterTableSyncClient.getRow(tid,0, "bb", indexName, timestamp.getTime());
            DataUtil.convertData(actualGetRow);
            Object[] data = new Object[]{"bb", s, 2, 3L, 1.1F, 2.1, timestamp, date, true};
            bbList.add(data);
            Assert.assertEquals(actualGetRow, data);

            List<Object[]> scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tid,0, "bb", indexName, timestamp.getTime(), timestamp.getTime()-1));
            Assert.assertEquals(scanList.size(),1);
            Object[] actualScanRow = scanList.get(0);
            DataUtil.convertData(actualScanRow);
            Assert.assertEquals(actualScanRow, data);

        }
        // put(int tid, int pid, long time, Object[] row)
        // getRow(int tid, int pid, String key, String idxName)
        // scan(int tid, int pid, String key, String idxName, long st, long et, int limit)
        {
            short s = 1;
            Date date = Date.valueOf("2020-05-01");
            Timestamp timestamp = new Timestamp(1590738989004L);
            Object[] data = new Object[]{"bb", s, 2, 3L, 1.1F, 2.1, timestamp, date, true};
            masterTableSyncClient.put(tid, 0,System.currentTimeMillis(), data);
            bbList.add(data);
            Object[] actualGetRow = masterTableSyncClient.getRow(tid,0,"bb", indexName);
            DataUtil.convertData(actualGetRow);
            Assert.assertEquals(actualGetRow, data);

            List<Object[]> scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tid,0, "bb", indexName, timestamp.getTime(), timestamp.getTime()-1,2));
            Assert.assertEquals(scanList.size(),1);
            Object[] actualScanRow = scanList.get(0);
            DataUtil.convertData(actualScanRow);
            Assert.assertEquals(actualScanRow, data);
        }
        Collections.reverse(bbList);
        // getRow(int tid, int pid, String key, long time)
        // scan(int tid, int pid, String key, long st, long et)
        // scan(int tid, int pid, String key, int limit)
        // scan(int tid, int pid, String key, long st, long et, int limit)
        // scan(int tid, int pid, String key, String idxName, int limit)
        {
            Object[] actualGetRow = masterTableSyncClient.getRow(tid,0,"bb",1590738989004L);
            DataUtil.convertData(actualGetRow);
            Assert.assertEquals(actualGetRow, bbList.get(0));

            List<Object[]> scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tid,0, "bb", 1590738989004L, 1590738989004L-1));
            Assert.assertEquals(scanList.size(),1);
            Object[] actualScanRow = scanList.get(0);
            DataUtil.convertData(actualScanRow);
            Assert.assertEquals(actualScanRow, bbList.get(0));

            scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tid,0, "bb", 1));
            Assert.assertEquals(scanList.size(),1);
            actualScanRow = scanList.get(0);
            DataUtil.convertData(actualScanRow);
            Assert.assertEquals(actualScanRow, bbList.get(0));

            scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tid,0, "bb", 1590738989004L, 1590738989004L-1,2));
            Assert.assertEquals(scanList.size(),1);
            actualScanRow = scanList.get(0);
            DataUtil.convertData(actualScanRow);
            Assert.assertEquals(actualScanRow, bbList.get(0));

            scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tid,0, "bb", indexName,1));
            Assert.assertEquals(scanList.size(),1);
            actualScanRow = scanList.get(0);
            DataUtil.convertData(actualScanRow);
            Assert.assertEquals(actualScanRow, bbList.get(0));
        }
        // getRow(String tname, String key, long time)
        // getRow(String tname, String key, long time, Tablet.GetType type)
        // getRow(String tname, String key, long time, GetOption getOption)
        // getRow(String tname, String key, long time, Object type)
        // getRow(String tname, String key, String idxName, long time, String tsName, Tablet.GetType type, long et, Tablet.GetType etType)
        {
            Object[] actualGetRow = masterTableSyncClient.getRow(tableName,"aa",1590738989009L);
            DataUtil.convertData(actualGetRow);
            Assert.assertEquals(actualGetRow, dataList.get(0));

            actualGetRow = masterTableSyncClient.getRow(tableName,"aa",1590738989009L, Tablet.GetType.kSubKeyEq);
            DataUtil.convertData(actualGetRow);
            Assert.assertEquals(actualGetRow, dataList.get(0));

            GetOption getOption = new GetOption();
            getOption.setStType(Tablet.GetType.kSubKeyEq);
            getOption.setTsName("c7");
            getOption.setEt(1590738989009L);
            getOption.setEtType(Tablet.GetType.kSubKeyEq);
            getOption.setIdxName(indexName);
            getOption.setProjection(Lists.newArrayList("c1","c7"));
            actualGetRow = masterTableSyncClient.getRow(tableName,"aa",1590738989009L, getOption);
            DataUtil.convertData(actualGetRow);
            Assert.assertEquals(actualGetRow, new Object[]{"aa",new Timestamp(1590738989009L)});

            Object type = Tablet.GetType.kSubKeyEq;
            actualGetRow = masterTableSyncClient.getRow(tableName,"aa",1590738989009L,type );
            DataUtil.convertData(actualGetRow);
            Assert.assertEquals(actualGetRow, dataList.get(0));

            actualGetRow = masterTableSyncClient.getRow(tableName,"aa", indexName, 1590738989009L, "c7", Tablet.GetType.kSubKeyEq, 1590738989009L, Tablet.GetType.kSubKeyEq);
            DataUtil.convertData(actualGetRow);
            Assert.assertEquals(actualGetRow, dataList.get(0));
        }
        // scan(String tname, String key, long st, long et)
        // scan(String tname, String key, int limit)
        // scan(String tname, String key, long st, long et, int limit)
        // scan(String tname, String key, long st, long et, ScanOption option)
        {
            List<Object[]> scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tableName, "aa", 1590738989009L, 1590738989009L-1));
            Assert.assertEquals(scanList.size(),1);
            Object[] actualScanRow = scanList.get(0);
            DataUtil.convertData(actualScanRow);
            Assert.assertEquals(actualScanRow, dataList.get(0));

            scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tableName, "aa", 1));
            Assert.assertEquals(scanList.size(),1);
            actualScanRow = scanList.get(0);
            DataUtil.convertData(actualScanRow);
            Assert.assertEquals(actualScanRow, dataList.get(0));

            scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tableName, "aa", 1590738989009L, 1590738989009L-1,2));
            Assert.assertEquals(scanList.size(),1);
            actualScanRow = scanList.get(0);
            DataUtil.convertData(actualScanRow);
            Assert.assertEquals(actualScanRow, dataList.get(0));

            ScanOption scanOption = new ScanOption();
            scanOption.setTsName("c7");
            scanOption.setLimit(2);
            scanOption.setRemoveDuplicateRecordByTime(true);
            scanOption.setAtLeast(1);
            scanOption.setIdxName(indexName);
            scanOption.setProjection(Lists.newArrayList("c1","c7"));
            scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tableName, "aa",1590738989009L, 1590738989009L-1,scanOption));
            Assert.assertEquals(scanList.size(),1);
            actualScanRow = scanList.get(0);
            DataUtil.convertData(actualScanRow);
            Assert.assertEquals(actualScanRow, new Object[]{"aa",new Timestamp(1590738989009L)});
        }
    }

    @Description("组合索引表-get-scan")
    @Test(dataProvider = "storageModeData")
    public void testUnionIndexTable(String storageMode) throws Exception {
        String tableName = tableNamePrefix + RandomStringUtils.randomAlphabetic(8);
        String tableDDL = "create table %s(c1 string,c2 smallint,c3 int,c4 bigint,c5 float,c6 double,c7 timestamp,c8 date,c9 bool," +
                "index(key=(c1,c3),ts=c7))options(partitionnum=2,replicanum=3,storage_mode='%s');";
        tableDDL = String.format(tableDDL, tableName, storageMode);
        sdkClient.execute(tableDDL);
        tableNameList.add(tableName);
        List<Object[]> dataList = new ArrayList<>();
        // put(String tname, Object[] row)
        for (int i = 0; i < 10; i++) {
            short s = 1;
            Date date = Date.valueOf("2020-05-01");
            Timestamp timestamp = new Timestamp(1590738989000L + i);
            Object[] data = new Object[]{"aa", s, 2, 3L, 1.1F, 2.1, timestamp, date, true};
            masterTableSyncClient.put(tableName, data);
            dataList.add(data);
        }
        Collections.reverse(dataList);
        NS.TableInfo tableInfo = masterNsc.showTable(tableName).get(0);
        String indexName = tableInfo.getColumnKey(0).getIndexName();
        int tid = tableInfo.getTid();
        log.info("indexName:" + indexName);
        log.info("tid:" + tid);

        short s = 1;
        Date date = Date.valueOf("2020-05-01");
        Timestamp timestamp = new Timestamp(1590738989000L);
        Object[] data = new Object[]{"bb", s, 2, 3L, 1.1F, 2.1, timestamp, date, true};
        masterTableSyncClient.put(tid, 0,System.currentTimeMillis(), data);

        // getRow(String tname, Object[] keyArr, String idxName, long time, String tsName, Tablet.GetType type, long et, Tablet.GetType etType)
        // getRow(String tname, Map<String, Object> keyMap, String idxName, long time, String tsName, Tablet.GetType type, long et, Tablet.GetType etType)
        // getRow(String tname, Object[] keyArr, String idxName, long time, String tsName, Tablet.GetType type)
        // getRow(String tname, Map<String, Object> keyMap, String idxName, long time, String tsName, Tablet.GetType type)
        // getRow(String tname, Object[] keyArr, long time, GetOption option)
        // getRow(String tname, Map<String, Object> keyMap,long time, GetOption option)
        {
            Object[] keys = new Object[]{"aa",2};
            Object[] actualGetRow = masterTableSyncClient.getRow(tableName,keys,indexName,1590738989009L,"c7", Tablet.GetType.kSubKeyEq,1590738989009L,Tablet.GetType.kSubKeyEq);
            DataUtil.convertData(actualGetRow);
            Assert.assertEquals(actualGetRow, dataList.get(0));

            Map<String, Object> keyMap = new HashMap<>();
            keyMap.put("c1","aa");
            keyMap.put("c3",2);
            actualGetRow = masterTableSyncClient.getRow(tableName,keyMap,indexName,1590738989009L,"c7", Tablet.GetType.kSubKeyEq,1590738989009L,Tablet.GetType.kSubKeyEq);
            DataUtil.convertData(actualGetRow);
            Assert.assertEquals(actualGetRow, dataList.get(0));

            actualGetRow = masterTableSyncClient.getRow(tableName,keys,indexName,1590738989009L,"c7", Tablet.GetType.kSubKeyEq);
            DataUtil.convertData(actualGetRow);
            Assert.assertEquals(actualGetRow, dataList.get(0));

            actualGetRow = masterTableSyncClient.getRow(tableName,keyMap,indexName,1590738989009L,"c7", Tablet.GetType.kSubKeyEq);
            DataUtil.convertData(actualGetRow);
            Assert.assertEquals(actualGetRow, dataList.get(0));

            GetOption getOption = new GetOption();
            getOption.setStType(Tablet.GetType.kSubKeyEq);
            getOption.setTsName("c7");
            getOption.setEt(1590738989009L);
            getOption.setEtType(Tablet.GetType.kSubKeyEq);
            getOption.setIdxName(indexName);
            getOption.setProjection(Lists.newArrayList("c1","c2","c3","c4","c5","c6","c7","c8","c9"));

            actualGetRow = masterTableSyncClient.getRow(tableName,keys,1590738989009L,getOption);
            DataUtil.convertData(actualGetRow);
            Assert.assertEquals(actualGetRow, dataList.get(0));

            actualGetRow = masterTableSyncClient.getRow(tableName,keyMap,1590738989009L,getOption);
            DataUtil.convertData(actualGetRow);
            Assert.assertEquals(actualGetRow, dataList.get(0));
        }
        // scan(int tid, int pid, Object[] row, String idxName, long st, long et, String tsName)
        // scan(int tid, int pid, Map<String, Object> keyMap, String idxName, long st, long et, String tsName, int limit)
        // scan(int tid, int pid, Map<String, Object> keyMap, String idxName, long st, long et, String tsName, int limit, int atLeast)
        {
            Object[] keys = new Object[]{"bb",2};
            List<Object[]> scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tid,0,keys,indexName, 1590738989000L, 1590738989000L-1,"c7"));
            Assert.assertEquals(scanList.size(),1);
            Object[] actualScanRow = scanList.get(0);
            DataUtil.convertData(actualScanRow);
            Assert.assertEquals(actualScanRow, data);

            Map<String, Object> keyMap = new HashMap<>();
            keyMap.put("c1","bb");
            keyMap.put("c3",2);
            scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tid,0,keyMap,indexName, 1590738989000L, 1590738989000L-1,"c7", 2));
            Assert.assertEquals(scanList.size(),1);
            actualScanRow = scanList.get(0);
            DataUtil.convertData(actualScanRow);
            Assert.assertEquals(actualScanRow, data);

            scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tid,0,keyMap,indexName, 1590738989000L, 1590738989000L-1,"c7", 2,1));
            Assert.assertEquals(scanList.size(),1);
            actualScanRow = scanList.get(0);
            DataUtil.convertData(actualScanRow);
            Assert.assertEquals(actualScanRow, data);

        }
        // scan(String tname, Object[] keyArr, String idxName, long st, long et, String tsName, int limit)
        // scan(String tname, Map<String, Object> keyMap, String idxName, long st, long et, String tsName, int limit)
        // scan(String tname, Map<String, Object> keyMap,long st, long et, ScanOption option)
        // scan(String tname, Object[] keyArr, long st, long et, ScanOption option)
        {
            Object[] keys = new Object[]{"aa",2};
            List<Object[]> scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tableName,keys,indexName, 1590738989009L, 1590738989009L-1,"c7",2));
            Assert.assertEquals(scanList.size(),1);
            Object[] actualScanRow = scanList.get(0);
            DataUtil.convertData(actualScanRow);
            Assert.assertEquals(actualScanRow, dataList.get(0));

            Map<String, Object> keyMap = new HashMap<>();
            keyMap.put("c1","aa");
            keyMap.put("c3",2);
            scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tableName,keyMap,indexName, 1590738989009L, 1590738989009L-1,"c7", 2));
            Assert.assertEquals(scanList.size(),1);
            actualScanRow = scanList.get(0);
            DataUtil.convertData(actualScanRow);
            Assert.assertEquals(actualScanRow, dataList.get(0));

            ScanOption scanOption = new ScanOption();
            scanOption.setTsName("c7");
            scanOption.setLimit(2);
            scanOption.setRemoveDuplicateRecordByTime(true);
            scanOption.setAtLeast(1);
            scanOption.setIdxName(indexName);
            scanOption.setProjection(Lists.newArrayList("c1","c7"));
            scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tableName,keyMap, 1590738989009L, 1590738989009L-1,scanOption));
            Assert.assertEquals(scanList.size(),1);
            actualScanRow = scanList.get(0);
            DataUtil.convertData(actualScanRow);
            Assert.assertEquals(actualScanRow, new Object[]{"aa",new Timestamp(1590738989009L)});

            scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tableName,keys, 1590738989009L, 1590738989009L-1,scanOption));
            Assert.assertEquals(scanList.size(),1);
            actualScanRow = scanList.get(0);
            DataUtil.convertData(actualScanRow);
            Assert.assertEquals(actualScanRow, new Object[]{"aa",new Timestamp(1590738989009L)});
        }
    }
    @Description("不指定索引-put-get-scan")
    @Test(dataProvider = "storageModeData")
    public void testNotIndexTable(String storageMode) throws Exception{
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        String tableDDL = "create table %s(c1 string,c2 smallint,c3 int,c4 bigint,c5 float,c6 double,c7 timestamp,c8 date,c9 bool)options(storage_mode='%s');";
        tableDDL = String.format(tableDDL,tableName,storageMode);
        sdkClient.execute(tableDDL);
        tableNameList.add(tableName);
        List<Object[]> dataList = new ArrayList<>();
        List<Object[]> bbList = new ArrayList<>();
        // put(String tname, Object[] row)
        // getRow(String tname, String key, String idxName)
        // scan(String tname, String key, String idxName, int limit)
        for(int i=0;i<10;i++) {
            short s = 1;
            Date date = Date.valueOf("2020-05-01");
            Timestamp timestamp = new Timestamp(1590738989000L+i);
            Object[] data = new Object[]{"aa",s, 2, 3L, 1.1F, 2.1,timestamp ,date,true};
            masterTableSyncClient.put(tableName,data);
            dataList.add(data);
        }
        Collections.reverse(dataList);
        NS.TableInfo tableInfo = masterNsc.showTable(tableName).get(0);
        String indexName = tableInfo.getColumnKey(0).getIndexName();
        int tid = tableInfo.getTid();
        log.info("indexName:"+indexName);
        log.info("tid:" + tid);
        Object[] actualRow = masterTableSyncClient.getRow(tableName, "aa", indexName);
        Object[] expectRow = dataList.get(0);
        // DateTime -> Timestamp
        DataUtil.convertData(actualRow);
        Assert.assertEquals(actualRow,expectRow);

        KvIterator scanIterator = masterTableSyncClient.scan(tableName, "aa", indexName, 10);
        List<Object[]> scanActualRows = KvIteratorUtil.kvIteratorToListForSchema(scanIterator);
        DataUtil.convertData(scanActualRows);
        AssertUtil.assertList(scanActualRows,dataList);
        // traverse(String tname)
        {
            KvIterator traverseIterator = masterTableSyncClient.traverse(tableName);
            List<Object[]> traverseActualRows = KvIteratorUtil.kvIteratorToListForSchema(traverseIterator);
            DataUtil.convertData(traverseActualRows);
            AssertUtil.assertList(traverseActualRows, dataList);
        }
        // traverse(String tname, String idxName)
        {
            KvIterator traverseIterator = masterTableSyncClient.traverse(tableName,indexName);
            List<Object[]> traverseActualRows = KvIteratorUtil.kvIteratorToListForSchema(traverseIterator);
            DataUtil.convertData(traverseActualRows);
            AssertUtil.assertList(traverseActualRows, dataList);
        }
        // traverse(String tname, String idxName, String tsName)
        {
            KvIterator traverseIterator = masterTableSyncClient.traverse(tableName,indexName,"c7");
            List<Object[]> traverseActualRows = KvIteratorUtil.kvIteratorToListForSchema(traverseIterator);
            DataUtil.convertData(traverseActualRows);
            AssertUtil.assertList(traverseActualRows, dataList);
        }
        // put(String tname, long time, Object[] row)
        // getRow(String tname, String key, String idxName, long time)
        // scan(String tname, String key, String idxName, long st, long et)
        {
            short s = 1;
            Date date = Date.valueOf("2020-05-01");
            Timestamp timestamp = new Timestamp(1590738989000L);
            Object[] data = new Object[]{"bb", s, 2, 3L, 1.1F, 2.1, timestamp, date, true};
            masterTableSyncClient.put(tableName, timestamp.getTime(), data);
            bbList.add(data);
            Object[] actualGetRow = masterTableSyncClient.getRow(tableName, "bb", indexName, timestamp.getTime());
            DataUtil.convertData(actualGetRow);
            Assert.assertEquals(actualGetRow, data);

            List<Object[]> scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tableName, "bb", indexName, timestamp.getTime(), timestamp.getTime()-1));
            Assert.assertEquals(scanList.size(),1);
            Object[] actualScanRow = scanList.get(0);
            DataUtil.convertData(actualScanRow);
            Assert.assertEquals(actualScanRow, data);
        }
        // put(String tname, long time, Map<String, Object> row)
        // getRow(String tname, String key, String idxName, long time, Tablet.GetType type)
        // scan(String tname, String key, String idxName, long st, long et, int limit)
        {
            short s = 1;
            Date date = Date.valueOf("2020-05-01");
            Timestamp timestamp = new Timestamp(1590738989001L);
            Map<String,Object> mapData = new HashMap<>();
            mapData.put("c1","bb");
            mapData.put("c2",s);
            mapData.put("c3",2);
            mapData.put("c4",3L);
            mapData.put("c5",1.1F);
            mapData.put("c6",2.1);
            mapData.put("c7",timestamp);
            mapData.put("c8",date);
            mapData.put("c9",true);
            masterTableSyncClient.put(tableName, timestamp.getTime(), mapData);
            Object[] actualGetRow = masterTableSyncClient.getRow(tableName, "bb", indexName, timestamp.getTime(), Tablet.GetType.kSubKeyEq);
            DataUtil.convertData(actualGetRow);
            Object[] data = new Object[]{"bb", s, 2, 3L, 1.1F, 2.1, timestamp, date, true};
            bbList.add(data);
            Assert.assertEquals(actualGetRow, data);

            List<Object[]> scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tableName, "bb", indexName, timestamp.getTime(), timestamp.getTime()-1, 2));
            Assert.assertEquals(scanList.size(),1);
            Object[] actualScanRow = scanList.get(0);
            DataUtil.convertData(actualScanRow);
            Assert.assertEquals(actualScanRow, data);
        }
        // put(String tname, Map<String, Object> row)
        // getRow(String tname, String key, String idxName, long time, String tsName, Tablet.GetType type)
        // scan(String tname, String key, String idxName, long st, long et, String tsName, int limit)
        {
            short s = 1;
            Date date = Date.valueOf("2020-05-01");
            Timestamp timestamp = new Timestamp(1590738989002L);
            Map<String,Object> mapData = new HashMap<>();
            mapData.put("c1","bb");
            mapData.put("c2",s);
            mapData.put("c3",2);
            mapData.put("c4",3L);
            mapData.put("c5",1.1F);
            mapData.put("c6",2.1);
            mapData.put("c7",timestamp);
            mapData.put("c8",date);
            mapData.put("c9",true);
            masterTableSyncClient.put(tableName, timestamp.getTime(), mapData);
            Object[] actualGetRow = masterTableSyncClient.getRow(tableName, "bb", indexName, timestamp.getTime(), "c7", Tablet.GetType.kSubKeyEq);
            DataUtil.convertData(actualGetRow);
            Object[] data = new Object[]{"bb", s, 2, 3L, 1.1F, 2.1, timestamp, date, true};
            bbList.add(data);
            Assert.assertEquals(actualGetRow, data);

            List<Object[]> scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tableName, "bb", indexName, timestamp.getTime(), timestamp.getTime()-1,"c7", 2));
            Assert.assertEquals(scanList.size(),1);
            Object[] actualScanRow = scanList.get(0);
            DataUtil.convertData(actualScanRow);
            Assert.assertEquals(actualScanRow, data);
        }
        // put(int tid, int pid, long time, Map<String, Object> row)
        // getRow(int tid, int pid, String key, String idxName, long time)
        // scan(int tid, int pid, String key, String idxName, long st, long et)
        {
            short s = 1;
            Date date = Date.valueOf("2020-05-01");
            Timestamp timestamp = new Timestamp(1590738989003L);
            Map<String,Object> mapData = new HashMap<>();
            mapData.put("c1","bb");
            mapData.put("c2",s);
            mapData.put("c3",2);
            mapData.put("c4",3L);
            mapData.put("c5",1.1F);
            mapData.put("c6",2.1);
            mapData.put("c7",timestamp);
            mapData.put("c8",date);
            mapData.put("c9",true);
            masterTableSyncClient.put(tid,0, timestamp.getTime(), mapData);
            Object[] actualGetRow = masterTableSyncClient.getRow(tid,0, "bb", indexName, timestamp.getTime());
            DataUtil.convertData(actualGetRow);
            Object[] data = new Object[]{"bb", s, 2, 3L, 1.1F, 2.1, timestamp, date, true};
            bbList.add(data);
            Assert.assertEquals(actualGetRow, data);

            List<Object[]> scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tid,0, "bb", indexName, timestamp.getTime(), timestamp.getTime()-1));
            Assert.assertEquals(scanList.size(),1);
            Object[] actualScanRow = scanList.get(0);
            DataUtil.convertData(actualScanRow);
            Assert.assertEquals(actualScanRow, data);

        }
        // put(int tid, int pid, long time, Object[] row)
        // getRow(int tid, int pid, String key, String idxName)
        // scan(int tid, int pid, String key, String idxName, long st, long et, int limit)
        {
            short s = 1;
            Date date = Date.valueOf("2020-05-01");
            Timestamp timestamp = new Timestamp(1590738989004L);
            Object[] data = new Object[]{"bb", s, 2, 3L, 1.1F, 2.1, timestamp, date, true};
            masterTableSyncClient.put(tid, 0,timestamp.getTime(), data);
            bbList.add(data);
            Object[] actualGetRow = masterTableSyncClient.getRow(tid,0,"bb", indexName);
            DataUtil.convertData(actualGetRow);
            Assert.assertEquals(actualGetRow, data);

            List<Object[]> scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tid,0, "bb", indexName, timestamp.getTime(), timestamp.getTime()-1,2));
            Assert.assertEquals(scanList.size(),1);
            Object[] actualScanRow = scanList.get(0);
            DataUtil.convertData(actualScanRow);
            Assert.assertEquals(actualScanRow, data);
        }
        Collections.reverse(bbList);
        // getRow(int tid, int pid, String key, long time)
        // scan(int tid, int pid, String key, long st, long et)
        // scan(int tid, int pid, String key, int limit)
        // scan(int tid, int pid, String key, long st, long et, int limit)
        // scan(int tid, int pid, String key, String idxName, int limit)
        {
            Object[] actualGetRow = masterTableSyncClient.getRow(tid,0,"bb",1590738989004L);
            DataUtil.convertData(actualGetRow);
            Assert.assertEquals(actualGetRow, bbList.get(0));

            List<Object[]> scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tid,0, "bb", 1590738989004L, 1590738989004L-1));
            Assert.assertEquals(scanList.size(),1);
            Object[] actualScanRow = scanList.get(0);
            DataUtil.convertData(actualScanRow);
            Assert.assertEquals(actualScanRow, bbList.get(0));

            scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tid,0, "bb", 1));
            Assert.assertEquals(scanList.size(),1);
            actualScanRow = scanList.get(0);
            DataUtil.convertData(actualScanRow);
            Assert.assertEquals(actualScanRow, bbList.get(0));

            scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tid,0, "bb", 1590738989004L, 1590738989004L-1,2));
            Assert.assertEquals(scanList.size(),1);
            actualScanRow = scanList.get(0);
            DataUtil.convertData(actualScanRow);
            Assert.assertEquals(actualScanRow, bbList.get(0));

            scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tid,0, "bb", indexName,1));
            Assert.assertEquals(scanList.size(),1);
            actualScanRow = scanList.get(0);
            DataUtil.convertData(actualScanRow);
            Assert.assertEquals(actualScanRow, bbList.get(0));
        }
        // getRow(String tname, String key, long time)
        // getRow(String tname, String key, long time, Tablet.GetType type)
        // getRow(String tname, String key, long time, GetOption getOption)
        // getRow(String tname, String key, long time, Object type)
        // getRow(String tname, String key, String idxName, long time, String tsName, Tablet.GetType type, long et, Tablet.GetType etType)
        {
            Object[] actualGetRow = masterTableSyncClient.getRow(tableName,"aa",0);
            DataUtil.convertData(actualGetRow);
            Assert.assertEquals(actualGetRow, dataList.get(0));

            actualGetRow = masterTableSyncClient.getRow(tableName,"aa",0, Tablet.GetType.kSubKeyEq);
            DataUtil.convertData(actualGetRow);
            Assert.assertEquals(actualGetRow, dataList.get(0));

            GetOption getOption = new GetOption();
            getOption.setProjection(Lists.newArrayList("c1","c7"));
            actualGetRow = masterTableSyncClient.getRow(tableName,"aa",0, getOption);
            DataUtil.convertData(actualGetRow);
            Assert.assertEquals(actualGetRow, new Object[]{"aa",new Timestamp(1590738989009L)});

            Object type = Tablet.GetType.kSubKeyEq;
            actualGetRow = masterTableSyncClient.getRow(tableName,"aa",0,type );
            DataUtil.convertData(actualGetRow);
            Assert.assertEquals(actualGetRow, dataList.get(0));

            actualGetRow = masterTableSyncClient.getRow(tableName,"aa", indexName, System.currentTimeMillis(), "c7", Tablet.GetType.kSubKeyLe, 1590738989009L, Tablet.GetType.kSubKeyGe);
            DataUtil.convertData(actualGetRow);
            Assert.assertEquals(actualGetRow, dataList.get(0));
        }
        // scan(String tname, String key, long st, long et)
        // scan(String tname, String key, int limit)
        // scan(String tname, String key, long st, long et, int limit)
        // scan(String tname, String key, long st, long et, ScanOption option)
        {
            List<Object[]> scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tableName, "aa", 0, 0));
            Assert.assertEquals(scanList.size(),10);
            DataUtil.convertData(scanList);
            AssertUtil.assertList(scanList, dataList);

            scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tableName, "aa", 1));
            Assert.assertEquals(scanList.size(),1);
            Object[] actualScanRow = scanList.get(0);
            DataUtil.convertData(actualScanRow);
            Assert.assertEquals(actualScanRow, dataList.get(0));

            scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tableName, "aa", 0, 0,1));
            Assert.assertEquals(scanList.size(),1);
            actualScanRow = scanList.get(0);
            DataUtil.convertData(actualScanRow);
            Assert.assertEquals(actualScanRow, dataList.get(0));

            ScanOption scanOption = new ScanOption();
            scanOption.setTsName("c7");
            scanOption.setLimit(1);
            scanOption.setRemoveDuplicateRecordByTime(true);
            scanOption.setAtLeast(1);
            scanOption.setIdxName(indexName);
            scanOption.setProjection(Lists.newArrayList("c1","c7"));
            scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tableName, "aa",0, 0,scanOption));
            Assert.assertEquals(scanList.size(),1);
            actualScanRow = scanList.get(0);
            DataUtil.convertData(actualScanRow);
            Assert.assertEquals(actualScanRow, new Object[]{"aa",new Timestamp(1590738989009L)});
        }
    }

    @Description("重复数据-内存表-put-get-scan")
    @Test()
    public void testRepeatData() throws Exception {
        String storageMode= "memory";
        String tableName = tableNamePrefix + RandomStringUtils.randomAlphabetic(8);
        String tableDDL = "create table %s(c1 string,c2 smallint,c3 int,c4 bigint,c5 float,c6 double,c7 timestamp,c8 date,c9 bool," +
                "index(key=(c1),ts=c7))options(partitionnum=2,replicanum=3,storage_mode='%s');";
        tableDDL = String.format(tableDDL, tableName, storageMode);
        sdkClient.execute(tableDDL);
        tableNameList.add(tableName);
        List<Object[]> dataList = new ArrayList<>();
        // put(String tname, Object[] row)
        // getRow(String tname, String key, String idxName)
        // scan(String tname, String key, String idxName, int limit)
        for (int i = 0; i < 10; i++) {
            short s = 1;
            Date date = Date.valueOf("2020-05-01");
            Timestamp timestamp = new Timestamp(1590738989000L);
            Object[] data = new Object[]{"aa", s, 2, 3L, 1.1F, 2.1, timestamp, date, true};
            masterTableSyncClient.put(tableName, data);
            dataList.add(data);
        }
        Collections.reverse(dataList);
        NS.TableInfo tableInfo = masterNsc.showTable(tableName).get(0);
        String indexName = tableInfo.getColumnKey(0).getIndexName();
        int tid = tableInfo.getTid();
        log.info("indexName:" + indexName);
        log.info("tid:" + tid);
        Object[] actualRow = masterTableSyncClient.getRow(tableName, "aa", indexName);
        Object[] expectRow = dataList.get(0);
        // DateTime -> Timestamp
        DataUtil.convertData(actualRow);
        Assert.assertEquals(actualRow, expectRow);

        KvIterator scanIterator = masterTableSyncClient.scan(tableName, "aa", indexName, 10);
        List<Object[]> scanActualRows = KvIteratorUtil.kvIteratorToListForSchema(scanIterator);
        DataUtil.convertData(scanActualRows);
        AssertUtil.assertList(scanActualRows, dataList);
        // traverse(String tname)
        {
            KvIterator traverseIterator = masterTableSyncClient.traverse(tableName);
            List<Object[]> traverseActualRows = KvIteratorUtil.kvIteratorToListForSchema(traverseIterator);
            DataUtil.convertData(traverseActualRows);
            AssertUtil.assertList(traverseActualRows, dataList);
        }
    }

    @Description("重复数据-磁盘表-put-get-scan")
    @Test(dataProvider = "ssdAndHdd")
    public void testRepeatDataByDiskTable(String storageMode) throws Exception {
        String tableName = tableNamePrefix + RandomStringUtils.randomAlphabetic(8);
        String tableDDL = "create table %s(c1 string,c2 smallint,c3 int,c4 bigint,c5 float,c6 double,c7 timestamp,c8 date,c9 bool," +
                "index(key=(c1),ts=c7))options(partitionnum=2,replicanum=3,storage_mode='%s');";
        tableDDL = String.format(tableDDL, tableName, storageMode);
        sdkClient.execute(tableDDL);
        tableNameList.add(tableName);
        List<Object[]> dataList = new ArrayList<>();
        // put(String tname, Object[] row)
        // getRow(String tname, String key, String idxName)
        // scan(String tname, String key, String idxName, int limit)
        for (int i = 0; i < 10; i++) {
            short s = 1;
            Date date = Date.valueOf("2020-05-01");
            Timestamp timestamp = new Timestamp(1590738989000L);
            Object[] data = new Object[]{"aa", s, 2, 3L, 1.1F, 2.1, timestamp, date, true};
            masterTableSyncClient.put(tableName, data);
            dataList.add(data);
        }
        Collections.reverse(dataList);
        NS.TableInfo tableInfo = masterNsc.showTable(tableName).get(0);
        String indexName = tableInfo.getColumnKey(0).getIndexName();
        int tid = tableInfo.getTid();
        log.info("indexName:" + indexName);
        log.info("tid:" + tid);
        Object[] actualRow = masterTableSyncClient.getRow(tableName, "aa", indexName);
        Object[] expectRow = dataList.get(0);
        // DateTime -> Timestamp
        DataUtil.convertData(actualRow);
        Assert.assertEquals(actualRow, expectRow);

        KvIterator scanIterator = masterTableSyncClient.scan(tableName, "aa", indexName, 10);
        List<Object[]> scanActualRows = KvIteratorUtil.kvIteratorToListForSchema(scanIterator);
        DataUtil.convertData(scanActualRows);
        Assert.assertEquals(scanActualRows.size(),1);
        Assert.assertEquals(scanActualRows.get(0), dataList.get(0));
        // traverse(String tname)
        {
            KvIterator traverseIterator = masterTableSyncClient.traverse(tableName);
            List<Object[]> traverseActualRows = KvIteratorUtil.kvIteratorToListForSchema(traverseIterator);
            DataUtil.convertData(traverseActualRows);
            Assert.assertEquals(scanActualRows.size(),1);
            Assert.assertEquals(scanActualRows.get(0), dataList.get(0));
        }
    }
}
