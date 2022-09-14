package com._4paradigm.openmldb.rtidb_client.test;

import com._4paradigm.openmldb.rtidb_client.common.OpenMLDBTest;
import com._4paradigm.openmldb.rtidb_client.util.AssertUtil;
import com._4paradigm.openmldb.rtidb_client.util.RtidbUtil;
import com._4paradigm.rtidb.common.Common;
import com._4paradigm.rtidb.common.Common.ColumnDesc;
import com._4paradigm.rtidb.common.Common.ColumnKey;
import com._4paradigm.rtidb.ns.NS;
import com._4paradigm.rtidb.tablet.Tablet;
import lombok.extern.log4j.Log4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import java.util.List;


/**
 * Created by zhangguanglin on 2019/10/17.
 */
@Log4j
public class CreateTableTest extends OpenMLDBTest {
    @Test
    public void testCreateByColumnDescNoIndex(){
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        NS.ColumnDesc columnDesc0 = NS.ColumnDesc.newBuilder().setName("id").setType("int32").setAddTsIdx(false).build();
        NS.ColumnDesc columnDesc1 = NS.ColumnDesc.newBuilder().setName("c1").setType("string").setAddTsIdx(false).build();
        NS.ColumnDesc columnDesc2 = NS.ColumnDesc.newBuilder().setName("c2").setType("int16").setAddTsIdx(false).build();
        NS.ColumnDesc columnDesc3 = NS.ColumnDesc.newBuilder().setName("c3").setType("int32").setAddTsIdx(false).build();
        NS.ColumnDesc columnDesc4 = NS.ColumnDesc.newBuilder().setName("c4").setType("int64").setAddTsIdx(false).build();
        NS.ColumnDesc columnDesc5 = NS.ColumnDesc.newBuilder().setName("c5").setType("float").setAddTsIdx(false).build();
        NS.ColumnDesc columnDesc6 = NS.ColumnDesc.newBuilder().setName("c6").setType("double").setAddTsIdx(false).build();
        NS.ColumnDesc columnDesc7 = NS.ColumnDesc.newBuilder().setName("c7").setType("timestamp").setAddTsIdx(false).build();
        NS.ColumnDesc columnDesc8 = NS.ColumnDesc.newBuilder().setName("c8").setType("date").setAddTsIdx(false).build();
        NS.ColumnDesc columnDesc9 = NS.ColumnDesc.newBuilder().setName("c9").setType("bool").setAddTsIdx(false).build();
        NS.TableInfo tableInfo = NS.TableInfo.newBuilder()
                .setName(tableName)
                .addColumnDesc(columnDesc0).addColumnDesc(columnDesc1).addColumnDesc(columnDesc2).addColumnDesc(columnDesc3)
                .addColumnDesc(columnDesc4).addColumnDesc(columnDesc5).addColumnDesc(columnDesc6).addColumnDesc(columnDesc7)
                .addColumnDesc(columnDesc8).addColumnDesc(columnDesc9)
                .setTtl(10).setTtlType("kLatestTime")
                .build();
        log.info("table info:"+tableInfo);
        Assert.assertTrue(RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo));
        tableNameList.add(tableName);
        NS.TableInfo actualTableInfo = masterNsc.showTable(tableName).get(0);
        List<NS.ColumnDesc> actualColumnDescV1List = actualTableInfo.getColumnDescList();
        List<NS.ColumnDesc> expectColumnDescV1List = Lists.newArrayList(columnDesc0,columnDesc1,columnDesc2,columnDesc3,columnDesc4,columnDesc5,columnDesc6,columnDesc7,columnDesc8,columnDesc9);
        AssertUtil.assertColumnDescList(actualColumnDescV1List,expectColumnDescV1List);
        List<ColumnKey> columnKeyList = actualTableInfo.getColumnKeyList();
        Assert.assertEquals(columnKeyList.size(),1);
        ColumnKey columnKey = columnKeyList.get(0);
        Assert.assertEquals(columnKey.getColName(0),"id");
        Assert.assertEquals(columnKey.getTsNameCount(),0);
        long ttl = actualTableInfo.getTtl();
        Assert.assertEquals(ttl,0);
        Assert.assertEquals(actualTableInfo.getTtlType(),"kAbsoluteTime");
    }
    @Test
    public void testCreateByColumnDescAndIndex(){
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        NS.ColumnDesc columnDesc0 = NS.ColumnDesc.newBuilder().setName("id").setType("int32").setAddTsIdx(false).build();
        NS.ColumnDesc columnDesc1 = NS.ColumnDesc.newBuilder().setName("c1").setType("string").setAddTsIdx(true).build();
        NS.ColumnDesc columnDesc2 = NS.ColumnDesc.newBuilder().setName("c2").setType("int16").setAddTsIdx(false).build();
        NS.ColumnDesc columnDesc3 = NS.ColumnDesc.newBuilder().setName("c3").setType("int32").setAddTsIdx(false).build();
        NS.ColumnDesc columnDesc4 = NS.ColumnDesc.newBuilder().setName("c4").setType("int64").setAddTsIdx(false).build();
        NS.ColumnDesc columnDesc5 = NS.ColumnDesc.newBuilder().setName("c5").setType("float").setAddTsIdx(false).build();
        NS.ColumnDesc columnDesc6 = NS.ColumnDesc.newBuilder().setName("c6").setType("double").setAddTsIdx(false).build();
        NS.ColumnDesc columnDesc7 = NS.ColumnDesc.newBuilder().setName("c7").setType("timestamp").setAddTsIdx(false).build();
        NS.ColumnDesc columnDesc8 = NS.ColumnDesc.newBuilder().setName("c8").setType("date").setAddTsIdx(false).build();
        NS.ColumnDesc columnDesc9 = NS.ColumnDesc.newBuilder().setName("c9").setType("bool").setAddTsIdx(false).build();
        NS.TableInfo tableInfo = NS.TableInfo.newBuilder()
                .setName(tableName)
                .addColumnDesc(columnDesc0).addColumnDesc(columnDesc1).addColumnDesc(columnDesc2).addColumnDesc(columnDesc3)
                .addColumnDesc(columnDesc4).addColumnDesc(columnDesc5).addColumnDesc(columnDesc6).addColumnDesc(columnDesc7)
                .addColumnDesc(columnDesc8).addColumnDesc(columnDesc9)
                .setTtl(10).setTtlType("kLatestTime")
                .build();
        log.info("table info:"+tableInfo);
        Assert.assertTrue(RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo));
        tableNameList.add(tableName);
        NS.TableInfo actualTableInfo = masterNsc.showTable(tableName).get(0);
        List<NS.ColumnDesc> actualColumnDescV1List = actualTableInfo.getColumnDescList();
        List<NS.ColumnDesc> expectColumnDescV1List = Lists.newArrayList(columnDesc0,columnDesc1,columnDesc2,columnDesc3,columnDesc4,columnDesc5,columnDesc6,columnDesc7,columnDesc8,columnDesc9);
        AssertUtil.assertColumnDescList(actualColumnDescV1List,expectColumnDescV1List);
        List<ColumnKey> columnKeyList = actualTableInfo.getColumnKeyList();
        Assert.assertEquals(columnKeyList.size(),1);
        ColumnKey columnKey = columnKeyList.get(0);
        Assert.assertEquals(columnKey.getColName(0),"c1");
        Assert.assertEquals(columnKey.getTsNameCount(),0);
        long ttl = actualTableInfo.getTtl();
        Assert.assertEquals(ttl,10);
        Assert.assertEquals(actualTableInfo.getTtlType(),"kLatestTime");
    }
    @Test
    public void testCreateByColumnDescAndIndexByColumnKey(){
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        NS.ColumnDesc columnDesc0 = NS.ColumnDesc.newBuilder().setName("id").setType("int32").setAddTsIdx(false).build();
        NS.ColumnDesc columnDesc1 = NS.ColumnDesc.newBuilder().setName("c1").setType("string").setAddTsIdx(true).build();
        NS.ColumnDesc columnDesc2 = NS.ColumnDesc.newBuilder().setName("c2").setType("int16").setAddTsIdx(false).build();
        NS.ColumnDesc columnDesc3 = NS.ColumnDesc.newBuilder().setName("c3").setType("int32").setAddTsIdx(false).build();
        NS.ColumnDesc columnDesc4 = NS.ColumnDesc.newBuilder().setName("c4").setType("int64").setAddTsIdx(false).build();
        NS.ColumnDesc columnDesc5 = NS.ColumnDesc.newBuilder().setName("c5").setType("float").setAddTsIdx(false).build();
        NS.ColumnDesc columnDesc6 = NS.ColumnDesc.newBuilder().setName("c6").setType("double").setAddTsIdx(false).build();
        NS.ColumnDesc columnDesc7 = NS.ColumnDesc.newBuilder().setName("c7").setType("timestamp").setAddTsIdx(false).build();
        NS.ColumnDesc columnDesc8 = NS.ColumnDesc.newBuilder().setName("c8").setType("date").setAddTsIdx(false).build();
        NS.ColumnDesc columnDesc9 = NS.ColumnDesc.newBuilder().setName("c9").setType("bool").setAddTsIdx(false).build();
        ColumnKey columnKey0 = ColumnKey.newBuilder().setIndexName("index1").addColName("c2").addTsName("c7").build();
        NS.TableInfo tableInfo = NS.TableInfo.newBuilder()
                .setName(tableName)
                .addColumnDesc(columnDesc0).addColumnDesc(columnDesc1).addColumnDesc(columnDesc2).addColumnDesc(columnDesc3)
                .addColumnDesc(columnDesc4).addColumnDesc(columnDesc5).addColumnDesc(columnDesc6).addColumnDesc(columnDesc7)
                .addColumnDesc(columnDesc8).addColumnDesc(columnDesc9).addColumnKey(columnKey0)
                .setTtl(10).setTtlType("kLatestTime")
                .build();
        log.info("table info:"+tableInfo);
        Assert.assertTrue(RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo));
        tableNameList.add(tableName);
        NS.TableInfo actualTableInfo = masterNsc.showTable(tableName).get(0);
        List<NS.ColumnDesc> actualColumnDescV1List = actualTableInfo.getColumnDescList();
        List<NS.ColumnDesc> expectColumnDescV1List = Lists.newArrayList(columnDesc0,columnDesc1,columnDesc2,columnDesc3,columnDesc4,columnDesc5,columnDesc6,columnDesc7,columnDesc8,columnDesc9);
        AssertUtil.assertColumnDescList(actualColumnDescV1List,expectColumnDescV1List);
        List<ColumnKey> columnKeyList = actualTableInfo.getColumnKeyList();
        Assert.assertEquals(columnKeyList.size(),1);
        ColumnKey columnKey = columnKeyList.get(0);
        Assert.assertEquals(columnKey.getColName(0),"c2");
        Assert.assertEquals(columnKey.getTsNameCount(),1);
        Assert.assertEquals(columnKey.getTsName(0),"c7");
        long ttl = actualTableInfo.getTtl();
        Assert.assertEquals(ttl,10);
        Assert.assertEquals(actualTableInfo.getTtlType(),"kLatestTime");
    }
    @Test
    public void testCreateByColumnDescV1AndNoIndex(){
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        ColumnDesc columnDesc0 = ColumnDesc.newBuilder().setName("id").setType("int32").build();
        ColumnDesc columnDesc1 = ColumnDesc.newBuilder().setName("c1").setType("string").build();
        ColumnDesc columnDesc2 = ColumnDesc.newBuilder().setName("c2").setType("int16").build();
        ColumnDesc columnDesc3 = ColumnDesc.newBuilder().setName("c3").setType("int32").build();
        ColumnDesc columnDesc4 = ColumnDesc.newBuilder().setName("c4").setType("int64").build();
        ColumnDesc columnDesc5 = ColumnDesc.newBuilder().setName("c5").setType("float").build();
        ColumnDesc columnDesc6 = ColumnDesc.newBuilder().setName("c6").setType("double").build();
        ColumnDesc columnDesc7 = ColumnDesc.newBuilder().setName("c7").setType("timestamp").build();
        ColumnDesc columnDesc8 = ColumnDesc.newBuilder().setName("c8").setType("date").build();
        ColumnDesc columnDesc9 = ColumnDesc.newBuilder().setName("c9").setType("bool").build();
        NS.TableInfo tableInfo = NS.TableInfo.newBuilder()
                .setName(tableName)
                .addColumnDescV1(columnDesc0).addColumnDescV1(columnDesc1).addColumnDescV1(columnDesc2).addColumnDescV1(columnDesc3)
                .addColumnDescV1(columnDesc4).addColumnDescV1(columnDesc5).addColumnDescV1(columnDesc6).addColumnDescV1(columnDesc7)
                .addColumnDescV1(columnDesc8).addColumnDescV1(columnDesc9)
                .build();
        log.info("table info:"+tableInfo);
        Assert.assertTrue(RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo));
        tableNameList.add(tableName);
        NS.TableInfo actualTableInfo = masterNsc.showTable(tableName).get(0);
        List<ColumnDesc> actualColumnDescV1List = actualTableInfo.getColumnDescV1List();
        List<ColumnDesc> expectColumnDescV1List = Lists.newArrayList(columnDesc0,columnDesc1,columnDesc2,columnDesc3,columnDesc4,columnDesc5,columnDesc6,columnDesc7,columnDesc8,columnDesc9);
        AssertUtil.assertColumnDescV1List(actualColumnDescV1List,expectColumnDescV1List);
        List<ColumnKey> columnKeyList = actualTableInfo.getColumnKeyList();
        Assert.assertEquals(columnKeyList.size(),1);
        ColumnKey columnKey = columnKeyList.get(0);
        Assert.assertEquals(columnKey.getColName(0),"id");
        Assert.assertEquals(columnKey.getTsNameCount(),0);
        long ttl = actualTableInfo.getTtl();
        Assert.assertEquals(ttl,0);
        Assert.assertEquals(actualTableInfo.getTtlType(),"kAbsoluteTime");
    }
    // 创建表无索引时，指定ttl失效，ttl走默认，0m absolute
    @Test
    public void testCreateByColumnDescV1AndNoIndexByTTL(){
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        ColumnDesc columnDesc0 = ColumnDesc.newBuilder().setName("id").setType("int32").build();
        ColumnDesc columnDesc1 = ColumnDesc.newBuilder().setName("c1").setType("string").build();
        ColumnDesc columnDesc2 = ColumnDesc.newBuilder().setName("c2").setType("int16").build();
        ColumnDesc columnDesc3 = ColumnDesc.newBuilder().setName("c3").setType("int32").build();
        ColumnDesc columnDesc4 = ColumnDesc.newBuilder().setName("c4").setType("int64").build();
        ColumnDesc columnDesc5 = ColumnDesc.newBuilder().setName("c5").setType("float").build();
        ColumnDesc columnDesc6 = ColumnDesc.newBuilder().setName("c6").setType("double").build();
        ColumnDesc columnDesc7 = ColumnDesc.newBuilder().setName("c7").setType("timestamp").build();
        ColumnDesc columnDesc8 = ColumnDesc.newBuilder().setName("c8").setType("date").build();
        ColumnDesc columnDesc9 = ColumnDesc.newBuilder().setName("c9").setType("bool").build();
        NS.TableInfo tableInfo = NS.TableInfo.newBuilder()
                .setName(tableName)
                .addColumnDescV1(columnDesc0).addColumnDescV1(columnDesc1).addColumnDescV1(columnDesc2).addColumnDescV1(columnDesc3)
                .addColumnDescV1(columnDesc4).addColumnDescV1(columnDesc5).addColumnDescV1(columnDesc6).addColumnDescV1(columnDesc7)
                .addColumnDescV1(columnDesc8).addColumnDescV1(columnDesc9)
                .setTtl(10).setTtlType("kLatestTime")
                .build();
        log.info("table info:"+tableInfo);
        Assert.assertTrue(RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo));
        tableNameList.add(tableName);
        NS.TableInfo actualTableInfo = masterNsc.showTable(tableName).get(0);
        List<ColumnDesc> actualColumnDescV1List = actualTableInfo.getColumnDescV1List();
        List<ColumnDesc> expectColumnDescV1List = Lists.newArrayList(columnDesc0,columnDesc1,columnDesc2,columnDesc3,columnDesc4,columnDesc5,columnDesc6,columnDesc7,columnDesc8,columnDesc9);
        AssertUtil.assertColumnDescV1List(actualColumnDescV1List,expectColumnDescV1List);
        List<ColumnKey> columnKeyList = actualTableInfo.getColumnKeyList();
        Assert.assertEquals(columnKeyList.size(),1);
        ColumnKey columnKey = columnKeyList.get(0);
        Assert.assertEquals(columnKey.getColName(0),"id");
        Assert.assertEquals(columnKey.getTsNameCount(),0);
        long ttl = actualTableInfo.getTtl();
        Assert.assertEquals(ttl,0);
        Assert.assertEquals(actualTableInfo.getTtlType(),"kAbsoluteTime");
    }
    // 创建表添加索引 指定ttl生效
    @Test
    public void testCreateByColumnDescV1AndIndexNoTs(){
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        ColumnDesc columnDesc0 = ColumnDesc.newBuilder().setName("id").setType("int32").build();
        ColumnDesc columnDesc1 = ColumnDesc.newBuilder().setName("c1").setType("string").setAddTsIdx(true).build();
        ColumnDesc columnDesc2 = ColumnDesc.newBuilder().setName("c2").setType("int16").build();
        ColumnDesc columnDesc3 = ColumnDesc.newBuilder().setName("c3").setType("int32").build();
        ColumnDesc columnDesc4 = ColumnDesc.newBuilder().setName("c4").setType("int64").build();
        ColumnDesc columnDesc5 = ColumnDesc.newBuilder().setName("c5").setType("float").build();
        ColumnDesc columnDesc6 = ColumnDesc.newBuilder().setName("c6").setType("double").build();
        ColumnDesc columnDesc7 = ColumnDesc.newBuilder().setName("c7").setType("timestamp").build();
        ColumnDesc columnDesc8 = ColumnDesc.newBuilder().setName("c8").setType("date").build();
        ColumnDesc columnDesc9 = ColumnDesc.newBuilder().setName("c9").setType("bool").build();
        NS.TableInfo tableInfo = NS.TableInfo.newBuilder()
                .setName(tableName)
                .addColumnDescV1(columnDesc0).addColumnDescV1(columnDesc1).addColumnDescV1(columnDesc2).addColumnDescV1(columnDesc3)
                .addColumnDescV1(columnDesc4).addColumnDescV1(columnDesc5).addColumnDescV1(columnDesc6).addColumnDescV1(columnDesc7)
                .addColumnDescV1(columnDesc8).addColumnDescV1(columnDesc9)
                .setTtl(10).setTtlType("kLatestTime")
                .build();
        log.info("table info:"+tableInfo);
        Assert.assertTrue(RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo));
        tableNameList.add(tableName);
        NS.TableInfo actualTableInfo = masterNsc.showTable(tableName).get(0);
        List<ColumnDesc> actualColumnDescV1List = actualTableInfo.getColumnDescV1List();
        List<ColumnDesc> expectColumnDescV1List = Lists.newArrayList(columnDesc0,columnDesc1,columnDesc2,columnDesc3,columnDesc4,columnDesc5,columnDesc6,columnDesc7,columnDesc8,columnDesc9);
        AssertUtil.assertColumnDescV1List(actualColumnDescV1List,expectColumnDescV1List);
        List<ColumnKey> columnKeyList = actualTableInfo.getColumnKeyList();
        Assert.assertEquals(columnKeyList.size(),1);
        ColumnKey columnKey = columnKeyList.get(0);
        Assert.assertEquals(columnKey.getColName(0),"c1");
        Assert.assertEquals(columnKey.getTsNameCount(),0);
        long ttl = actualTableInfo.getTtl();
        Assert.assertEquals(ttl,10);
        Assert.assertEquals(actualTableInfo.getTtlType(),"kLatestTime");
    }
    // 创建表有索引没有ts  指定TtlDEsc生效
    @Test
    public void testCreateByColumnDescV1AndIndexNoTsByTTLDesc(){
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        ColumnDesc columnDesc0 = ColumnDesc.newBuilder().setName("id").setType("int32").build();
        ColumnDesc columnDesc1 = ColumnDesc.newBuilder().setName("c1").setType("string").setAddTsIdx(true).build();
        ColumnDesc columnDesc2 = ColumnDesc.newBuilder().setName("c2").setType("int16").build();
        ColumnDesc columnDesc3 = ColumnDesc.newBuilder().setName("c3").setType("int32").build();
        ColumnDesc columnDesc4 = ColumnDesc.newBuilder().setName("c4").setType("int64").build();
        ColumnDesc columnDesc5 = ColumnDesc.newBuilder().setName("c5").setType("float").build();
        ColumnDesc columnDesc6 = ColumnDesc.newBuilder().setName("c6").setType("double").build();
        ColumnDesc columnDesc7 = ColumnDesc.newBuilder().setName("c7").setType("timestamp").build();
        ColumnDesc columnDesc8 = ColumnDesc.newBuilder().setName("c8").setType("date").build();
        ColumnDesc columnDesc9 = ColumnDesc.newBuilder().setName("c9").setType("bool").build();
        Tablet.TTLDesc ttlDesc = Tablet.TTLDesc.newBuilder().setTtlType(Tablet.TTLType.kAbsOrLat).setAbsTtl(10).setLatTtl(3).build();
        NS.TableInfo tableInfo = NS.TableInfo.newBuilder()
                .setName(tableName)
                .addColumnDescV1(columnDesc0).addColumnDescV1(columnDesc1).addColumnDescV1(columnDesc2).addColumnDescV1(columnDesc3)
                .addColumnDescV1(columnDesc4).addColumnDescV1(columnDesc5).addColumnDescV1(columnDesc6).addColumnDescV1(columnDesc7)
                .addColumnDescV1(columnDesc8).addColumnDescV1(columnDesc9)
                .setTtlDesc(ttlDesc)
                .build();
        log.info("table info:"+tableInfo);
        Assert.assertTrue(RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo));
        tableNameList.add(tableName);
        NS.TableInfo actualTableInfo = masterNsc.showTable(tableName).get(0);
        List<ColumnDesc> actualColumnDescV1List = actualTableInfo.getColumnDescV1List();
        List<ColumnDesc> expectColumnDescV1List = Lists.newArrayList(columnDesc0,columnDesc1,columnDesc2,columnDesc3,columnDesc4,columnDesc5,columnDesc6,columnDesc7,columnDesc8,columnDesc9);
        AssertUtil.assertColumnDescV1List(actualColumnDescV1List,expectColumnDescV1List);
        List<ColumnKey> columnKeyList = actualTableInfo.getColumnKeyList();
        Assert.assertEquals(columnKeyList.size(),1);
        ColumnKey columnKey = columnKeyList.get(0);
        Assert.assertEquals(columnKey.getColName(0),"c1");
        Assert.assertEquals(columnKey.getTsNameCount(),0);
        Tablet.TTLDesc actualTtlDesc = actualTableInfo.getTtlDesc();
        Assert.assertEquals(actualTtlDesc.getAbsTtl(),10);
        Assert.assertEquals(actualTtlDesc.getLatTtl(),3);
        Assert.assertEquals(actualTtlDesc.getTtlType(), Tablet.TTLType.kAbsOrLat);
    }
    // 在列上指定索引，指定ts无效
    @Test
    public void testCreateByColumnDescV1AndIndexTs(){
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        ColumnDesc columnDesc0 = ColumnDesc.newBuilder().setName("id").setType("int32").build();
        ColumnDesc columnDesc1 = ColumnDesc.newBuilder().setName("c1").setType("string").setAddTsIdx(true).build();
        ColumnDesc columnDesc2 = ColumnDesc.newBuilder().setName("c2").setType("int16").build();
        ColumnDesc columnDesc3 = ColumnDesc.newBuilder().setName("c3").setType("int32").build();
        ColumnDesc columnDesc4 = ColumnDesc.newBuilder().setName("c4").setType("int64").build();
        ColumnDesc columnDesc5 = ColumnDesc.newBuilder().setName("c5").setType("float").build();
        ColumnDesc columnDesc6 = ColumnDesc.newBuilder().setName("c6").setType("double").build();
        ColumnDesc columnDesc7 = ColumnDesc.newBuilder().setName("c7").setType("timestamp").setIsTsCol(true).build();
        ColumnDesc columnDesc8 = ColumnDesc.newBuilder().setName("c8").setType("date").build();
        ColumnDesc columnDesc9 = ColumnDesc.newBuilder().setName("c9").setType("bool").build();
        NS.TableInfo tableInfo = NS.TableInfo.newBuilder()
                .setName(tableName)
                .addColumnDescV1(columnDesc0).addColumnDescV1(columnDesc1).addColumnDescV1(columnDesc2).addColumnDescV1(columnDesc3)
                .addColumnDescV1(columnDesc4).addColumnDescV1(columnDesc5).addColumnDescV1(columnDesc6).addColumnDescV1(columnDesc7)
                .addColumnDescV1(columnDesc8).addColumnDescV1(columnDesc9)
                .setTtl(10).setTtlType("kAbsoluteTime")
                .build();
        log.info("table info:"+tableInfo);
        Assert.assertTrue(RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo));
        tableNameList.add(tableName);
        NS.TableInfo actualTableInfo = masterNsc.showTable(tableName).get(0);
        System.out.println("actualTableInfo = " + actualTableInfo);
        List<ColumnDesc> actualColumnDescV1List = actualTableInfo.getColumnDescV1List();
        List<ColumnDesc> expectColumnDescV1List = Lists.newArrayList(columnDesc0,columnDesc1,columnDesc2,columnDesc3,columnDesc4,columnDesc5,columnDesc6,columnDesc7,columnDesc8,columnDesc9);
        AssertUtil.assertColumnDescV1List(actualColumnDescV1List,expectColumnDescV1List);
        List<ColumnKey> columnKeyList = actualTableInfo.getColumnKeyList();
        Assert.assertEquals(columnKeyList.size(),1);
        ColumnKey columnKey = columnKeyList.get(0);
        Assert.assertEquals(columnKey.getColName(0),"c1");
        Assert.assertEquals(columnKey.getTsNameCount(),0);
        long ttl = actualTableInfo.getTtl();
        Assert.assertEquals(ttl,10);
        Assert.assertEquals(actualTableInfo.getTtlType(),"kAbsoluteTime");
    }
    @Test
    public void testCreateByColumnDescV1ByColumnKey(){
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        ColumnDesc columnDesc0 = ColumnDesc.newBuilder().setName("id").setType("int32").build();
        ColumnDesc columnDesc1 = ColumnDesc.newBuilder().setName("c1").setType("string").build();
        ColumnDesc columnDesc2 = ColumnDesc.newBuilder().setName("c2").setType("int16").build();
        ColumnDesc columnDesc3 = ColumnDesc.newBuilder().setName("c3").setType("int32").build();
        ColumnDesc columnDesc4 = ColumnDesc.newBuilder().setName("c4").setType("int64").build();
        ColumnDesc columnDesc5 = ColumnDesc.newBuilder().setName("c5").setType("float").build();
        ColumnDesc columnDesc6 = ColumnDesc.newBuilder().setName("c6").setType("double").build();
        ColumnDesc columnDesc7 = ColumnDesc.newBuilder().setName("c7").setType("timestamp").build();
        ColumnDesc columnDesc8 = ColumnDesc.newBuilder().setName("c8").setType("date").build();
        ColumnDesc columnDesc9 = ColumnDesc.newBuilder().setName("c9").setType("bool").build();
        ColumnKey columnKey0 = ColumnKey.newBuilder().setIndexName("index1").addColName("c1").addTsName("c7").build();
        NS.TableInfo tableInfo = NS.TableInfo.newBuilder()
                .setName(tableName)
                .addColumnDescV1(columnDesc0).addColumnDescV1(columnDesc1).addColumnDescV1(columnDesc2).addColumnDescV1(columnDesc3)
                .addColumnDescV1(columnDesc4).addColumnDescV1(columnDesc5).addColumnDescV1(columnDesc6).addColumnDescV1(columnDesc7)
                .addColumnDescV1(columnDesc8).addColumnDescV1(columnDesc9)
                .addColumnKey(columnKey0)
                .build();
        log.info("table info:"+tableInfo);
        Assert.assertTrue(RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo));
        tableNameList.add(tableName);
        NS.TableInfo actualTableInfo = masterNsc.showTable(tableName).get(0);
        List<ColumnDesc> actualColumnDescV1List = actualTableInfo.getColumnDescV1List();
        List<ColumnDesc> expectColumnDescV1List = Lists.newArrayList(columnDesc0,columnDesc1,columnDesc2,columnDesc3,columnDesc4,columnDesc5,columnDesc6,columnDesc7,columnDesc8,columnDesc9);
        AssertUtil.assertColumnDescV1List(actualColumnDescV1List,expectColumnDescV1List);
        List<ColumnKey> actualColumnKeyList = actualTableInfo.getColumnKeyList();
        List<ColumnKey> expectColumnKeyList = Lists.newArrayList(columnKey0);
        AssertUtil.assertColumnKeyList(actualColumnKeyList,expectColumnKeyList);
    }
    @Test
    public void testCreateByColumnDescV1ByColumnKey2(){
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        ColumnDesc columnDesc0 = ColumnDesc.newBuilder().setName("id").setType("int32").build();
        ColumnDesc columnDesc1 = ColumnDesc.newBuilder().setName("c1").setType("string").build();
        ColumnDesc columnDesc2 = ColumnDesc.newBuilder().setName("c2").setType("int16").setAddTsIdx(true).build();
        ColumnDesc columnDesc3 = ColumnDesc.newBuilder().setName("c3").setType("int32").build();
        ColumnDesc columnDesc4 = ColumnDesc.newBuilder().setName("c4").setType("int64").setIsTsCol(true).build();
        ColumnDesc columnDesc5 = ColumnDesc.newBuilder().setName("c5").setType("float").build();
        ColumnDesc columnDesc6 = ColumnDesc.newBuilder().setName("c6").setType("double").build();
        ColumnDesc columnDesc7 = ColumnDesc.newBuilder().setName("c7").setType("timestamp").build();
        ColumnDesc columnDesc8 = ColumnDesc.newBuilder().setName("c8").setType("date").build();
        ColumnDesc columnDesc9 = ColumnDesc.newBuilder().setName("c9").setType("bool").build();
        ColumnKey columnKey0 = ColumnKey.newBuilder().setIndexName("index1").addColName("c1").addTsName("c7").build();
        NS.TableInfo tableInfo = NS.TableInfo.newBuilder()
                .setName(tableName)
                .addColumnDescV1(columnDesc0).addColumnDescV1(columnDesc1).addColumnDescV1(columnDesc2).addColumnDescV1(columnDesc3)
                .addColumnDescV1(columnDesc4).addColumnDescV1(columnDesc5).addColumnDescV1(columnDesc6).addColumnDescV1(columnDesc7)
                .addColumnDescV1(columnDesc8).addColumnDescV1(columnDesc9)
                .addColumnKey(columnKey0)
                .build();
        log.info("table info:"+tableInfo);
        Assert.assertTrue(RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo));
        tableNameList.add(tableName);
        NS.TableInfo actualTableInfo = masterNsc.showTable(tableName).get(0);
        List<ColumnDesc> actualColumnDescV1List = actualTableInfo.getColumnDescV1List();
        List<ColumnDesc> expectColumnDescV1List = Lists.newArrayList(columnDesc0,columnDesc1,columnDesc2,columnDesc3,columnDesc4,columnDesc5,columnDesc6,columnDesc7,columnDesc8,columnDesc9);
        AssertUtil.assertColumnDescV1List(actualColumnDescV1List,expectColumnDescV1List);
        List<ColumnKey> actualColumnKeyList = actualTableInfo.getColumnKeyList();
        List<ColumnKey> expectColumnKeyList = Lists.newArrayList(columnKey0);
        AssertUtil.assertColumnKeyList(actualColumnKeyList,expectColumnKeyList);
    }
    @Test
    public void testCreateByColumnDescV1ByColumnKeyAndTwoTs(){
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        ColumnDesc columnDesc0 = ColumnDesc.newBuilder().setName("id").setType("int32").build();
        ColumnDesc columnDesc1 = ColumnDesc.newBuilder().setName("c1").setType("string").build();
        ColumnDesc columnDesc2 = ColumnDesc.newBuilder().setName("c2").setType("int16").build();
        ColumnDesc columnDesc3 = ColumnDesc.newBuilder().setName("c3").setType("int32").build();
        ColumnDesc columnDesc4 = ColumnDesc.newBuilder().setName("c4").setType("int64").build();
        ColumnDesc columnDesc5 = ColumnDesc.newBuilder().setName("c5").setType("float").build();
        ColumnDesc columnDesc6 = ColumnDesc.newBuilder().setName("c6").setType("double").build();
        ColumnDesc columnDesc7 = ColumnDesc.newBuilder().setName("c7").setType("timestamp").build();
        ColumnDesc columnDesc8 = ColumnDesc.newBuilder().setName("c8").setType("date").build();
        ColumnDesc columnDesc9 = ColumnDesc.newBuilder().setName("c9").setType("bool").build();
        ColumnKey columnKey0 = ColumnKey.newBuilder().setIndexName("index1").addColName("c1").addTsName("c4").addTsName("c7").build();
        NS.TableInfo tableInfo = NS.TableInfo.newBuilder()
                .setName(tableName)
                .addColumnDescV1(columnDesc0).addColumnDescV1(columnDesc1).addColumnDescV1(columnDesc2).addColumnDescV1(columnDesc3)
                .addColumnDescV1(columnDesc4).addColumnDescV1(columnDesc5).addColumnDescV1(columnDesc6).addColumnDescV1(columnDesc7)
                .addColumnDescV1(columnDesc8).addColumnDescV1(columnDesc9)
                .addColumnKey(columnKey0)
                .build();
        log.info("table info:"+tableInfo);
        Assert.assertFalse(RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo));
    }
    @Test
    public void testCreateByColumnDescV1ByColumnKeyByTTL(){
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        ColumnDesc columnDesc0 = ColumnDesc.newBuilder().setName("id").setType("int32").build();
        ColumnDesc columnDesc1 = ColumnDesc.newBuilder().setName("c1").setType("string").build();
        ColumnDesc columnDesc2 = ColumnDesc.newBuilder().setName("c2").setType("int16").build();
        ColumnDesc columnDesc3 = ColumnDesc.newBuilder().setName("c3").setType("int32").build();
        ColumnDesc columnDesc4 = ColumnDesc.newBuilder().setName("c4").setType("int64").build();
        ColumnDesc columnDesc5 = ColumnDesc.newBuilder().setName("c5").setType("float").build();
        ColumnDesc columnDesc6 = ColumnDesc.newBuilder().setName("c6").setType("double").build();
        ColumnDesc columnDesc7 = ColumnDesc.newBuilder().setName("c7").setType("timestamp").build();
        ColumnDesc columnDesc8 = ColumnDesc.newBuilder().setName("c8").setType("date").build();
        ColumnDesc columnDesc9 = ColumnDesc.newBuilder().setName("c9").setType("bool").build();
        ColumnKey columnKey0 = ColumnKey.newBuilder().setIndexName("index1").addColName("c1").addTsName("c7").build();
        NS.TableInfo tableInfo = NS.TableInfo.newBuilder()
                .setName(tableName)
                .addColumnDescV1(columnDesc0).addColumnDescV1(columnDesc1).addColumnDescV1(columnDesc2).addColumnDescV1(columnDesc3)
                .addColumnDescV1(columnDesc4).addColumnDescV1(columnDesc5).addColumnDescV1(columnDesc6).addColumnDescV1(columnDesc7)
                .addColumnDescV1(columnDesc8).addColumnDescV1(columnDesc9)
                .addColumnKey(columnKey0)
                .setTtl(10).setTtlType("kAbsoluteTime")
                .build();
        log.info("table info:"+tableInfo);
        Assert.assertTrue(RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo));
        tableNameList.add(tableName);
        NS.TableInfo actualTableInfo = masterNsc.showTable(tableName).get(0);
        List<ColumnDesc> actualColumnDescV1List = actualTableInfo.getColumnDescV1List();
        List<ColumnDesc> expectColumnDescV1List = Lists.newArrayList(columnDesc0,columnDesc1,columnDesc2,columnDesc3,columnDesc4,columnDesc5,columnDesc6,columnDesc7,columnDesc8,columnDesc9);
        AssertUtil.assertColumnDescV1List(actualColumnDescV1List,expectColumnDescV1List);
        List<ColumnKey> actualColumnKeyList = actualTableInfo.getColumnKeyList();
        List<ColumnKey> expectColumnKeyList = Lists.newArrayList(columnKey0);
        AssertUtil.assertColumnKeyList(actualColumnKeyList,expectColumnKeyList);
        long ttl = actualTableInfo.getTtl();
        Assert.assertEquals(ttl,10);
        Assert.assertEquals(actualTableInfo.getTtlType(),"kAbsoluteTime");
    }
    @Test
    public void testCreateByColumnDescV1ByColumnKeyByTTLDesc(){
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        ColumnDesc columnDesc0 = ColumnDesc.newBuilder().setName("id").setType("int32").build();
        ColumnDesc columnDesc1 = ColumnDesc.newBuilder().setName("c1").setType("string").build();
        ColumnDesc columnDesc2 = ColumnDesc.newBuilder().setName("c2").setType("int16").build();
        ColumnDesc columnDesc3 = ColumnDesc.newBuilder().setName("c3").setType("int32").build();
        ColumnDesc columnDesc4 = ColumnDesc.newBuilder().setName("c4").setType("int64").build();
        ColumnDesc columnDesc5 = ColumnDesc.newBuilder().setName("c5").setType("float").build();
        ColumnDesc columnDesc6 = ColumnDesc.newBuilder().setName("c6").setType("double").build();
        ColumnDesc columnDesc7 = ColumnDesc.newBuilder().setName("c7").setType("timestamp").build();
        ColumnDesc columnDesc8 = ColumnDesc.newBuilder().setName("c8").setType("date").build();
        ColumnDesc columnDesc9 = ColumnDesc.newBuilder().setName("c9").setType("bool").build();
        ColumnKey columnKey0 = ColumnKey.newBuilder().setIndexName("index1").addColName("c1").addTsName("c7").build();
        Tablet.TTLDesc ttlDesc = Tablet.TTLDesc.newBuilder().setTtlType(Tablet.TTLType.kAbsoluteTime).setAbsTtl(10).build();
        NS.TableInfo tableInfo = NS.TableInfo.newBuilder()
                .setName(tableName)
                .addColumnDescV1(columnDesc0).addColumnDescV1(columnDesc1).addColumnDescV1(columnDesc2).addColumnDescV1(columnDesc3)
                .addColumnDescV1(columnDesc4).addColumnDescV1(columnDesc5).addColumnDescV1(columnDesc6).addColumnDescV1(columnDesc7)
                .addColumnDescV1(columnDesc8).addColumnDescV1(columnDesc9)
                .addColumnKey(columnKey0)
                .setTtlDesc(ttlDesc)
                .build();
        log.info("table info:"+tableInfo);
        Assert.assertTrue(RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo));
        tableNameList.add(tableName);
        NS.TableInfo actualTableInfo = masterNsc.showTable(tableName).get(0);
        List<ColumnDesc> actualColumnDescV1List = actualTableInfo.getColumnDescV1List();
        List<ColumnDesc> expectColumnDescV1List = Lists.newArrayList(columnDesc0,columnDesc1,columnDesc2,columnDesc3,columnDesc4,columnDesc5,columnDesc6,columnDesc7,columnDesc8,columnDesc9);
        AssertUtil.assertColumnDescV1List(actualColumnDescV1List,expectColumnDescV1List);
        List<ColumnKey> actualColumnKeyList = actualTableInfo.getColumnKeyList();
        List<ColumnKey> expectColumnKeyList = Lists.newArrayList(columnKey0);
        AssertUtil.assertColumnKeyList(actualColumnKeyList,expectColumnKeyList);
        Tablet.TTLDesc actualTtlDesc = actualTableInfo.getTtlDesc();
        Assert.assertEquals(actualTtlDesc.getAbsTtl(),10);
        Assert.assertEquals(actualTtlDesc.getTtlType(), Tablet.TTLType.kAbsoluteTime);
    }
    @Test(dataProvider = "storageModeData")
    public void testCreateAllParameter(Common.StorageMode storage){
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        ColumnDesc columnDesc0 = ColumnDesc.newBuilder().setName("id").setType("int32").build();
        ColumnDesc columnDesc1 = ColumnDesc.newBuilder().setName("c1").setType("string").build();
        ColumnDesc columnDesc2 = ColumnDesc.newBuilder().setName("c2").setType("int16").build();
        ColumnDesc columnDesc3 = ColumnDesc.newBuilder().setName("c3").setType("int32").build();
        ColumnDesc columnDesc4 = ColumnDesc.newBuilder().setName("c4").setType("int64").build();
        ColumnDesc columnDesc5 = ColumnDesc.newBuilder().setName("c5").setType("float").build();
        ColumnDesc columnDesc6 = ColumnDesc.newBuilder().setName("c6").setType("double").build();
        ColumnDesc columnDesc7 = ColumnDesc.newBuilder().setName("c7").setType("timestamp").build();
        ColumnDesc columnDesc8 = ColumnDesc.newBuilder().setName("c8").setType("date").build();
        ColumnDesc columnDesc9 = ColumnDesc.newBuilder().setName("c9").setType("bool").build();
        ColumnKey columnKey0 = ColumnKey.newBuilder().setIndexName("index1").addColName("c1").addTsName("c7").build();
        Tablet.TTLDesc ttlDesc = Tablet.TTLDesc.newBuilder().setTtlType(Tablet.TTLType.kAbsoluteTime).setAbsTtl(10).build();
        NS.TableInfo tableInfo = NS.TableInfo.newBuilder()
                .setName(tableName)
                .addColumnDescV1(columnDesc0).addColumnDescV1(columnDesc1).addColumnDescV1(columnDesc2).addColumnDescV1(columnDesc3)
                .addColumnDescV1(columnDesc4).addColumnDescV1(columnDesc5).addColumnDescV1(columnDesc6).addColumnDescV1(columnDesc7)
                .addColumnDescV1(columnDesc8).addColumnDescV1(columnDesc9)
                .addColumnKey(columnKey0)
                .setTtlDesc(ttlDesc)
                .setReplicaNum(2).setPartitionNum(3).setStorageMode(storage)
                .build();
        log.info("table info:"+tableInfo);
        Assert.assertTrue(RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo));
        tableNameList.add(tableName);
        NS.TableInfo actualTableInfo = masterNsc.showTable(tableName).get(0);
        List<ColumnDesc> actualColumnDescV1List = actualTableInfo.getColumnDescV1List();
        List<ColumnDesc> expectColumnDescV1List = Lists.newArrayList(columnDesc0,columnDesc1,columnDesc2,columnDesc3,columnDesc4,columnDesc5,columnDesc6,columnDesc7,columnDesc8,columnDesc9);
        AssertUtil.assertColumnDescV1List(actualColumnDescV1List,expectColumnDescV1List);
        List<ColumnKey> actualColumnKeyList = actualTableInfo.getColumnKeyList();
        List<ColumnKey> expectColumnKeyList = Lists.newArrayList(columnKey0);
        AssertUtil.assertColumnKeyList(actualColumnKeyList,expectColumnKeyList);
        Tablet.TTLDesc actualTtlDesc = actualTableInfo.getTtlDesc();
        Assert.assertEquals(actualTtlDesc.getAbsTtl(),10);
        Assert.assertEquals(actualTtlDesc.getTtlType(), Tablet.TTLType.kAbsoluteTime);
        Assert.assertEquals(actualTableInfo.getReplicaNum(),2);
        Assert.assertEquals(actualTableInfo.getPartitionNum(),3);
        Assert.assertEquals(actualTableInfo.getStorageMode(),storage);
    }
    @Test(dataProvider = "storageModeData")
    public void testCreateAllParameterByMoreTTL(Common.StorageMode storage){
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        ColumnDesc columnDesc0 = ColumnDesc.newBuilder().setName("id").setType("int32").build();
        ColumnDesc columnDesc1 = ColumnDesc.newBuilder().setName("c1").setType("string").build();
        ColumnDesc columnDesc2 = ColumnDesc.newBuilder().setName("c2").setType("int16").build();
        ColumnDesc columnDesc3 = ColumnDesc.newBuilder().setName("c3").setType("int32").build();
        ColumnDesc columnDesc4 = ColumnDesc.newBuilder().setName("c4").setType("int64").build();
        ColumnDesc columnDesc5 = ColumnDesc.newBuilder().setName("c5").setType("float").build();
        ColumnDesc columnDesc6 = ColumnDesc.newBuilder().setName("c6").setType("double").build();
        ColumnDesc columnDesc7 = ColumnDesc.newBuilder().setName("c7").setType("timestamp").setIsTsCol(true).setLatTtl(11).build();
        ColumnDesc columnDesc8 = ColumnDesc.newBuilder().setName("c8").setType("date").build();
        ColumnDesc columnDesc9 = ColumnDesc.newBuilder().setName("c9").setType("bool").build();
        ColumnKey columnKey0 = ColumnKey.newBuilder().setIndexName("index1").addColName("c1").addTsName("c7").build();
        Tablet.TTLDesc ttlDesc = Tablet.TTLDesc.newBuilder().setTtlType(Tablet.TTLType.kAbsoluteTime).setAbsTtl(10).build();
        NS.TableInfo tableInfo = NS.TableInfo.newBuilder()
                .setName(tableName)
                .addColumnDescV1(columnDesc0).addColumnDescV1(columnDesc1).addColumnDescV1(columnDesc2).addColumnDescV1(columnDesc3)
                .addColumnDescV1(columnDesc4).addColumnDescV1(columnDesc5).addColumnDescV1(columnDesc6).addColumnDescV1(columnDesc7)
                .addColumnDescV1(columnDesc8).addColumnDescV1(columnDesc9)
                .addColumnKey(columnKey0)
                .setTtlDesc(ttlDesc)
                .setReplicaNum(2).setPartitionNum(3).setStorageMode(storage)
                .build();
        log.info("table info:"+tableInfo);
        Assert.assertTrue(RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo));
        tableNameList.add(tableName);
        NS.TableInfo actualTableInfo = masterNsc.showTable(tableName).get(0);
        List<ColumnDesc> actualColumnDescV1List = actualTableInfo.getColumnDescV1List();
        List<ColumnDesc> expectColumnDescV1List = Lists.newArrayList(columnDesc0,columnDesc1,columnDesc2,columnDesc3,columnDesc4,columnDesc5,columnDesc6,columnDesc7,columnDesc8,columnDesc9);
        AssertUtil.assertColumnDescV1List(actualColumnDescV1List,expectColumnDescV1List);
        List<ColumnKey> actualColumnKeyList = actualTableInfo.getColumnKeyList();
        List<ColumnKey> expectColumnKeyList = Lists.newArrayList(columnKey0);
        AssertUtil.assertColumnKeyList(actualColumnKeyList,expectColumnKeyList);
        long ttl = actualTableInfo.getTtl();
        Assert.assertEquals(ttl,11);
        Assert.assertEquals(actualTableInfo.getTtlType(),"kAbsoluteTime");
        Tablet.TTLDesc actualTtlDesc = actualTableInfo.getTtlDesc();
        Assert.assertEquals(actualTtlDesc.getAbsTtl(),11);
        Assert.assertEquals(actualTtlDesc.getTtlType(), Tablet.TTLType.kAbsoluteTime);
        Assert.assertEquals(actualTableInfo.getReplicaNum(),2);
        Assert.assertEquals(actualTableInfo.getPartitionNum(),3);
        Assert.assertEquals(actualTableInfo.getStorageMode(),storage);
    }
}
