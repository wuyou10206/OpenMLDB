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
public class CreateTableFailTest extends OpenMLDBTest {
    @Test
    public void testCreateByColumnDescAndIndex(){
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        NS.ColumnDesc columnDesc0 = NS.ColumnDesc.newBuilder().setName("id").setType("int32").setAddTsIdx(false).build();
        NS.ColumnDesc columnDesc1 = NS.ColumnDesc.newBuilder().setName("c1").setType("string").setAddTsIdx(false).build();
        NS.ColumnDesc columnDesc2 = NS.ColumnDesc.newBuilder().setName("c2").setType("int16").setAddTsIdx(false).build();
        NS.ColumnDesc columnDesc3 = NS.ColumnDesc.newBuilder().setName("c3").setType("int32").setAddTsIdx(false).build();
        NS.ColumnDesc columnDesc4 = NS.ColumnDesc.newBuilder().setName("c4").setType("int64").setAddTsIdx(false).build();
        NS.ColumnDesc columnDesc5 = NS.ColumnDesc.newBuilder().setName("c5").setType("float").setAddTsIdx(true).build();
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
        Assert.assertFalse(RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo));
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
        ColumnKey columnKey0 = ColumnKey.newBuilder().setIndexName("index1").addColName("c2").addTsName("c2").build();
        NS.TableInfo tableInfo = NS.TableInfo.newBuilder()
                .setName(tableName)
                .addColumnDesc(columnDesc0).addColumnDesc(columnDesc1).addColumnDesc(columnDesc2).addColumnDesc(columnDesc3)
                .addColumnDesc(columnDesc4).addColumnDesc(columnDesc5).addColumnDesc(columnDesc6).addColumnDesc(columnDesc7)
                .addColumnDesc(columnDesc8).addColumnDesc(columnDesc9).addColumnKey(columnKey0)
                .setTtl(10).setTtlType("kLatestTime")
                .build();
        log.info("table info:"+tableInfo);
        Assert.assertFalse(RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo));
    }
    // 创建表添加索引 指定ttl生效
    @Test
    public void testCreateByColumnDescV1AndIndexNoTs(){
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        ColumnDesc columnDesc0 = ColumnDesc.newBuilder().setName("id").setType("int32").build();
        ColumnDesc columnDesc1 = ColumnDesc.newBuilder().setName("c1").setType("string").setAddTsIdx(false).build();
        ColumnDesc columnDesc2 = ColumnDesc.newBuilder().setName("c2").setType("int16").build();
        ColumnDesc columnDesc3 = ColumnDesc.newBuilder().setName("c3").setType("int32").build();
        ColumnDesc columnDesc4 = ColumnDesc.newBuilder().setName("c4").setType("int64").build();
        ColumnDesc columnDesc5 = ColumnDesc.newBuilder().setName("c5").setType("float").setAddTsIdx(true).build();
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
        Assert.assertFalse(RtidbUtil.createTable(masterNsc,masterClusterClient,tableInfo));
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
        ColumnKey columnKey0 = ColumnKey.newBuilder().setIndexName("index1").addColName("c1").addTsName("c3").build();
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
}
