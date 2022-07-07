package com._4paradigm.openmldb.devops_test.high_availability;

import com._4paradigm.openmldb.devops_test.common.ClusterTest;
import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBGlobalVar;
import com._4paradigm.openmldb.test_common.openmldb.SDKClient;
import com._4paradigm.openmldb.test_common.util.SDKByJDBCUtil;
import com._4paradigm.openmldb.test_common.util.SDKUtil;
import com._4paradigm.qa.openmldb_deploy.util.Tool;
import com._4paradigm.test_tool.command_tool.common.ExecutorUtil;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class TestCluster extends ClusterTest {
    @Test
    public void testMoreReplica(){
        SDKClient sdkClient = SDKClient.of(executor);
        // 创建磁盘表和内存表。
        String dbName = "test_devops4";
        String memoryTable = "test_memory";
        String ssdTable = "test_ssd";
        String hddTable = "test_hdd";
        int dataCount = 100;
        sdkClient.createAndUseDB(dbName);
        String memoryTableDDL = "create table test_memory(\n" +
                "c1 string,\n" +
                "c2 smallint,\n" +
                "c3 int,\n" +
                "c4 bigint,\n" +
                "c5 float,\n" +
                "c6 double,\n" +
                "c7 timestamp,\n" +
                "c8 date,\n" +
                "c9 bool,\n" +
                "index(key=(c1),ts=c7))options(partitionnum=8,replicanum=3);";
        String ssdTableDDL = "create table test_ssd(\n" +
                "c1 string,\n" +
                "c2 smallint,\n" +
                "c3 int,\n" +
                "c4 bigint,\n" +
                "c5 float,\n" +
                "c6 double,\n" +
                "c7 timestamp,\n" +
                "c8 date,\n" +
                "c9 bool,\n" +
                "index(key=(c1),ts=c7))options(partitionnum=8,replicanum=3,storage_mode=\"SSD\");";
        String hddTableDDL = "create table test_hdd(\n" +
                "c1 string,\n" +
                "c2 smallint,\n" +
                "c3 int,\n" +
                "c4 bigint,\n" +
                "c5 float,\n" +
                "c6 double,\n" +
                "c7 timestamp,\n" +
                "c8 date,\n" +
                "c9 bool,\n" +
                "index(key=(c1),ts=c7))options(partitionnum=8,replicanum=3,storage_mode=\"HDD\");";
        sdkClient.execute(Lists.newArrayList(memoryTableDDL,ssdTableDDL,hddTableDDL));
        // 插入一定量的数据
        List<List<Object>> dataList = new ArrayList<>();
        for(int i=0;i<dataCount;i++){
            List<Object> list = Lists.newArrayList("aa" + i, 1, 2, 3, 1.1, 2.1, 1590738989000L, "2020-05-01", true);
            dataList.add(list);
        }
        sdkClient.insertList(memoryTable,dataList);
        sdkClient.insertList(ssdTable,dataList);
        sdkClient.insertList(hddTable,dataList);
        // 其中一个tablet stop，leader 内存表和磁盘表可以正常访问，flower 内存表和磁盘表可以正常访问。
        String basePath = OpenMLDBGlobalVar.mainInfo.getBasePath();
        String stopOneTabletCommand = String.format("sh %s/openmldb-tablet-1/bin/start.sh stop tablet",basePath);
        ExecutorUtil.run(stopOneTabletCommand);
        Tool.sleep(5*1000);
        String selectMemory = String.format("select c1 from %s;",memoryTable);
        String selectSSD = String.format("select c1 from %s;",ssdTable);
        String selectHDD = String.format("select c1 from %s;",hddTable);
        OpenMLDBResult memoryResult = sdkClient.execute(selectMemory);
        OpenMLDBResult ssdResult = sdkClient.execute(selectSSD);
        OpenMLDBResult hddResult = sdkClient.execute(selectHDD);
        String oneTabletStopMsg = "tablet1 stop tablet row count check failed.";
        Assert.assertEquals(memoryResult.getCount(),dataCount,oneTabletStopMsg);
        Assert.assertEquals(ssdResult.getCount(),dataCount,oneTabletStopMsg);
        Assert.assertEquals(hddResult.getCount(),dataCount,oneTabletStopMsg);
        // tablet start，数据可以回复，要看磁盘表和内存表。
        String startOneTabletCommand = String.format("sh %s/openmldb-tablet-1/bin/start.sh start tablet",basePath);
        ExecutorUtil.run(startOneTabletCommand);
        Tool.sleep(5*1000);

        //创建磁盘表和内存表，在重启tablet，数据可回复，内存表和磁盘表可以正常访问。
        //创建磁盘表和内存表，插入一些数据，然后make snapshot，在重启tablet，数据可回复。
        //tablet 依次restart，数据可回复，可以访问。
        //3个tablet stop，不能访问。
        // 1个tablet启动，数据可回复，分片所在的表，可以访问。
        //ns stop，可以正常访问。
        //2个ns stop，不能访问。
        //ns start 可以访问。
        //一个 zk stop，可以正常访问
        //3个zk stop，不能正常访问。
        //一个zk start，可正常访问。
        //3个 zk start，可正常访问。
        // 一个节点（ns leader 所在服务器）重启，leader可以正常访问，flower可以正常访问。
        //一直查询某一个表，然后重启一个机器。
    }
}
