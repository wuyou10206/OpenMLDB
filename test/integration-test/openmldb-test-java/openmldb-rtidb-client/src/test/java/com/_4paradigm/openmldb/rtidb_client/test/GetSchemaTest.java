package com._4paradigm.openmldb.rtidb_client.test;

import com._4paradigm.openmldb.rtidb_client.common.OpenMLDBTest;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import lombok.extern.log4j.Log4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;

@Log4j
public class GetSchemaTest extends OpenMLDBTest {
    @Test(dataProvider = "storageModeData")
    public void testGetSchema(String storageMode) throws Exception{
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        String tableDDL = "create table %s(c1 string,c2 smallint,c3 int,c4 bigint,c5 float,c6 double,c7 timestamp,c8 date,c9 bool," +
                "index(key=(c1),ts=c7))options(partitionnum=2,replicanum=3,storage_mode='%s');";
        tableDDL = String.format(tableDDL,tableName,storageMode);
        sdkClient.execute(tableDDL);
        tableNameList.add(tableName);
        List<com._4paradigm.rtidb.client.schema.ColumnDesc> schema = masterTableSyncClient.getSchema(tableName);
        String schemaStr = schema.stream().map(c -> c.getName() + " " + c.getDataType().name().toLowerCase()).collect(Collectors.joining(","));
        String expectStr = "c1 varchar,c2 smallint,c3 int,c4 bigint,c5 float,c6 double,c7 timestamp,c8 date,c9 bool";
        Assert.assertEquals(schemaStr,expectStr,"schema 不一致");
        int tid = masterNsc.showTable(tableName).get(0).getTid();
        List<ColumnDesc> schemaByTid = masterTableSyncClient.getSchema(tid);
        Assert.assertEquals(schema,schemaByTid,"schema 不一致");
    }
}
