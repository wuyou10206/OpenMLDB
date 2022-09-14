package com._4paradigm.openmldb.rtidb_client.test;

import com._4paradigm.openmldb.rtidb_client.common.OpenMLDBTest;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.ns.NS;
import lombok.extern.log4j.Log4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;

@Log4j
public class DropTableTest extends OpenMLDBTest {
    @Test(dataProvider = "storageModeDataByString")
    public void testGetSchema(String storageMode) throws Exception{
        String tableName = tableNamePrefix+ RandomStringUtils.randomAlphabetic(8);
        String tableDDL = "create table %s(c1 string,c2 smallint,c3 int,c4 bigint,c5 float,c6 double,c7 timestamp,c8 date,c9 bool," +
                "index(key=(c1),ts=c7))options(partitionnum=2,replicanum=3,storage_mode='%s');";
        tableDDL = String.format(tableDDL,tableName,storageMode);
        sdkClient.execute(tableDDL);
        List<NS.TableInfo> tableInfo = masterNsc.showTable(tableName);
        Assert.assertEquals(tableInfo.size(),1);
        boolean b = masterNsc.dropTable(tableName);
        Assert.assertTrue(b);
        masterClusterClient.refreshRouteTable();
        List<NS.TableInfo> deleteTableInfo = masterNsc.showTable(tableName);
        Assert.assertEquals(deleteTableInfo.size(),0);
    }
}
