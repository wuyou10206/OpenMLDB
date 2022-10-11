package com._4paradigm.openmldb.rtidb_client.tmp;

import com._4paradigm.openmldb.rtidb_client.common.OpenMLDBTest;
import com._4paradigm.openmldb.rtidb_client.util.DataUtil;
import com._4paradigm.openmldb.rtidb_client.util.KvIteratorUtil;
import com._4paradigm.rtidb.ns.NS;
import com._4paradigm.rtidb.tablet.Tablet;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestPut extends OpenMLDBTest {
    @Test
    public void test1() throws Exception {
        String tableName = "auto_WtdBFCdN";
        Object[] row = new Object[]{"card1", 1, 1.0, System.currentTimeMillis()};
        Assert.assertTrue(masterTableSyncClient.put(tableName, row));
    }

}
