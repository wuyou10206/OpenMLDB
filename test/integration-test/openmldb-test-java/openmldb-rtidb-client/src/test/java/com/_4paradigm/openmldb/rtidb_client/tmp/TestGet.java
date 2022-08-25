package com._4paradigm.openmldb.rtidb_client.tmp;

import com._4paradigm.openmldb.rtidb_client.common.OpenMLDBTest;
import com._4paradigm.openmldb.rtidb_client.util.DataUtil;
import com._4paradigm.rtidb.tablet.Tablet;
import org.testng.annotations.Test;

import java.util.Arrays;

public class TestGet extends OpenMLDBTest {
    @Test
    public void test1() throws Exception {
        String tableName = "auto_rqEwxCfE";
        String indexName = "INDEX_0_1661335979";

        Object[] actualGetRow = masterTableAsyncClient.get(tableName, "bb", indexName, 1590738989002L, "c7", Tablet.GetType.kSubKeyEq).getRow();
        DataUtil.convertData(actualGetRow);
        System.out.println("actualGetRow = " + Arrays.toString(actualGetRow));
    }
}
