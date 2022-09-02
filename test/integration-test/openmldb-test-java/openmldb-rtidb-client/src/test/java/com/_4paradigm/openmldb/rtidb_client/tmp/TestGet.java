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

public class TestGet extends OpenMLDBTest {
    @Test
    public void test1() throws Exception {
        String tableName = "test_ssd_latest";
        String indexName = "INDEX_0_1662030084";

        Object[] actualGetRow = masterTableAsyncClient.get(tableName, "bb", indexName, 1590738989002L, "c7", Tablet.GetType.kSubKeyEq).getRow();
        DataUtil.convertData(actualGetRow);
        System.out.println("actualGetRow = " + Arrays.toString(actualGetRow));
    }

    @Test
    public void test2() throws Exception {
        List<NS.TableInfo> test_memory = masterNsc.showTable("test_memory");
        System.out.println("test_memory = " + test_memory);
    }
    @Test
    public void test3() throws Exception {
        String tableName = "test_ssd_latest";
        String indexName = "INDEX_0_1662030084";
        Map<String, Object> keyMap = new HashMap<>();
        keyMap.put("user_code","user_code11");
        keyMap.put("update_time","update_time11");
        Object[] actualGetRow = masterTableAsyncClient.get(tableName,keyMap,indexName,0L,null, Tablet.GetType.kSubKeyEq).getRow();
        DataUtil.convertData(actualGetRow);
        System.out.println("actualGetRow = " + Arrays.toString(actualGetRow));
    }

    @Test
    public void test4() throws Exception {
        String tableName = "test_ssd_latest";
        String indexName = "INDEX_0_1662030084";
        Map<String, Object> keyMap = new HashMap<>();
        keyMap.put("user_code","user_code11");
        keyMap.put("update_time","update_time11");
        List<Object[]> scanList = KvIteratorUtil.kvIteratorToListForSchema(masterTableSyncClient.scan(tableName,keyMap,indexName, 0L, 0L,null, 100));
        scanList.forEach(arr->System.out.println(Arrays.toString(arr)));
        System.out.println(scanList.size());
    }
}
