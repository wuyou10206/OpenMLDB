package com._4paradigm.openmldb.rtidb_client.util;

import com._4paradigm.rtidb.common.Common;
import com._4paradigm.rtidb.ns.NS;
import org.testng.Assert;

import java.util.List;

public class AssertUtil {
    public static void assertList(List<Object[]> actualList,List<Object[]> expectList){
        Assert.assertEquals(actualList.size(),expectList.size());
        for(int i=0;i<actualList.size();i++){
            Assert.assertEquals(actualList.get(i),expectList.get(i));
        }
    }
    public static void assertColumnDescV1List(List<Common.ColumnDesc> actualColumnDescV1List, List<Common.ColumnDesc> expectColumnDescV1List){
        Assert.assertEquals(actualColumnDescV1List.size(),expectColumnDescV1List.size());
        for(int i=0;i<actualColumnDescV1List.size();i++){
            Common.ColumnDesc actualColumnDesc = actualColumnDescV1List.get(i);
            Common.ColumnDesc expectColumnDesc = expectColumnDescV1List.get(i);
            Assert.assertEquals(actualColumnDesc.getName(),expectColumnDesc.getName());
            Assert.assertEquals(actualColumnDesc.getType(),expectColumnDesc.getType());
        }
    }
    public static void assertColumnDescList(List<NS.ColumnDesc> actualColumnDescList, List<NS.ColumnDesc> expectColumnDescList){
        Assert.assertEquals(actualColumnDescList.size(),expectColumnDescList.size());
        for(int i=0;i<actualColumnDescList.size();i++){
            NS.ColumnDesc actualColumnDesc = actualColumnDescList.get(i);
            NS.ColumnDesc expectColumnDesc = expectColumnDescList.get(i);
            Assert.assertEquals(actualColumnDesc.getName(),expectColumnDesc.getName());
            Assert.assertEquals(actualColumnDesc.getType(),expectColumnDesc.getType());
        }
    }
    public static void assertColumnKeyList(List<Common.ColumnKey> actualColumnKeyList, List<Common.ColumnKey> expectColumnKeyList){
        Assert.assertEquals(actualColumnKeyList.size(),expectColumnKeyList.size());
        for(int i=0;i<actualColumnKeyList.size();i++){
            Common.ColumnKey actualColumnKey = actualColumnKeyList.get(i);
            Common.ColumnKey expectColumnKey = expectColumnKeyList.get(i);
            Assert.assertEquals(actualColumnKey.getIndexName(),expectColumnKey.getIndexName());
            Assert.assertEquals(actualColumnKey.getColNameList().toArray(),expectColumnKey.getColNameList().toArray());
            Assert.assertEquals(actualColumnKey.getTsNameList().toArray(),expectColumnKey.getTsNameList().toArray());
        }
    }
}
