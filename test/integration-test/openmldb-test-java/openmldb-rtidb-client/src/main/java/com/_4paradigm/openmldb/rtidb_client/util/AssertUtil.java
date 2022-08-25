package com._4paradigm.openmldb.rtidb_client.util;

import org.testng.Assert;

import java.util.List;

public class AssertUtil {
    public static void assertList(List<Object[]> actualList,List<Object[]> expectList){
        Assert.assertEquals(actualList.size(),expectList.size());
        for(int i=0;i<actualList.size();i++){
            Assert.assertEquals(actualList.get(i),expectList.get(i));
        }
    }
}
