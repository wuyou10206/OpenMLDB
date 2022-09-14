package com._4paradigm.openmldb.rtidb_client.common;

import com._4paradigm.rtidb.common.Common;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.DataProvider;

import java.lang.reflect.Method;

/**
 * @author zhaowei
 * @date 2020/3/23 下午1:02
 */
@Slf4j
public class RtidbDataProvider {
    @DataProvider()
    public Object[][] formatVersion(Method method) {
        return new Object[][]{
                new Object[]{0},
                new Object[]{1}
        };
    }
    @DataProvider(name="storageModeDataByString")
    public Object[][] storageModeDataByString(Method method) {
        return new Object[][]{
                new Object[]{"memory"},
                new Object[]{"ssd"},
                new Object[]{"hdd"}
        };
    }
    @DataProvider(name="ssdAndHdd")
    public Object[][] ssdAndHdd(Method method) {
        return new Object[][]{
                new Object[]{"ssd"},
                new Object[]{"hdd"}
        };
    }
    @DataProvider(name="storageModeData")
    public Object[][] storageModeData(Method method) {
        return new Object[][]{
                new Object[]{Common.StorageMode.kMemory},
                new Object[]{Common.StorageMode.kSSD},
                new Object[]{Common.StorageMode.kHDD}
        };
    }

    @DataProvider()
    public Object[][] storageModeDataNoMemory(Method method) {
        return new Object[][]{
                new Object[]{Common.StorageMode.kSSD},
                new Object[]{Common.StorageMode.kHDD}
        };
    }

}
