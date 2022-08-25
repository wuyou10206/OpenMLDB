package com._4paradigm.openmldb.rtidb_client.common;

import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.DataProvider;

import java.lang.reflect.Method;

/**
 * @author zhaowei
 * @date 2020/3/23 下午1:02
 */
@Slf4j
public class RtidbDataProvider {

    @DataProvider(name="storageModeData")
    public Object[][] storageModeData(Method method) {
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
}
