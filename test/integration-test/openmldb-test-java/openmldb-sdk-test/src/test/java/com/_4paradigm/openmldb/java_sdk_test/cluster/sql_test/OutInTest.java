package com._4paradigm.openmldb.java_sdk_test.cluster.sql_test;

import com._4paradigm.openmldb.java_sdk_test.common.OpenMLDBTest;
import com._4paradigm.openmldb.java_sdk_test.executor.ExecutorFactory;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.model.SQLCaseType;
import com._4paradigm.openmldb.test_common.provider.Yaml;
import io.qameta.allure.Feature;
import io.qameta.allure.Story;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
@Feature("Out-In")
public class OutInTest extends OpenMLDBTest {

    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "integration_test/out_in/test_select_into_load_data.yaml")
    @Story("LOAD DATA")
    public void testOutInByOffline(SQLCase testCase){
        System.out.println("testCase = " + testCase);
        ExecutorFactory.build(executor, testCase, SQLCaseType.kJob).run();
    }


     @Test(dataProvider = "getCase")
     @Yaml(filePaths = "function/out_in/test_out_in.yaml")
     @Story("online")
     public void testOutInByOnline(SQLCase testCase){
         ExecutorFactory.build(executor,testCase, SQLCaseType.kBatch).run();
     }

}