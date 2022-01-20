/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.openmldb.java_sdk_test.cluster.v230;

import com._4paradigm.openmldb.java_sdk_test.common.FedbTest;
import com._4paradigm.openmldb.java_sdk_test.executor.ExecutorFactory;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.model.SQLCaseType;
import com._4paradigm.openmldb.test_common.provider.Yaml;
import io.qameta.allure.Feature;
import io.qameta.allure.Story;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

/**
 * @author zhaowei
 * @date 2020/6/11 2:53 PM
 */
@Slf4j
@Feature("Lastjoin")
public class LastJoinTest extends FedbTest {

    @Story("batch")
    @Test(dataProvider = "getCase")
    @Yaml(filePaths = {"function/join/","function/cluster/window_and_lastjoin.yaml"})
    public void testLastJoin(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor,testCase, SQLCaseType.kBatch).run();
    }
    @Story("request")
    @Test(dataProvider = "getCase")
    @Yaml(filePaths = {"function/join/","function/cluster/window_and_lastjoin.yaml"})
    public void testLastJoinRequestMode(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor,testCase, SQLCaseType.kRequest).run();
    }
    @Story("requestWithSp")
    @Test(dataProvider = "getCase")
    @Yaml(filePaths = {"function/join/","function/cluster/window_and_lastjoin.yaml"})
    public void testLastJoinRequestModeWithSp(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor,testCase, SQLCaseType.kRequestWithSp).run();
    }
    @Story("requestWithSpAysn")
    @Test(dataProvider = "getCase")
    @Yaml(filePaths = {"function/join/","function/cluster/window_and_lastjoin.yaml"})
    public void testLastJoinRequestModeWithSpAsync(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor,testCase, SQLCaseType.kRequestWithSpAsync).run();
    }
}
