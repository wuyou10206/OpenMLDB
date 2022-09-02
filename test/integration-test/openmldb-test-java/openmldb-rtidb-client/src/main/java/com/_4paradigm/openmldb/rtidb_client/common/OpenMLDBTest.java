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

package com._4paradigm.openmldb.rtidb_client.common;


import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBClient;
import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBGlobalVar;
import com._4paradigm.openmldb.test_common.openmldb.SDKClient;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBDeployType;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import com._4paradigm.qa.openmldb_deploy.common.OpenMLDBDeploy;
import com._4paradigm.rtidb.client.TableAsyncClient;
import com._4paradigm.rtidb.client.TableSyncClient;
import com._4paradigm.rtidb.client.ha.impl.NameServerClientImpl;
import com._4paradigm.rtidb.client.ha.impl.RTIDBClusterClient;
import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.*;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.Map;

/**
 * @author zhaowei
 * @date 2020/6/11 2:02 PM
 */
@Slf4j
public class OpenMLDBTest extends RtidbDataProvider{
    protected ArrayList<String> tableNameList = new ArrayList<>();

    protected NameServerClientImpl masterNsc;
    protected TableSyncClient masterTableSyncClient;
    protected TableAsyncClient masterTableAsyncClient;
    protected RTIDBClusterClient masterClusterClient;
    protected SDKClient sdkClient;

    @Getter
    protected String tableNamePrefix = "auto_";
    protected static SqlExecutor executor;

    @BeforeTest()
    @Parameters({"env","version","openMLDBPath"})
    public void beforeTest(@Optional("qa") String env,@Optional("main") String version,@Optional("")String openMLDBPath) throws Exception {
        OpenMLDBGlobalVar.env = env;
        if(env.equalsIgnoreCase("cluster")){
            OpenMLDBDeploy openMLDBDeploy = new OpenMLDBDeploy(version);;
            openMLDBDeploy.setOpenMLDBPath(openMLDBPath);
            openMLDBDeploy.setCluster(true);
            OpenMLDBGlobalVar.mainInfo = openMLDBDeploy.deployCluster(2, 3);
        }else if(env.equalsIgnoreCase("standalone")){
            OpenMLDBDeploy openMLDBDeploy = new OpenMLDBDeploy(version);
            openMLDBDeploy.setOpenMLDBPath(openMLDBPath);
            openMLDBDeploy.setCluster(false);
            OpenMLDBGlobalVar.mainInfo = openMLDBDeploy.deployCluster(2, 3);
        }else{
            OpenMLDBGlobalVar.mainInfo = OpenMLDBInfo.builder()
                    .deployType(OpenMLDBDeployType.CLUSTER)
                    .basePath("/home/zhaowei01/openmldb-auto-test/tmp")
                    .openMLDBPath("/home/zhaowei01/openmldb-auto-test/tmp/openmldb-ns-1/bin/openmldb")
                    .zk_cluster("172.24.4.55:30008")
                    .zk_root_path("/openmldb")
                    .nsNum(2).tabletNum(3)
                    .nsEndpoints(Lists.newArrayList("172.24.4.55:30004", "172.24.4.55:30005"))
                    .tabletEndpoints(Lists.newArrayList("172.24.4.55:30001", "172.24.4.55:30002", "172.24.4.55:30003"))
                    .apiServerEndpoints(Lists.newArrayList("172.24.4.55:30006"))
                    .build();
            OpenMLDBGlobalVar.env = "cluster";

        }
        String caseEnv = System.getProperty("caseEnv");
        if (!StringUtils.isEmpty(caseEnv)) {
            OpenMLDBGlobalVar.env = caseEnv;
        }
        log.info("openMLDB global var env: {}", env);
        OpenMLDBClient openMLDBClient = new OpenMLDBClient(OpenMLDBGlobalVar.mainInfo.getZk_cluster(), OpenMLDBGlobalVar.mainInfo.getZk_root_path());
        executor = openMLDBClient.getExecutor();
        log.info("executor:{}",executor);
        sdkClient = SDKClient.of(executor);
        sdkClient.setOnline();
        sdkClient.createAndUseDB("default_db");
        RTIDBClient masterRtidbClient = new RTIDBClient(OpenMLDBGlobalVar.mainInfo.getZk_cluster(),OpenMLDBGlobalVar.mainInfo.getZk_root_path());
        masterTableSyncClient = masterRtidbClient.getTableSyncClient();
        masterNsc = masterRtidbClient.getNsc();
        masterClusterClient= masterRtidbClient.getClusterClient();
        masterTableAsyncClient = masterRtidbClient.getTableAsyncClient();

    }

    @AfterClass
    public void ARemoveTable() {

    }
}
