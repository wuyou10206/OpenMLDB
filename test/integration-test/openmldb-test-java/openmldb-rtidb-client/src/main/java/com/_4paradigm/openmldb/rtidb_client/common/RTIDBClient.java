package com._4paradigm.openmldb.rtidb_client.common;

import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import com._4paradigm.rtidb.client.TableAsyncClient;
import com._4paradigm.rtidb.client.TableSyncClient;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.client.ha.impl.NameServerClientImpl;
import com._4paradigm.rtidb.client.ha.impl.RTIDBClusterClient;
import com._4paradigm.rtidb.client.impl.TableAsyncClientImpl;
import com._4paradigm.rtidb.client.impl.TableSyncClientImpl;
import lombok.Getter;

import java.util.Map;

/**
 * Created by zhangguanglin on 2019/12/18.
 */
@Getter
public class RTIDBClient {
    private RTIDBClusterClient clusterClient;
    private TableSyncClient tableSyncClient;
    private TableAsyncClient tableAsyncClient;
    private NameServerClientImpl nsc;

    public RTIDBClient(String zkEndpoints, String zkRootPath) throws Exception {
        this(zkEndpoints,zkRootPath,TableHandler.ReadStrategy.kReadLeader);
    }

    public RTIDBClient(String zkEndpoints, String zkRootPath, TableHandler.ReadStrategy readStrategy) throws Exception {
        RTIDBClientConfig config = new RTIDBClientConfig();
        config.setZkEndpoints(zkEndpoints);
        config.setZkRootPath(zkRootPath);
        config.setReadTimeout(1000 * 1000);
        config.setWriteTimeout(1000 * 1000);
        config.setZkSesstionTimeout(60*1000);
        config.setGlobalReadStrategies(readStrategy);
        config.setHandleNull(true);
        clusterClient = new RTIDBClusterClient(config);
        nsc = new NameServerClientImpl(config);
        nsc.init();
        clusterClient.init();
        tableSyncClient = new TableSyncClientImpl(clusterClient);
        tableAsyncClient = new TableAsyncClientImpl(clusterClient);
    }

    public RTIDBClient(Map<String, String> clusterInfo) throws Exception {
        this(clusterInfo.get("zk_end_point"), clusterInfo.get("zk_root_path"));
    }
    public RTIDBClient(Map<String, String> clusterInfo, TableHandler.ReadStrategy readStrategy) throws Exception {
        this(clusterInfo.get("zk_end_point"), clusterInfo.get("zk_root_path"),readStrategy);
    }
}
