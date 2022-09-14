package com._4paradigm.openmldb.rtidb_client.util;

import com._4paradigm.openmldb.test_common.util.Tool;
import com._4paradigm.openmldb.test_common.util.WaitUtil;
import com._4paradigm.rtidb.client.TableSyncClient;
import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.ha.impl.NameServerClientImpl;
import com._4paradigm.rtidb.client.ha.impl.RTIDBClusterClient;

import com._4paradigm.rtidb.client.impl.TabletSyncClientImpl;
import com._4paradigm.rtidb.common.Common;
import com._4paradigm.rtidb.ns.NS;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;

import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@Slf4j
public class RtidbUtil {
    public static boolean tableIsExist(NameServerClientImpl nsc,String tableName){
        List<NS.TableInfo> infos = nsc.showTable(tableName);
        return infos.size()==1;
    }
    public static synchronized boolean createTable(NameServerClientImpl nsc, RTIDBClusterClient client, NS.TableInfo tableInfo){
        String tableName = tableInfo.getName();
        boolean ok = false;
        try {
            ok = nsc.createTable(tableInfo);
        }catch (Exception e){
            if(e instanceof java.util.concurrent.TimeoutException){
                log.info("表："+tableName+" 创建超时...");
                WaitUtil.waitCondition(()->tableIsExist(nsc,tableName));
            }else {
                e.printStackTrace();
            }
        }
        client.refreshRouteTable();
        log.info("表：\n{},创建:{}",tableInfo,ok);
        return ok;
    }
    public static void putSchemaLessThanSchema(TableSyncClient tabletSyncClient, String tableName, Object[] row, Boolean isSynClient){
        if (isSynClient){
            try {
                tabletSyncClient.put(tableName,row);
            } catch (TimeoutException e) {
                e.printStackTrace();
                throw new IllegalStateException();
            } catch (TabletException e) {
                e.printStackTrace();
                Assert.assertEquals(e.getMessage(),"row length mismatch schema");
            }
        }else {
            throw new IllegalArgumentException();
        }
    }
    public static boolean checkOPStatus(NameServerClientImpl nsc,String name){
        return checkOPStatus(nsc,name,2000,60);
    }
    public static boolean checkOPStatus(NameServerClientImpl nsc,String name,int time,int count){
        int num = 0;
        do{
            List<NS.OPStatus> ops = nsc.showOPStatus(name);
            boolean flag = checkOPStatusByDone(ops);
            if(flag){
                return true;
            }else {
                if(checkOPStatusByFailed(ops)){
                    return false;
                }
                num++;
                Tool.sleep(time);
            }
        }while (num<count);
        return false;
    }
    public static boolean checkOPStatusByFailed(List<NS.OPStatus> ops){
        for(NS.OPStatus op:ops){
            String status = op.getStatus();
            String type = op.getOpType();
            if(type.equals("kAddReplicaSimplyRemoteOP")) continue;
            if(status.equals("kFailed")){
                return true;
            }
        }
        return false;
    }

    public static boolean checkOPStatusByDone(List<NS.OPStatus> ops){
        if(ops==null || ops.size()==0) return false;
        for(NS.OPStatus op:ops){
//            System.out.println("op:" +op.getOpId()+":"+op.getOpType()+":"+op.getStatus());
            String status = op.getStatus();
            String type = op.getOpType();
            if(type.equals("kAddReplicaSimplyRemoteOP")) continue;
            if(!status.equals("kDone")){
                return false;
            }
        }
        return true;
    }
    public static List<String> getIndexName(NameServerClientImpl nsc,String tableName){
        NS.TableInfo tableInfo = nsc.showTable(tableName).get(0);
        List<Common.ColumnKey> columnKeyList = tableInfo.getColumnKeyList();
        List<String> list = tableInfo.getColumnKeyList().stream().map(ck -> ck.getIndexName()).collect(Collectors.toList());
        return list;
    }
}
