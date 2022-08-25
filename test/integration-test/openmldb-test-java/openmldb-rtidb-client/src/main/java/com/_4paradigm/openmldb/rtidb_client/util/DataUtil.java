package com._4paradigm.openmldb.rtidb_client.util;

import org.joda.time.DateTime;

import java.sql.Timestamp;
import java.util.List;

public class DataUtil {
    public static void convertData(Object[] dataArray){
        for(int i=0;i<dataArray.length;i++){
            Object data = dataArray[i];
            if(data instanceof DateTime){
                DateTime dateTime = (DateTime) data;
                Timestamp timestamp = new Timestamp(dateTime.getMillis());
                dataArray[i] = timestamp;
            }
        }
    }
    public static void convertData(List<Object[]> dataList){
        for(Object[] dataArray:dataList) {
            for (int i = 0; i < dataArray.length; i++) {
                Object data = dataArray[i];
                if (data instanceof DateTime) {
                    DateTime dateTime = (DateTime) data;
                    Timestamp timestamp = new Timestamp(dateTime.getMillis());
                    dataArray[i] = timestamp;
                }
            }
        }
    }
}
