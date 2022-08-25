package com._4paradigm.openmldb.rtidb_client.util;

import com._4paradigm.rtidb.client.KvIterator;
import com._4paradigm.rtidb.client.TabletException;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * Created by zhangguanglin on 2019/9/20.
 */
public class KvIteratorUtil {
    public static ArrayList<String> kvIteratorToList(KvIterator kvIterator){
        ArrayList<String> resultList = new ArrayList<>();
        while (kvIterator.valid()){
            Charset charset = Charset.forName("UTF-8");
            String scanValue = charset.decode(kvIterator.getValue()).toString();
            resultList.add(scanValue.trim());
            kvIterator.next();
        }
        return resultList;
    }

    public static List<Object[]> kvIteratorToListForSchema(KvIterator kvIterator) throws TabletException {
        List<Object[]> resultSet = new ArrayList<>();
        while (kvIterator.valid()) {
            Object[] row = kvIterator.getDecodedValue();
            resultSet.add(row);
            kvIterator.next();
        }
        return resultSet;
    }

    public static HashSet<Object> kvIteratorToSet(KvIterator kvIterator) throws TabletException{
        HashSet<Object> resultSet = new HashSet<>();
        while (kvIterator.valid()) {
            Object[] row = kvIterator.getDecodedValue();
            resultSet.add(new HashSet<>(Arrays.asList(row)));
            kvIterator.next();
        }
        return resultSet;
    }
}
