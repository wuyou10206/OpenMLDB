package com._4paradigm.openmldb.rtidb_client.util;

import com._4paradigm.rtidb.common.Common.ColumnDesc;

import java.util.List;
import java.util.stream.Collectors;

public class ResultConvert {
    public static List<ColumnDesc> convertGetSchemaRes(List<com._4paradigm.rtidb.client.schema.ColumnDesc> columnDescList){
        return columnDescList.stream()
                .map(columnDesc->{
                    ColumnDesc.Builder builder = ColumnDesc.newBuilder();
                    builder.setName(columnDesc.getName())
                            //返回的ColumnDesc的type需要特殊处理
                            .setType(columnDesc.getType().name().substring(1).toLowerCase()).setAddTsIdx(columnDesc.isAddTsIndex());
                    //add_ts_idx,is_ts_col两个字段只能二选一，需要特殊处理
                    if (columnDesc.isTsCol()){
                        builder.setIsTsCol(true);
                    }else {
                        builder.setAddTsIdx(columnDesc.isAddTsIndex());
                    }
                    return builder.build();
//                    return ColumnDesc.newBuilder().setName(columnDesc.getName()).setAddTsIdx(columnDesc.isAddTsIndex()).setIsTsCol(columnDesc.isTsCol()).setType(columnDesc.getType().name().substring(1).toLowerCase()).build();
                }).collect(Collectors.toList());
    }
}
