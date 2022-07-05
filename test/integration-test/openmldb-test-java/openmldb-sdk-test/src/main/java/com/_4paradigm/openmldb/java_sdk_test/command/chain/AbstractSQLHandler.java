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
package com._4paradigm.openmldb.java_sdk_test.command.chain;


import com._4paradigm.openmldb.java_sdk_test.entity.FesqlResult;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import lombok.Setter;

@Setter
public abstract class AbstractSQLHandler {
    private AbstractSQLHandler nextHandler;

    public abstract boolean preHandle(String sql);

    public abstract FesqlResult onHandle(OpenMLDBInfo openMLDBInfo, String dbName, String sql);

    public FesqlResult doHandle(OpenMLDBInfo openMLDBInfo, String dbName,String sql){
        if(preHandle(sql)){
            return onHandle(openMLDBInfo,dbName,sql);
        }
        if(nextHandler!=null){
            return nextHandler.doHandle(openMLDBInfo,dbName,sql);
        }
        throw new RuntimeException("no next chain");
    }
}
