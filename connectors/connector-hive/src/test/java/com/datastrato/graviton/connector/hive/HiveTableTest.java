/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datastrato.graviton.connector.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.thrift.TException;
//import org.junit.Assert;
//import org.junit.BeforeClass;
import org.junit.jupiter.api.Test;
//import java.util.*;
import org.junit.jupiter.api.BeforeAll;

public class HiveTableTest extends HiveTableBaseTest {
//  @BeforeClass
//  public static void startMetastore() throws Exception {
//    int i =0 ;
//  }

//  @BeforeAll
//  public static void setUp() {
//    // 在所有测试用例之前执行的初始化操作
//    int i = 0;
//  }

  @Test
  public void testCreate() throws TException {
    // Table should be created in hive metastore
    // Table should be renamed in hive metastore
    String tableName = "tbl"; //TABLE_IDENTIFIER.name();
//    org.apache.hadoop.hive.metastore.api.Table table =
//        metastoreClient.getTable("hivedb"/*TABLE_IDENTIFIER.namespace().level(0)*/, tableName);

    Database database = metastoreClient.getDatabase("hivedb");
  }

  @Test
  public void test1() throws Exception {
    HiveMetaOperation hiveOps = new HiveMetaOperation(this.metastore.hiveConf());

    System.out.println("----------------------------获取所有catalogs-------------------------------------");
    hiveOps.getAllDatabases(null).forEach(System.out::println);
  }

}
