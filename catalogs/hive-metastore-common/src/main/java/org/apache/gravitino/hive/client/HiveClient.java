/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.hive.client;

import java.util.List;
import org.apache.gravitino.Schema;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.partitions.Partition;

/**
 * An externally visible interface to the Hive client. This interface is shared across both the
 * internal and external classloaders for a given version of Hive and thus must expose only shared
 * classes.
 */
public interface HiveClient {

  default void createDatabase(Schema schema) {}
  ;

  default Schema getDatabase(String name) {
    return null;
  }
  ;

  List<String> getAllDatabases();

  default void alterDatabase(String name, Schema database) {}
  ;

  default void dropDatabase(String name, boolean b, boolean b1, boolean cascade) {}
  ;

  default List<String> getAllTables(String name) {
    return null;
  }
  ;

  default List<String> listTableNamesByFilter(
      String name, String icebergAndPaimonFilter, short maxTables) {
    return null;
  }
  ;

  default Table getTable(String name, String name1) {
    return null;
  }
  ;

  default void alterTable(String name, String name1, Table alteredHiveTable) {}
  ;

  default void dropTable(
      String name, String name1, boolean deleteData, boolean b, boolean ifPurge) {}
  ;

  default void createTable(Table hiveTable) {}
  ;

  default List<String> listPartitionNames(String s, String name, short i) {
    return null;
  }
  ;

  default List<Partition> listPartitions(String s, String name, short i) {
    return null;
  }
  ;

  default List<Partition> listPartitions(
      String s, String name, List<String> filterPartitionValueList, short i) {
    return null;
  }
  ;

  default Partition getPartition(String s, String name, String partitionName) {
    return null;
  }
  ;

  default Partition addPartition(Partition partition) {
    return null;
  }
  ;

  default void dropPartition(String dbName, String tableName, String partitionName, boolean b) {}
  ;

  default String getDelegationToken(String finalPrincipalName, String userName) {
    return null;
  }

  default List<Table> getTableObjectsByName(String name, List<String> allTables) {
    return null;
  }
  ;

  public enum HiveVersion {
    HIVE2,
    HIVE3,
  }
}
