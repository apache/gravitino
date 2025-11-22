/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gravitino.hive.client;

import java.lang.reflect.Method;
import java.util.List;
import org.apache.gravitino.Schema;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.hive.converter.HiveDatabaseConverter;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;

class HiveShimV2 extends Shim {

  private IMetaStoreClient client;
  Method getAllDatabasesMethod;
  Method createDatabaseMethod;
  Method getDabaseMethod;

  HiveShimV2(IMetaStoreClient client) {
    try {
      this.client = client;

      this.getAllDatabasesMethod = IMetaStoreClient.class.getMethod("getAllDatabases");
      this.createDatabaseMethod =
          IMetaStoreClient.class.getMethod(
              "createDatabase", org.apache.hadoop.hive.metastore.api.Database.class);
      this.getDabaseMethod = IMetaStoreClient.class.getMethod("getDatabase", String.class);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Failed to initialize HiveShimV2", e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<String> getAllDatabase() {
    try {
      return (List<String>) getAllDatabasesMethod.invoke(client);
    } catch (Exception e) {
      throw new RuntimeException("Failed to get all databases using HiveShimV2", e);
    }
  }

  public void createDatabase(String catalogName, Schema database) {
    try {
      Database db = new Database();
      db.setName(database.name());
      db.setDescription(database.comment());
      createDatabaseMethod.invoke(client, db);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create database using HiveShimV2", e);
    }
  }

  @Override
  public Schema getDatabase(String catalogName, String dbName) {
    try {
      Database db = (Database) getDabaseMethod.invoke(client, dbName);
      if (db == null) {
        throw new NoSuchSchemaException("Database %s does not exist", dbName);
      }
      return HiveDatabaseConverter.fromHiveDB(db);
    } catch (Exception e) {
      throw new RuntimeException("Failed to get database using HiveShimV2", e);
    }
  }
}
