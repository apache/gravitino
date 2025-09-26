/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.iceberg.hive;

import java.lang.reflect.Field;
import java.util.Map;
import org.apache.gravitino.iceberg.common.ClosableHiveCatalog;
import org.apache.gravitino.iceberg.common.cache.SupportsMetadataLocation;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.thrift.TException;

public class HiveCatalogWithMetadataLocation extends ClosableHiveCatalog
    implements SupportsMetadataLocation {
  private ClientPool<IMetaStoreClient, TException> metaClients;
  private String catalogName;

  @Override
  public void initialize(String name, Map<String, String> properties) {
    super.initialize(name, properties);
    loadFields();
  }

  @Override
  public String metadataLocation(TableIdentifier tableIdentifier) {
    String dbName = tableIdentifier.namespace().level(0);
    String tableName = tableIdentifier.name();
    String fullName = catalogName + "." + dbName + "." + tableName;

    try {
      Table table = metaClients.run(client -> client.getTable(dbName, tableName));
      HiveOperationsBase.validateTableIsIceberg(table, fullName);
      return table.getParameters().get(METADATA_LOCATION_PROP);
    } catch (Exception e) {
      return null;
    }
  }

  private void loadFields() {
    try {
      Class<?> baseClass = getClass().getSuperclass();
      Field catalogNameField = baseClass.getDeclaredField("name");
      catalogNameField.setAccessible(true);
      this.catalogName = (String) catalogNameField.get(this);

      Field clientsField = baseClass.getDeclaredField("metaClients");
      clientsField.setAccessible(true);
      this.metaClients = (ClientPool<IMetaStoreClient, TException>) clientsField.get(this);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
