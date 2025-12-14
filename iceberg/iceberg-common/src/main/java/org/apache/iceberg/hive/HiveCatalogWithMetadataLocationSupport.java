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

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.iceberg.common.ClosableHiveCatalog;
import org.apache.gravitino.iceberg.common.cache.SupportsMetadataLocation;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.thrift.TException;

public class HiveCatalogWithMetadataLocationSupport extends ClosableHiveCatalog
    implements SupportsMetadataLocation {
  private ClientPool<IMetaStoreClient, TException> metaClients;

  @Override
  public void initialize(String name, Map<String, String> properties) {
    super.initialize(name, properties);
    loadFields();
  }

  @Override
  public String metadataLocation(TableIdentifier tableIdentifier) {
    String dbName = tableIdentifier.namespace().level(0);
    String tableName = tableIdentifier.name();

    try {
      Table table = metaClients.run(client -> client.getTable(dbName, tableName));
      String tableType = table.getParameters().get(BaseMetastoreTableOperations.TABLE_TYPE_PROP);
      if (tableType == null
          || !tableType.equalsIgnoreCase(BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE)) {
        return null;
      }
      return table.getParameters().get(METADATA_LOCATION_PROP);
    } catch (Exception e) {
      return null;
    }
  }

  private void loadFields() {
    try {
      this.metaClients =
          (ClientPool<IMetaStoreClient, TException>) FieldUtils.readField(this, "clients", true);
      Preconditions.checkState(
          metaClients != null, "Failed to get clients field from hive catalog");
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
