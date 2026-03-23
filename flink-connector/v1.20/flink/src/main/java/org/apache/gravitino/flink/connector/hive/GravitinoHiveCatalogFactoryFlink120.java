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

package org.apache.gravitino.flink.connector.hive;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.flink.table.catalog.Catalog;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.apache.hadoop.hive.conf.HiveConf;

public class GravitinoHiveCatalogFactoryFlink120 extends GravitinoHiveCatalogFactory {

  @Override
  protected Catalog newCatalog(
      String catalogName,
      String defaultDatabase,
      Map<String, String> catalogOptions,
      SchemaAndTablePropertiesConverter schemaAndTablePropertiesConverter,
      PartitionConverter partitionConverter,
      @Nullable HiveConf hiveConf,
      @Nullable String hiveVersion) {
    return new GravitinoHiveCatalogFlink120(
        catalogName,
        defaultDatabase,
        catalogOptions,
        schemaAndTablePropertiesConverter,
        partitionConverter,
        hiveConf,
        hiveVersion);
  }
}
