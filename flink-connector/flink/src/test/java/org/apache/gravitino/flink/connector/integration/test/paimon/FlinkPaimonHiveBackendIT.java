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
package org.apache.gravitino.flink.connector.integration.test.paimon;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonConstants;
import org.junit.jupiter.api.Tag;

@Tag("gravitino-docker-test")
public class FlinkPaimonHiveBackendIT extends FlinkPaimonCatalogIT {

  private static final String DEFAULT_PAIMON_CATALOG = "test_flink_paimon_hive_catalog";

  @Override
  protected void createGravitinoCatalogByFlinkSql(String catalogName) {
    tableEnv.executeSql(
        String.format(
            "create catalog %s with ("
                + "'type'='gravitino-paimon', "
                + "'warehouse'='%s',"
                + "'metastore'='hive',"
                + "'uri'='%s'"
                + ")",
            catalogName, warehouse, hiveMetastoreUri));
  }

  @Override
  protected String getPaimonCatalogName() {
    return DEFAULT_PAIMON_CATALOG;
  }

  @Override
  protected Map<String, String> getPaimonCatalogOptions() {
    return ImmutableMap.of(
        PaimonConstants.CATALOG_BACKEND, "hive", "warehouse", warehouse, "uri", hiveMetastoreUri);
  }

  @Override
  protected String getWarehouse() {
    return warehouse;
  }
}
