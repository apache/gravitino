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

package org.apache.gravitino.flink.connector.integration.test.hive;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FlinkHiveCatalogIT120 extends FlinkHiveCatalogIT {

  @Override
  @Test
  public void testCreateGravitinoHiveCatalogRequireOptions() {
    tableEnv.useCatalog(DEFAULT_CATALOG);

    String catalogName = "gravitino_hive_sql2";
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            tableEnv.executeSql(
                String.format(
                    "create catalog %s with ("
                        + "'type'='gravitino-hive', "
                        + "'hive-conf-dir'='%s'"
                        + ")",
                    catalogName, getSharedHiveConfDir())));

    Assertions.assertFalse(metalake.catalogExists(catalogName));
  }
}
