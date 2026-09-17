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
package org.apache.gravitino.maintenance.jobs.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TestIcebergJobUtils {

  @Test
  void applyIcebergRestAuthSetsSparkCatalogConfigs() {
    Map<String, String> properties = new HashMap<>();
    properties.put(OptimizerConfig.AUTH_TYPE, "basic");
    properties.put(OptimizerConfig.AUTH_USERNAME, "admin");
    properties.put(OptimizerConfig.AUTH_PASSWORD, "secret");

    SparkSession.Builder sparkBuilder = mock(SparkSession.Builder.class);
    IcebergJobUtils.applyIcebergRestAuth(
        sparkBuilder, "rest_catalog", new OptimizerConfig(properties));

    ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
    verify(sparkBuilder, org.mockito.Mockito.times(3))
        .config(keyCaptor.capture(), valueCaptor.capture());

    Map<String, String> applied = new HashMap<>();
    for (int i = 0; i < keyCaptor.getAllValues().size(); i++) {
      applied.put(keyCaptor.getAllValues().get(i), valueCaptor.getAllValues().get(i));
    }
    assertEquals("basic", applied.get("spark.sql.catalog.rest_catalog.rest.auth.type"));
    assertEquals("admin", applied.get("spark.sql.catalog.rest_catalog.rest.auth.basic.username"));
    assertEquals("secret", applied.get("spark.sql.catalog.rest_catalog.rest.auth.basic.password"));
  }
}
