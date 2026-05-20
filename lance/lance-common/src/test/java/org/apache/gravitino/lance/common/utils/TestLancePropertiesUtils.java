/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.lance.common.utils;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestLancePropertiesUtils {

  @Test
  public void testGetLanceStorageOptions() {
    Map<String, String> properties =
        ImmutableMap.of(
            "lance.storage.endpoint", "http://minio:9000",
            "lance.storage.access_key_id", "ak",
            "not.storage.key", "ignored");

    Map<String, String> storageOptions = LancePropertiesUtils.getLanceStorageOptions(properties);

    Assertions.assertEquals(2, storageOptions.size());
    Assertions.assertEquals("http://minio:9000", storageOptions.get("endpoint"));
    Assertions.assertEquals("ak", storageOptions.get("access_key_id"));
    Assertions.assertFalse(storageOptions.containsKey("not.storage.key"));
  }

  @Test
  public void testResolveLanceStorageOptionsPrefersTableProperties() {
    Map<String, String> catalogProperties =
        ImmutableMap.of(
            "lance.storage.endpoint", "http://catalog:9000",
            "lance.storage.region", "us-east-1");
    Map<String, String> tableProperties =
        ImmutableMap.of(
            "lance.storage.endpoint", "http://table:9000",
            "lance.storage.access_key_id", "table-ak");

    Map<String, String> storageOptions =
        LancePropertiesUtils.resolveLanceStorageOptions(catalogProperties, tableProperties);

    Assertions.assertEquals(3, storageOptions.size());
    Assertions.assertEquals("http://table:9000", storageOptions.get("endpoint"));
    Assertions.assertEquals("us-east-1", storageOptions.get("region"));
    Assertions.assertEquals("table-ak", storageOptions.get("access_key_id"));
  }
}
