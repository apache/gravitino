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

import static org.apache.gravitino.lance.common.utils.LanceConstants.LANCE_TABLE_FORMAT;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestLancePropertiesUtils {

  /** Verifies the binary Lance format predicate's case and null handling. */
  @Test
  public void testIsLanceTableFormatIsCaseInsensitiveAndNullSafe() {
    Assertions.assertTrue(LancePropertiesUtils.isLanceTableFormat(LANCE_TABLE_FORMAT));
    Assertions.assertTrue(LancePropertiesUtils.isLanceTableFormat("LANCE"));
    Assertions.assertFalse(LancePropertiesUtils.isLanceTableFormat("delta"));
    Assertions.assertFalse(LancePropertiesUtils.isLanceTableFormat(null));
  }

  @Test
  public void testGetLanceStorageOptions() {
    Map<String, String> properties =
        ImmutableMap.of(
            "lance.storage.endpoint", "http://minio:9000",
            "lance.storage.access_key_id", "ak",
            "lance.storage.s3.custom_option", "custom-value",
            "not.storage.key", "ignored");

    Map<String, String> storageOptions = LancePropertiesUtils.getLanceStorageOptions(properties);

    Assertions.assertEquals(3, storageOptions.size());
    Assertions.assertEquals("http://minio:9000", storageOptions.get("endpoint"));
    Assertions.assertEquals("ak", storageOptions.get("access_key_id"));
    Assertions.assertEquals("custom-value", storageOptions.get("s3.custom_option"));
    Assertions.assertFalse(storageOptions.containsKey("not.storage.key"));
    Assertions.assertEquals(
        List.of("endpoint", "access_key_id", "s3.custom_option"),
        new ArrayList<>(storageOptions.keySet()));
  }

  /** Verifies that the storage prefix itself cannot become an empty provider option key. */
  @Test
  public void testGetLanceStorageOptionsRejectsEmptyOptionKey() {
    String propertyValue = "secret-value-must-not-leak";
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                LancePropertiesUtils.getLanceStorageOptions(
                    Map.of("lance.storage.", propertyValue)));

    Assertions.assertTrue(exception.getMessage().contains("lance.storage."));
    Assertions.assertFalse(exception.getMessage().contains(propertyValue));
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
    Assertions.assertEquals(
        List.of("endpoint", "region", "access_key_id"), new ArrayList<>(storageOptions.keySet()));
  }
}
