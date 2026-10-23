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
package org.apache.gravitino.catalog.hive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.gravitino.connector.capability.CapabilityResult;
import org.apache.gravitino.hive.client.HiveClientClassLoader.HiveVersion;
import org.junit.jupiter.api.Test;

public class TestHiveCatalogCapability {

  @Test
  public void testColumnConstraintsOnHive2() {
    HiveCatalogCapability capability = new HiveCatalogCapability(() -> HiveVersion.HIVE2);

    CapabilityResult notNull = capability.columnNotNull();
    assertFalse(notNull.supported());
    assertTrue(notNull.unsupportedMessage().contains("connected Hive Metastore version is HIVE2"));

    CapabilityResult defaultValue = capability.columnDefaultValue();
    assertFalse(defaultValue.supported());
    assertTrue(
        defaultValue.unsupportedMessage().contains("connected Hive Metastore version is HIVE2"));
  }

  @Test
  public void testColumnConstraintsOnHive3() {
    HiveCatalogCapability capability = new HiveCatalogCapability(() -> HiveVersion.HIVE3);
    assertTrue(capability.columnNotNull().supported());
    assertTrue(capability.columnDefaultValue().supported());
  }

  @Test
  public void testVersionIsResolvedLazilyAndCached() {
    int[] resolutions = {0};
    HiveCatalogCapability capability =
        new HiveCatalogCapability(
            () -> {
              resolutions[0]++;
              return HiveVersion.HIVE3;
            });
    assertEquals(0, resolutions[0]);
    assertTrue(capability.columnNotNull().supported());
    assertTrue(capability.columnDefaultValue().supported());
    assertEquals(1, resolutions[0]);
  }
}
