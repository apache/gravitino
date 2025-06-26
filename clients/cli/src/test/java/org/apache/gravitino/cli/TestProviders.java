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

package org.apache.gravitino.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.gravitino.Catalog;
import org.junit.jupiter.api.Test;

public class TestProviders {

  @Test
  public void validProviders() {
    assertTrue(Providers.isValidProvider(Providers.HIVE), "HIVE should be a valid entity");
    assertTrue(Providers.isValidProvider(Providers.HADOOP), "HADOOP should be a valid entity");
    assertTrue(Providers.isValidProvider(Providers.ICEBERG), "ICEBERG should be a valid entity");
    assertTrue(Providers.isValidProvider(Providers.MYSQL), "MYSQL should be a valid entity");
    assertTrue(Providers.isValidProvider(Providers.POSTGRES), "POSTGRES should be a valid entity");
    assertTrue(Providers.isValidProvider(Providers.KAFKA), "KAFKA should be a valid entity");
    assertTrue(Providers.isValidProvider(Providers.DORIS), "DORIS should be a valid entity");
    assertTrue(Providers.isValidProvider(Providers.PAIMON), "PAIMON should be a valid entity");
    assertTrue(Providers.isValidProvider(Providers.HUDI), "HUDI should be a valid entity");
    assertTrue(
        Providers.isValidProvider(Providers.OCEANBASE), "OCEANBASE should be a valid entity");
    assertTrue(Providers.isValidProvider(Providers.MODEL), "MODEL should be a valid entity");
  }

  @Test
  public void invalidProvider() {
    assertFalse(
        Providers.isValidProvider("invalidEntity"), "An invalid provider should return false");
  }

  @Test
  public void nullEntity() {
    assertFalse(
        Providers.isValidProvider(null), "Null should return false as it's not a valid provider");
  }

  @Test
  public void emptyEntity() {
    assertFalse(
        Providers.isValidProvider(""),
        "Empty string should return false as it's not a valid entity");
  }

  @Test
  public void caseSensitive() {
    assertFalse(Providers.isValidProvider("HIVE"), "Providers should be case-sensitive");
  }

  @Test
  public void internalNotNull() {
    assertNotNull(Providers.internal(Providers.HIVE), "Internal string should not be null");
    assertNotNull(Providers.internal(Providers.HADOOP), "Internal string should not be null");
    assertNotNull(Providers.internal(Providers.ICEBERG), "Internal string should not be null");
    assertNotNull(Providers.internal(Providers.MYSQL), "Internal string should not be null");
    assertNotNull(Providers.internal(Providers.POSTGRES), "Internal string should not be null");
    assertNotNull(Providers.internal(Providers.KAFKA), "Internal string should not be null");
    assertNotNull(Providers.internal(Providers.DORIS), "Internal string should not be null");
    assertNotNull(Providers.internal(Providers.PAIMON), "Internal string should not be null");
    assertNotNull(Providers.internal(Providers.HUDI), "Internal string should not be null");
    assertNotNull(Providers.internal(Providers.OCEANBASE), "Internal string should not be null");
    assertNotNull(Providers.internal(Providers.MODEL), "Internal string should not be null");
  }

  @Test
  public void internalNull() {
    assertThrows(
        IllegalArgumentException.class,
        () -> Providers.internal("unknown"),
        "Expected an IllegalArgumentException for an unknown provider");
  }

  @Test
  public void catalogTypeNotNull() {
    assertNotNull(Providers.catalogType(Providers.HIVE), "Catalog type should not be null");
    assertNotNull(Providers.catalogType(Providers.HADOOP), "Catalog type should not be null");
    assertNotNull(Providers.catalogType(Providers.ICEBERG), "Catalog type should not be null");
    assertNotNull(Providers.catalogType(Providers.MYSQL), "Catalog type should not be null");
    assertNotNull(Providers.catalogType(Providers.POSTGRES), "Catalog type should not be null");
    assertNotNull(Providers.catalogType(Providers.KAFKA), "Catalog type should not be null");
    assertNotNull(Providers.catalogType(Providers.DORIS), "Catalog type should not be null");
    assertNotNull(Providers.catalogType(Providers.PAIMON), "Catalog type should not be null");
    assertNotNull(Providers.catalogType(Providers.HUDI), "Catalog type should not be null");
    assertNotNull(Providers.catalogType(Providers.OCEANBASE), "Catalog type should not be null");
    assertNotNull(Providers.catalogType(Providers.MODEL), "Catalog type should not be null");
  }

  @Test
  public void catalogTypeNull() {
    assertThrows(
        IllegalArgumentException.class,
        () -> Providers.catalogType("unknown"),
        "Expected an IllegalArgumentException for an unknown provider");
  }

  @Test
  public void modelProviderInternal() {
    assertEquals(
        "model", Providers.internal(Providers.MODEL), "MODEL internal should return 'model'");
  }

  @Test
  public void modelProviderCatalogType() {
    assertEquals(
        Catalog.Type.MODEL,
        Providers.catalogType(Providers.MODEL),
        "MODEL catalog type should be MODEL");
  }
}
