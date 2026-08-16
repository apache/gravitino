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
package org.apache.gravitino.iceberg.common.idempotency;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIdempotencyStoreFactory {

  @Test
  void testCreateDefaultsToInMemoryStore() throws IOException {
    try (IdempotencyStore store = IdempotencyStoreFactory.create(new IcebergConfig())) {
      Assertions.assertInstanceOf(InMemoryIdempotencyStore.class, store);
    }
  }

  @Test
  void testCreateResolvesShortName() throws IOException {
    IcebergConfig config =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.ICEBERG_IDEMPOTENCY_STORE_TYPE,
                IcebergConstants.ICEBERG_IDEMPOTENCY_STORE_IN_MEMORY));
    try (IdempotencyStore store = IdempotencyStoreFactory.create(config)) {
      Assertions.assertInstanceOf(InMemoryIdempotencyStore.class, store);
    }
  }

  @Test
  void testCreateResolvesCustomClassNameAndInitializesIt() throws IOException {
    IcebergConfig config =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.ICEBERG_IDEMPOTENCY_STORE_TYPE,
                // Binary name, since Class.forName cannot resolve a nested class by canonical name.
                RecordingIdempotencyStore.class.getName()));
    try (IdempotencyStore store = IdempotencyStoreFactory.create(config)) {
      Assertions.assertInstanceOf(RecordingIdempotencyStore.class, store);
      Assertions.assertTrue(((RecordingIdempotencyStore) store).initialized);
    }
  }

  @Test
  void testCreateFailsForUnknownStore() {
    IcebergConfig config =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.ICEBERG_IDEMPOTENCY_STORE_TYPE, "com.example.NoSuchStore"));
    Exception exception =
        Assertions.assertThrows(
            RuntimeException.class, () -> IdempotencyStoreFactory.create(config));
    Assertions.assertTrue(exception.getMessage().contains("com.example.NoSuchStore"));
  }

  /** Store loaded by class name, asserting the factory initializes what it constructs. */
  public static class RecordingIdempotencyStore implements IdempotencyStore {

    private boolean initialized = false;

    @Override
    public void initialize(Map<String, String> properties) {
      this.initialized = true;
    }

    @Override
    public ReserveResult reserve(String idempotencyKey, String operationBinding, long expiresAtMs) {
      return ReserveResult.RESERVED;
    }

    @Override
    public Optional<IdempotencyRecord> load(String idempotencyKey) {
      return Optional.empty();
    }

    @Override
    public void finalizeRecord(String idempotencyKey, int httpStatus, String responseSummary) {}

    @Override
    public void release(String idempotencyKey) {}

    @Override
    public int purgeExpired(long beforeMs) {
      return 0;
    }

    @Override
    public void close() {}
  }
}
