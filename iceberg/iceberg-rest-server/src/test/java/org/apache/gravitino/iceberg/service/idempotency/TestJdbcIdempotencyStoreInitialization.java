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

package org.apache.gravitino.iceberg.service.idempotency;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.idempotency.IdempotencyStoreFactory;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class TestJdbcIdempotencyStoreInitialization {
  @Test
  void testStandaloneFailsWithActionableError() {
    SqlSessionFactoryHelper helper = Mockito.mock(SqlSessionFactoryHelper.class);
    Mockito.when(helper.getSqlSessionFactory())
        .thenThrow(new IllegalStateException("not initialized"));
    try (MockedStatic<SqlSessionFactoryHelper> mocked =
        Mockito.mockStatic(SqlSessionFactoryHelper.class)) {
      mocked.when(SqlSessionFactoryHelper::getInstance).thenReturn(helper);
      RuntimeException failure =
          Assertions.assertThrows(
              RuntimeException.class,
              () ->
                  IdempotencyStoreFactory.create(
                      new IcebergConfig(ImmutableMap.of("idempotency-store-type", "jdbc"))));
      Assertions.assertTrue(failure.getCause().getMessage().contains("auxiliary mode"));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"PT0S", "-PT1S", "PT0.000001S", "invalid"})
  void testInvalidCleanupIntervalFailsBeforeAllocatingResources(String interval) {
    try (JdbcIdempotencyStore store = new JdbcIdempotencyStore()) {
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> store.initialize(ImmutableMap.of("idempotency-cleanup-interval", interval)));
    }
  }

  @Test
  void testDefaultCleanupInterval() {
    Assertions.assertEquals(
        "PT5M",
        new IcebergConfig(Collections.emptyMap())
            .get(IcebergConfig.ICEBERG_IDEMPOTENCY_CLEANUP_INTERVAL));
  }
}
