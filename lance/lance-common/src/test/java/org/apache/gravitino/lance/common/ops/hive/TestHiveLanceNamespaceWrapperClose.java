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
package org.apache.gravitino.lance.common.ops.hive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Field;
import java.util.Map;
import org.apache.gravitino.lance.common.config.LanceConfig;
import org.junit.jupiter.api.Test;
import org.lance.namespace.hive2.Hive2Namespace;

/**
 * Verifies that closing {@link HiveLanceNamespaceWrapper} releases the Hive Metastore client pool
 * held by its {@link Hive2Namespace} delegate.
 *
 * <p>{@code lance-namespace-impls} 0.4.0 (lance-format/lance-namespace-impls#137) makes {@code
 * Hive2Namespace} implement {@code Closeable}; before the bump the pooled metastore connections
 * were leaked on shutdown. The delegate's client pool connects to the metastore only lazily (on the
 * first metastore call), so this test exercises construction and close entirely offline — no
 * running metastore required.
 */
class TestHiveLanceNamespaceWrapperClose {

  private static final Map<String, String> CONFIG =
      ImmutableMap.of(
          "namespace-backend", "hive2",
          "hive-metastore-uris", "thrift://localhost:1",
          "hive-warehouse", "s3://test-bucket/warehouse",
          "hive-client-pool-size", "1");

  @Test
  void testCloseReleasesTheClientPool() throws Exception {
    HiveLanceNamespaceWrapper wrapper = new HiveLanceNamespaceWrapper(new LanceConfig(CONFIG));
    // Trigger lazy initialization so the delegate (and its client pool) is built.
    wrapper.asTableOps();
    Object pool = clientPoolOf(wrapper.getDelegate());
    assertNotNull(pool, "the Hive2Namespace client pool should be created on initialization");

    // Closing the wrapper must release the delegate's Hive metastore client pool; before the
    // lance-namespace-impls 0.4.0 Closeable fix this pool was leaked on shutdown.
    wrapper.close();

    assertClosed(pool);
  }

  private static Object clientPoolOf(Object hive2Namespace) {
    return readField(hive2Namespace, "clientPool");
  }

  /** Assert the lance-namespace ClientPoolImpl's private {@code closed} flag is set. */
  private static void assertClosed(Object clientPool) {
    Object closed = readField(clientPool, "closed");
    assertEquals(Boolean.TRUE, closed, "the Hive metastore client pool must be closed");
  }

  private static Object readField(Object target, String fieldName) {
    // Walk up the class hierarchy: e.g. ClientPoolImpl#closed is declared on a superclass of
    // Hive2ClientPool.
    for (Class<?> cls = target.getClass(); cls != null; cls = cls.getSuperclass()) {
      try {
        Field field = cls.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
      } catch (NoSuchFieldException e) {
        // try the superclass
      } catch (IllegalAccessException e) {
        return fail("Could not read field '" + fieldName + "': " + e.getMessage());
      }
    }
    return fail(
        "Field '"
            + fieldName
            + "' not found on "
            + target.getClass().getName()
            + "; the Hive client-pool close assumption must be re-verified.");
  }
}
