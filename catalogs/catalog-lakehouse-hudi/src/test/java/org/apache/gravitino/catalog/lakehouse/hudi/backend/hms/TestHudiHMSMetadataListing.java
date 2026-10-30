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

package org.apache.gravitino.catalog.lakehouse.hudi.backend.hms;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.hive.CachedClientPool;
import org.apache.gravitino.hive.client.HiveClient;
import org.apache.gravitino.utils.ClientPool;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestHudiHMSMetadataListing {
  @Test
  void testListingUsesOnlyStorageMetadataAndPropagatesFailures() throws Exception {
    HudiHMSBackendOps ops = spy(new HudiHMSBackendOps());
    ops.initialize(Map.of());
    ops.clientPool.close();
    CachedClientPool pool = mock(CachedClientPool.class);
    HiveClient client = mock(HiveClient.class);
    ops.clientPool = pool;
    Namespace namespace = Namespace.of("db");
    doReturn(true).when(ops).schemaExists(NameIdentifier.of("db"));
    when(pool.run(any()))
        .thenAnswer(
            invocation -> {
              ClientPool.Action<?, HiveClient, ?> action = invocation.getArgument(0);
              return action.run(client);
            });
    when(client.getAllTables("hive", "db")).thenReturn(List.of("hudi", "broken_csv"));
    when(client.getTableNamesByInputFormatPrefix(
            "hive", "db", List.of("hudi", "broken_csv"), "org.apache.hudi"))
        .thenReturn(List.of("hudi"));

    Assertions.assertArrayEquals(
        new NameIdentifier[] {NameIdentifier.of("db", "hudi")}, ops.listTables(namespace));
    verify(client, never()).getTableObjectsByName(any(), anyString(), anyList());
    GravitinoRuntimeException failure = new GravitinoRuntimeException("Metastore failed");
    when(client.getTableNamesByInputFormatPrefix(
            "hive", "db", List.of("hudi", "broken_csv"), "org.apache.hudi"))
        .thenThrow(failure);
    Assertions.assertSame(
        failure,
        Assertions.assertThrows(GravitinoRuntimeException.class, () -> ops.listTables(namespace)));
  }
}
