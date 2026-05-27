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

package org.apache.gravitino.iceberg.service.purge;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.service.CatalogWrapperForREST;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergNamespaceOperationExecutor;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergTableOperationExecutor;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.jdbc.JdbcClientPool;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.ImmutableRegisterTableRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestIcebergPurgeTombstone {

  private static final Schema TABLE_SCHEMA =
      new Schema(NestedField.required(1, "test_field", StringType.get()));

  private JdbcClientPool pool;
  private IcebergPurgeJobStore store;
  private IcebergPurgeService purgeService;
  private CatalogWrapperForREST wrapper;
  private IcebergRequestContext context;

  @BeforeEach
  void setUp() throws Exception {
    String url =
        "jdbc:h2:mem:purge_tombstone_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1;MODE=MySQL";
    Map<String, String> props = new HashMap<>();
    props.put("jdbc.user", "sa");
    props.put("jdbc.password", "");
    pool = new JdbcClientPool(url, props);
    pool.run(
        conn -> {
          try (Statement st = conn.createStatement()) {
            for (String ddl : PurgeTestSchema.H2_CREATE.split(";")) {
              if (!ddl.trim().isEmpty()) {
                st.execute(ddl);
              }
            }
          }
          return null;
        });

    store = new IcebergPurgeJobStore(pool);
    purgeService = new IcebergPurgeService(store, new IcebergConfig(new HashMap<>()));
    wrapper = mock(CatalogWrapperForREST.class);
    context = mock(IcebergRequestContext.class);
    when(context.catalogName()).thenReturn("cat");
    when(context.userName()).thenReturn("alice");
    when(context.requestCredentialVending()).thenReturn(false);
  }

  @AfterEach
  void tearDown() {
    if (purgeService != null) {
      purgeService.close();
    }
    if (pool != null) {
      pool.close();
    }
  }

  @Test
  void testCreateTableBlockedWhileActiveJob() {
    long id = store.enqueue(TestIcebergPurgeJobStore.sampleJob());
    IcebergTableOperationExecutor executor = newTableExecutor();

    Assertions.assertThrows(
        AlreadyExistsException.class,
        () -> executor.createTable(context, Namespace.of("db"), createRequestFor("t")));

    store.claimNext(System.currentTimeMillis(), 300_000L, 10);
    store.markSucceeded(id);
    executor.createTable(context, Namespace.of("db"), createRequestFor("t"));

    verify(wrapper).createTable(any(), any(), anyBoolean());
  }

  @Test
  void testRegisterTableBlockedWhileActiveJob() {
    long id = store.enqueue(TestIcebergPurgeJobStore.sampleJob());
    IcebergNamespaceOperationExecutor executor = newNamespaceExecutor();

    Assertions.assertThrows(
        AlreadyExistsException.class,
        () -> executor.registerTable(context, Namespace.of("db"), registerRequestFor("t")));

    store.claimNext(System.currentTimeMillis(), 300_000L, 10);
    store.markSucceeded(id);
    executor.registerTable(context, Namespace.of("db"), registerRequestFor("t"));

    verify(wrapper).registerTable(any(), any(), anyBoolean());
  }

  private IcebergTableOperationExecutor newTableExecutor() {
    return new IcebergTableOperationExecutor(wrapperManager(), purgeService);
  }

  private IcebergNamespaceOperationExecutor newNamespaceExecutor() {
    return new IcebergNamespaceOperationExecutor(wrapperManager(), purgeService);
  }

  private IcebergCatalogWrapperManager wrapperManager() {
    IcebergCatalogWrapperManager manager = mock(IcebergCatalogWrapperManager.class);
    when(manager.getCatalogWrapper("cat")).thenReturn(wrapper);
    return manager;
  }

  private static CreateTableRequest createRequestFor(String table) {
    return CreateTableRequest.builder().withName(table).withSchema(TABLE_SCHEMA).build();
  }

  private static RegisterTableRequest registerRequestFor(String table) {
    return ImmutableRegisterTableRequest.builder()
        .name(table)
        .metadataLocation("s3://bucket/db/" + table + "/metadata/00000.json")
        .build();
  }
}
