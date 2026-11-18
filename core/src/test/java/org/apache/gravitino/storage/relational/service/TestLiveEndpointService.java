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
package org.apache.gravitino.storage.relational.service;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.storage.relational.RelationalEntityStoreIdResolver;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.TestTemplate;

/** Tests database-backed liveness and identity checks for relation endpoints. */
public class TestLiveEndpointService extends TestJDBCBackend {

  @TestTemplate
  public void testRejectsRecreatedCatalogWithSameName() throws IOException {
    String metalake = "endpoint_fence_metalake";
    String catalogName = "endpoint_fence_catalog";
    createAndInsertMakeLake(metalake);
    CatalogEntity oldCatalog = createAndInsertCatalog(metalake, catalogName);
    NamespacedEntityId observed =
        EntityIdService.getEntityIds(oldCatalog.nameIdentifier(), Entity.EntityType.CATALOG);

    assertThrows(
        IllegalStateException.class,
        () ->
            LiveEndpointService.lockLiveEndpoint(
                oldCatalog.nameIdentifier(), Entity.EntityType.CATALOG, observed));
    SessionUtils.doMultipleWithCommit(
        () ->
            LiveEndpointService.lockLiveEndpoint(
                oldCatalog.nameIdentifier(), Entity.EntityType.CATALOG, observed));

    CatalogMetaService.getInstance().deleteCatalog(oldCatalog.nameIdentifier(), true);
    CatalogEntity replacement = createAndInsertCatalog(metalake, catalogName);
    assertNotEquals(oldCatalog.id(), replacement.id());
    assertThrows(
        NoSuchEntityException.class,
        () ->
            SessionUtils.doMultipleWithCommit(
                () ->
                    LiveEndpointService.lockLiveEndpoint(
                        oldCatalog.nameIdentifier(), Entity.EntityType.CATALOG, observed)));
  }

  @TestTemplate
  public void testLocksSchemaScopedTarget() throws IOException {
    String metalake = "endpoint_fence_table_metalake";
    String catalog = "endpoint_fence_table_catalog";
    String schema = "endpoint_fence_table_schema";
    createAndInsertMakeLake(metalake);
    createAndInsertCatalog(metalake, catalog);
    createAndInsertSchema(metalake, catalog, schema);
    TableEntity table =
        createAndInsertTableEntity(Namespace.of(metalake, catalog, schema), "endpoint_table");
    NamespacedEntityId observed =
        EntityIdService.getEntityIds(table.nameIdentifier(), Entity.EntityType.TABLE);

    SessionUtils.doMultipleWithCommit(
        () ->
            LiveEndpointService.lockLiveEndpoint(
                table.nameIdentifier(), Entity.EntityType.TABLE, observed));
  }

  @TestTemplate
  public void testRejectsTargetDeletedAfterIdResolution() throws Exception {
    String metalake = "endpoint_race_metalake";
    createAndInsertMakeLake(metalake);
    CatalogEntity catalog = createAndInsertCatalog(metalake, "endpoint_race_catalog");
    CountDownLatch resolved = new CountDownLatch(1);
    CountDownLatch continueWrite = new CountDownLatch(1);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<Throwable> write =
          executor.submit(
              () -> {
                NamespacedEntityId observed =
                    EntityIdService.getEntityIds(
                        catalog.nameIdentifier(), Entity.EntityType.CATALOG);
                resolved.countDown();
                if (!continueWrite.await(30, TimeUnit.SECONDS)) {
                  throw new IllegalStateException("Timed out waiting for endpoint deletion");
                }
                try {
                  SessionUtils.doMultipleWithCommit(
                      () ->
                          LiveEndpointService.lockLiveEndpoint(
                              catalog.nameIdentifier(), Entity.EntityType.CATALOG, observed));
                  return null;
                } catch (Throwable failure) {
                  return failure;
                }
              });
      assertTrue(resolved.await(30, TimeUnit.SECONDS));
      CatalogMetaService.getInstance().deleteCatalog(catalog.nameIdentifier(), true);
      continueWrite.countDown();
      assertInstanceOf(NoSuchEntityException.class, write.get(30, TimeUnit.SECONDS));
    } finally {
      continueWrite.countDown();
      executor.shutdownNow();
    }
  }

  @TestTemplate
  public void testRechecksNameAfterTransactionLocalLookup() throws Exception {
    String metalake = "endpoint_rename_metalake";
    createAndInsertMakeLake(metalake);
    CatalogEntity catalog = createAndInsertCatalog(metalake, "endpoint_rename_catalog");
    NamespacedEntityId observed =
        EntityIdService.getEntityIds(catalog.nameIdentifier(), Entity.EntityType.CATALOG);

    SessionUtils.doMultipleWithCommit(
        () -> {
          new RelationalEntityStoreIdResolver()
              .getEntityIds(catalog.nameIdentifier(), Entity.EntityType.CATALOG);
          try (SqlSession otherSession =
                  SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
              Connection connection = otherSession.getConnection();
              PreparedStatement rename =
                  connection.prepareStatement(
                      "UPDATE catalog_meta SET catalog_name = ? WHERE catalog_id = ?")) {
            rename.setString(1, "endpoint_renamed_catalog");
            rename.setLong(2, catalog.id());
            assertTrue(rename.executeUpdate() == 1);
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
          assertThrows(
              NoSuchEntityException.class,
              () ->
                  LiveEndpointService.lockLiveEndpoint(
                      catalog.nameIdentifier(), Entity.EntityType.CATALOG, observed));
        });
  }
}
