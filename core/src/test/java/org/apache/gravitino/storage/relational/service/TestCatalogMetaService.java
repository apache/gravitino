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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestCatalogMetaService extends TestJDBCBackend {

  private final AuditInfo auditInfo =
      AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
  private final String metalakeName = "metalake_for_catalog_test";

  @BeforeEach
  public void prepare() throws IOException {
    createAndInsertMakeLake(metalakeName);
  }

  @TestTemplate
  public void testInsertAlreadyExistsException() throws IOException {
    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            "catalog",
            auditInfo);
    CatalogEntity catalogCopy =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            "catalog",
            auditInfo);
    backend.insert(catalog, false);
    assertThrows(EntityAlreadyExistsException.class, () -> backend.insert(catalogCopy, false));
  }

  @TestTemplate
  public void testUpdateAlreadyExistsException() throws IOException {
    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            "catalog",
            auditInfo);
    CatalogEntity catalogCopy =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            "catalog1",
            auditInfo);
    backend.insert(catalog, false);
    backend.insert(catalogCopy, false);
    assertThrows(
        EntityAlreadyExistsException.class,
        () ->
            backend.update(
                catalogCopy.nameIdentifier(),
                Entity.EntityType.CATALOG,
                e ->
                    createCatalog(
                        catalogCopy.id(), catalogCopy.namespace(), "catalog", auditInfo)));
  }

  @TestTemplate
  void testUpdateCatalogWithNullableComment() throws IOException {
    CatalogEntity catalog =
        CatalogEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withNamespace(NamespaceUtil.ofCatalog(metalakeName))
            .withName("catalog")
            .withAuditInfo(auditInfo)
            .withComment(null)
            .withProperties(null)
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("test")
            .build();
    backend.insert(catalog, false);

    backend.update(
        catalog.nameIdentifier(),
        Entity.EntityType.CATALOG,
        e ->
            CatalogEntity.builder()
                .withId(catalog.id())
                .withNamespace(catalog.namespace())
                .withName(catalog.name())
                .withAuditInfo(auditInfo)
                .withComment("comment")
                .withProperties(catalog.getProperties())
                .withType(Catalog.Type.RELATIONAL)
                .withProvider("test")
                .build());

    CatalogEntity updatedCatalog = backend.get(catalog.nameIdentifier(), Entity.EntityType.CATALOG);
    Assertions.assertNotNull(updatedCatalog.getComment());
  }

  @TestTemplate
  public void testMetaLifeCycleFromCreationToDeletion() throws IOException {
    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            "catalog",
            auditInfo);
    backend.insert(catalog, false);

    List<CatalogEntity> catalogs =
        backend.list(catalog.namespace(), Entity.EntityType.CATALOG, true);
    assertTrue(catalogs.contains(catalog));
    assertEquals(1, catalogs.size());

    CatalogEntity catalogEntity = backend.get(catalog.nameIdentifier(), Entity.EntityType.CATALOG);
    assertEquals(catalog, catalogEntity);
    Assertions.assertNotNull(
        CatalogMetaService.getInstance()
            .getCatalogPOByName(catalogEntity.namespace().level(0), catalog.name()));
    assertEquals(
        catalog.id(),
        CatalogMetaService.getInstance()
            .getCatalogIdByName(catalog.namespace().level(0), catalog.name()));

    // meta data soft delete
    backend.delete(NameIdentifierUtil.ofMetalake(metalakeName), Entity.EntityType.METALAKE, true);

    assertEquals(
        0,
        SessionUtils.doWithCommitAndFetchResult(
                CatalogMetaMapper.class,
                mapper -> mapper.listCatalogPOsByMetalakeName(metalakeName))
            .size());

    // check existence after soft delete
    assertFalse(backend.exists(catalog.nameIdentifier(), Entity.EntityType.CATALOG));

    // check legacy record after soft delete
    assertTrue(legacyRecordExistsInDB(catalog.id(), Entity.EntityType.CATALOG));

    // meta data hard delete
    backend.hardDeleteLegacyData(Entity.EntityType.CATALOG, Instant.now().toEpochMilli() + 3000);
    assertFalse(legacyRecordExistsInDB(catalog.id(), Entity.EntityType.CATALOG));
  }
}
