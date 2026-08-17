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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.po.MetalakePO;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

public class TestMetalakeMetaService extends TestJDBCBackend {

  private static final String METALAKE_NAME = "metalake_for_metalake_test";

  @TestTemplate
  public void testInsertAlreadyExistsException() throws IOException {
    BaseMetalake metalake = createAndInsertMakeLake(METALAKE_NAME);
    BaseMetalake metalakeCopy =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalake.name(), AUDIT_INFO);
    assertThrows(EntityAlreadyExistsException.class, () -> backend.insert(metalakeCopy, false));
  }

  @TestTemplate
  public void testUpdateAlreadyExistsException() throws IOException {
    BaseMetalake metalake = createAndInsertMakeLake(METALAKE_NAME);
    BaseMetalake metalakeCopy = createAndInsertMakeLake("another_metalake_for_metalake_test");
    assertThrows(
        EntityAlreadyExistsException.class,
        () ->
            backend.update(
                metalakeCopy.nameIdentifier(),
                Entity.EntityType.METALAKE,
                e -> createBaseMakeLake(metalakeCopy.id(), metalake.name(), AUDIT_INFO)));
  }

  @TestTemplate
  void testUpdateMetalakeWithNullableComment() throws IOException {
    BaseMetalake metalake =
        BaseMetalake.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(METALAKE_NAME)
            .withAuditInfo(AUDIT_INFO)
            .withComment(null)
            .withProperties(null)
            .withVersion(SchemaVersion.V_0_1)
            .build();
    backend.insert(metalake, false);

    backend.update(
        metalake.nameIdentifier(),
        Entity.EntityType.METALAKE,
        e ->
            BaseMetalake.builder()
                .withId(metalake.id())
                .withName(metalake.name())
                .withAuditInfo(AUDIT_INFO)
                .withComment("comment")
                .withProperties(metalake.properties())
                .withVersion(metalake.getVersion())
                .build());

    BaseMetalake updatedMetalake =
        backend.get(metalake.nameIdentifier(), Entity.EntityType.METALAKE);
    Assertions.assertNotNull(updatedMetalake.comment());

    backend.delete(metalake.nameIdentifier(), Entity.EntityType.METALAKE, false);
  }

  @TestTemplate
  public void testAlterAndDeleteUseCurrentVersion() throws IOException {
    BaseMetalake metalake = createAndInsertMakeLake(METALAKE_NAME);
    MetalakePO oldPO =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeMetaByName(metalake.name()));
    BaseMetalake updatedMetalake =
        BaseMetalake.builder()
            .withId(metalake.id())
            .withName(metalake.name())
            .withAuditInfo(metalake.auditInfo())
            .withComment("updated")
            .withProperties(metalake.properties())
            .withVersion(metalake.getVersion())
            .build();
    MetalakePO newPO = POConverters.updateMetalakePOWithVersion(oldPO, updatedMetalake);

    int updated =
        SessionUtils.doWithCommitAndFetchResult(
            MetalakeMetaMapper.class, mapper -> mapper.updateMetalakeMeta(newPO, oldPO));
    int staleUpdate =
        SessionUtils.doWithCommitAndFetchResult(
            MetalakeMetaMapper.class, mapper -> mapper.updateMetalakeMeta(newPO, oldPO));
    int staleDelete =
        SessionUtils.doWithCommitAndFetchResult(
            MetalakeMetaMapper.class,
            mapper ->
                mapper.softDeleteMetalakeMetaByMetalakeId(
                    metalake.id(), oldPO.getCurrentVersion()));
    Assertions.assertEquals(1, updated);
    Assertions.assertEquals(0, staleUpdate);
    Assertions.assertEquals(0, staleDelete);
    assertTrue(backend.exists(metalake.nameIdentifier(), Entity.EntityType.METALAKE));
    int deleted =
        SessionUtils.doWithCommitAndFetchResult(
            MetalakeMetaMapper.class,
            mapper ->
                mapper.softDeleteMetalakeMetaByMetalakeId(
                    metalake.id(), newPO.getCurrentVersion()));
    Assertions.assertEquals(1, deleted);
  }

  @TestTemplate
  public void testAlterReportsOptimisticLockConflict() throws IOException {
    BaseMetalake metalake = createAndInsertMakeLake(METALAKE_NAME);

    assertThrows(
        OptimisticLockException.class,
        () ->
            MetalakeMetaService.getInstance()
                .updateMetalake(
                    metalake.nameIdentifier(),
                    entity -> {
                      BaseMetalake current = (BaseMetalake) entity;
                      MetalakePO currentPO =
                          SessionUtils.getWithoutCommit(
                              MetalakeMetaMapper.class,
                              mapper -> mapper.selectMetalakeMetaByName(current.name()));
                      BaseMetalake competingUpdate =
                          BaseMetalake.builder()
                              .withId(current.id())
                              .withName(current.name())
                              .withAuditInfo(current.auditInfo())
                              .withComment("competing update")
                              .withProperties(current.properties())
                              .withVersion(current.getVersion())
                              .build();
                      MetalakePO competingPO =
                          POConverters.updateMetalakePOWithVersion(currentPO, competingUpdate);
                      SessionUtils.doWithCommitAndFetchResult(
                          MetalakeMetaMapper.class,
                          mapper -> mapper.updateMetalakeMeta(competingPO, currentPO));
                      return BaseMetalake.builder()
                          .withId(current.id())
                          .withName(current.name())
                          .withAuditInfo(current.auditInfo())
                          .withComment("requested update")
                          .withProperties(current.properties())
                          .withVersion(current.getVersion())
                          .build();
                    }));
  }

  @TestTemplate
  public void testAlterReportsNoSuchWhenMetalakeIsDeletedConcurrently() throws IOException {
    BaseMetalake metalake = createAndInsertMakeLake(METALAKE_NAME);

    assertThrows(
        NoSuchEntityException.class,
        () ->
            MetalakeMetaService.getInstance()
                .updateMetalake(
                    metalake.nameIdentifier(),
                    entity -> {
                      BaseMetalake current = (BaseMetalake) entity;
                      MetalakePO currentPO =
                          SessionUtils.getWithoutCommit(
                              MetalakeMetaMapper.class,
                              mapper -> mapper.selectMetalakeMetaById(current.id()));
                      SessionUtils.doWithCommitAndFetchResult(
                          MetalakeMetaMapper.class,
                          mapper ->
                              mapper.softDeleteMetalakeMetaByMetalakeId(
                                  current.id(), currentPO.getCurrentVersion()));
                      return BaseMetalake.builder()
                          .withId(current.id())
                          .withName(current.name())
                          .withAuditInfo(current.auditInfo())
                          .withComment("requested update")
                          .withProperties(current.properties())
                          .withVersion(current.getVersion())
                          .build();
                    }));
  }

  @TestTemplate
  public void testDeleteReportsOptimisticLockConflict() throws IOException {
    BaseMetalake metalake = createAndInsertMakeLake(METALAKE_NAME);
    MetalakePO stalePO =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeMetaByName(metalake.name()));
    BaseMetalake competingUpdate =
        BaseMetalake.builder()
            .withId(metalake.id())
            .withName(metalake.name())
            .withAuditInfo(metalake.auditInfo())
            .withComment("competing update")
            .withProperties(metalake.properties())
            .withVersion(metalake.getVersion())
            .build();
    MetalakePO competingPO = POConverters.updateMetalakePOWithVersion(stalePO, competingUpdate);
    SessionUtils.doWithCommitAndFetchResult(
        MetalakeMetaMapper.class, mapper -> mapper.updateMetalakeMeta(competingPO, stalePO));

    assertThrows(
        OptimisticLockException.class,
        () ->
            SessionUtils.doMultipleWithCommit(
                () ->
                    MetalakeMetaService.getInstance()
                        .deleteMetalakeWithVersion(
                            metalake.nameIdentifier(),
                            metalake.id(),
                            stalePO.getCurrentVersion())));
    assertTrue(backend.exists(metalake.nameIdentifier(), Entity.EntityType.METALAKE));
  }

  @TestTemplate
  public void testNonCascadeDeleteRollsBackMetalakeFence() throws IOException {
    BaseMetalake metalake = createAndInsertMakeLake(METALAKE_NAME);
    createAndInsertCatalog(METALAKE_NAME, "catalog");
    MetalakePO beforeDelete =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeMetaByName(metalake.name()));

    assertThrows(
        NonEmptyEntityException.class,
        () -> MetalakeMetaService.getInstance().deleteMetalake(metalake.nameIdentifier(), false));

    MetalakePO afterDelete =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeMetaByName(metalake.name()));
    Assertions.assertEquals(beforeDelete.getCurrentVersion(), afterDelete.getCurrentVersion());
    assertTrue(backend.exists(metalake.nameIdentifier(), Entity.EntityType.METALAKE));
  }

  @TestTemplate
  public void testMetaLifeCycleFromCreationToDeletion() throws IOException {
    // meta data creation
    BaseMetalake metalake = createAndInsertMakeLake(METALAKE_NAME);
    BaseMetalake anotherMetaLake = createAndInsertMakeLake("another_metalake_for_metalake_test");

    // meta data list
    List<BaseMetalake> metaLakes =
        backend.list(metalake.namespace(), Entity.EntityType.METALAKE, true);
    assertTrue(metaLakes.contains(metalake));

    // meta data soft delete
    backend.delete(metalake.nameIdentifier(), Entity.EntityType.METALAKE, true);

    // check existence after soft delete
    assertFalse(backend.exists(metalake.nameIdentifier(), Entity.EntityType.METALAKE));
    assertTrue(backend.exists(anotherMetaLake.nameIdentifier(), Entity.EntityType.METALAKE));

    // check legacy record after soft delete
    assertTrue(legacyRecordExistsInDB(metalake.id(), Entity.EntityType.METALAKE));

    // meta data hard delete
    for (Entity.EntityType entityType : Entity.EntityType.values()) {
      backend.hardDeleteLegacyData(entityType, Instant.now().toEpochMilli() + 1000);
    }
    assertFalse(legacyRecordExistsInDB(metalake.id(), Entity.EntityType.METALAKE));
  }
}
