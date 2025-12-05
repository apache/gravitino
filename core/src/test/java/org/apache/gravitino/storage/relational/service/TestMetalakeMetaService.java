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
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
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
