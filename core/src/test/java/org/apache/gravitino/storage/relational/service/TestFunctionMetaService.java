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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.meta.FunctionEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.FunctionMetaMapper;
import org.apache.gravitino.storage.relational.mapper.FunctionVersionMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.po.FunctionPO;
import org.apache.gravitino.storage.relational.po.SchemaPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestFunctionMetaService extends TestJDBCBackend {
  private final String metalakeName = GravitinoITUtils.genRandomName("tst_metalake");
  private final String catalogName = GravitinoITUtils.genRandomName("tst_fn_catalog");
  private final String schemaName = GravitinoITUtils.genRandomName("tst_fn_schema");

  @BeforeEach
  public void prepare() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    createAndInsertSchema(metalakeName, catalogName, schemaName);
  }

  @TestTemplate
  public void testInsertAlreadyExistsException() throws IOException {
    FunctionEntity function =
        createFunctionEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFunction(metalakeName, catalogName, schemaName),
            "test_function",
            AUDIT_INFO);
    FunctionEntity functionCopy =
        createFunctionEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFunction(metalakeName, catalogName, schemaName),
            "test_function",
            AUDIT_INFO);

    FunctionMetaService.getInstance().insertFunction(function, false);
    assertThrows(
        EntityAlreadyExistsException.class,
        () -> FunctionMetaService.getInstance().insertFunction(functionCopy, false));
  }

  @TestTemplate
  public void testInsertAndGetFunction() throws IOException {
    String functionName = GravitinoITUtils.genRandomName("test_function");
    Namespace ns = NamespaceUtil.ofFunction(metalakeName, catalogName, schemaName);
    FunctionEntity function =
        createFunctionEntity(RandomIdGenerator.INSTANCE.nextId(), ns, functionName, AUDIT_INFO);

    FunctionMetaService.getInstance().insertFunction(function, false);

    // Get function using standard identifier (always returns latest version)
    NameIdentifier functionIdent =
        NameIdentifier.of(metalakeName, catalogName, schemaName, functionName);
    FunctionEntity loadedFunction =
        FunctionMetaService.getInstance().getFunctionByIdentifier(functionIdent);

    assertNotNull(loadedFunction);
    assertEquals(function.id(), loadedFunction.id());
    assertEquals(function.name(), loadedFunction.name());
    assertEquals(function.comment(), loadedFunction.comment());
    assertEquals(function.functionType(), loadedFunction.functionType());
    assertEquals(function.deterministic(), loadedFunction.deterministic());
  }

  @TestTemplate
  public void testGetFunctionIdBySchemaIdAndFunctionName() throws IOException {
    String functionName = GravitinoITUtils.genRandomName("test_function");
    Namespace ns = NamespaceUtil.ofFunction(metalakeName, catalogName, schemaName);
    FunctionEntity function =
        createFunctionEntity(RandomIdGenerator.INSTANCE.nextId(), ns, functionName, AUDIT_INFO);
    FunctionMetaService.getInstance().insertFunction(function, false);

    Long schemaId =
        EntityIdService.getEntityId(
            NameIdentifier.of(metalakeName, catalogName, schemaName), Entity.EntityType.SCHEMA);
    Long functionId =
        FunctionMetaService.getInstance()
            .getFunctionIdBySchemaIdAndFunctionName(schemaId, functionName);
    assertEquals(function.id(), functionId);

    assertThrows(
        NoSuchEntityException.class,
        () ->
            FunctionMetaService.getInstance()
                .getFunctionIdBySchemaIdAndFunctionName(schemaId, functionName + "_missing"));
    assertThrows(
        NoSuchEntityException.class,
        () ->
            FunctionMetaService.getInstance()
                .getFunctionIdBySchemaIdAndFunctionName(-1L, functionName));
  }

  @TestTemplate
  public void testMultipleVersionsInStorage() throws IOException {
    // This test verifies that multiple versions are created in storage layer
    // even though the API always returns the latest version
    String functionName = GravitinoITUtils.genRandomName("test_function");
    Namespace ns = NamespaceUtil.ofFunction(metalakeName, catalogName, schemaName);
    FunctionEntity function =
        createFunctionEntity(RandomIdGenerator.INSTANCE.nextId(), ns, functionName, AUDIT_INFO);

    FunctionMetaService.getInstance().insertFunction(function, false);

    // Update function to create version 2 in storage layer
    NameIdentifier functionIdent =
        NameIdentifier.of(metalakeName, catalogName, schemaName, functionName);
    FunctionEntity updatedFunction =
        FunctionEntity.builder()
            .withId(function.id())
            .withName(function.name())
            .withNamespace(ns)
            .withComment("updated comment")
            .withFunctionType(function.functionType())
            .withDeterministic(function.deterministic())
            .withDefinitions(function.definitions())
            .withAuditInfo(AUDIT_INFO)
            .build();

    FunctionMetaService.getInstance().updateFunction(functionIdent, e -> updatedFunction);

    // Get function always returns latest version
    FunctionEntity loadedLatest =
        FunctionMetaService.getInstance().getFunctionByIdentifier(functionIdent);
    assertEquals("updated comment", loadedLatest.comment());

    // Verify both versions exist in storage
    Map<Integer, Long> versions = listFunctionVersions(function.id());
    assertEquals(2, versions.size());
    assertTrue(versions.containsKey(1));
    assertTrue(versions.containsKey(2));
  }

  @TestTemplate
  public void testListFunctions() throws IOException {
    Namespace ns = NamespaceUtil.ofFunction(metalakeName, catalogName, schemaName);

    String functionName1 = GravitinoITUtils.genRandomName("test_function1");
    FunctionEntity function1 =
        createFunctionEntity(RandomIdGenerator.INSTANCE.nextId(), ns, functionName1, AUDIT_INFO);

    String functionName2 = GravitinoITUtils.genRandomName("test_function2");
    FunctionEntity function2 =
        createFunctionEntity(RandomIdGenerator.INSTANCE.nextId(), ns, functionName2, AUDIT_INFO);

    FunctionMetaService.getInstance().insertFunction(function1, false);
    FunctionMetaService.getInstance().insertFunction(function2, false);

    List<FunctionEntity> functions = FunctionMetaService.getInstance().listFunctionsByNamespace(ns);

    assertEquals(2, functions.size());
    assertTrue(functions.stream().anyMatch(f -> f.name().equals(functionName1)));
    assertTrue(functions.stream().anyMatch(f -> f.name().equals(functionName2)));
  }

  @TestTemplate
  public void testUpdateFunction() throws IOException {
    String functionName = GravitinoITUtils.genRandomName("test_function");
    Namespace ns = NamespaceUtil.ofFunction(metalakeName, catalogName, schemaName);
    FunctionEntity function =
        createFunctionEntity(RandomIdGenerator.INSTANCE.nextId(), ns, functionName, AUDIT_INFO);

    FunctionMetaService.getInstance().insertFunction(function, false);

    // Update function (new version in storage layer)
    NameIdentifier functionIdent =
        NameIdentifier.of(metalakeName, catalogName, schemaName, functionName);
    FunctionEntity updatedFunction =
        FunctionEntity.builder()
            .withId(function.id())
            .withName(function.name())
            .withNamespace(ns)
            .withComment("updated comment")
            .withFunctionType(function.functionType())
            .withDeterministic(true)
            .withDefinitions(function.definitions())
            .withAuditInfo(AUDIT_INFO)
            .build();

    FunctionEntity result =
        FunctionMetaService.getInstance().updateFunction(functionIdent, e -> updatedFunction);

    assertEquals("updated comment", result.comment());
    assertTrue(result.deterministic());

    // Verify both versions exist in DB
    Map<Integer, Long> versions = listFunctionVersions(function.id());
    assertEquals(2, versions.size());
    assertTrue(versions.containsKey(1));
    assertTrue(versions.containsKey(2));
  }

  @TestTemplate
  public void testCreateFunctionWaitsForConcurrentSchemaDelete() throws Exception {
    SchemaPO observedSchemaPO =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper ->
                mapper.selectSchemaByFullQualifiedName(metalakeName, catalogName, schemaName));
    Namespace namespace = NamespaceUtil.ofFunction(metalakeName, catalogName, schemaName);
    FunctionEntity function =
        createFunctionEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            namespace,
            GravitinoITUtils.genRandomName("function_parent_delete_race"),
            AUDIT_INFO);

    CountDownLatch deleteWritten = new CountDownLatch(1);
    CountDownLatch allowDeleteCommit = new CountDownLatch(1);
    CountDownLatch createStarted = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    Future<Throwable> deleteResult =
        executor.submit(
            () -> {
              try {
                SessionUtils.doMultipleWithCommit(
                    () ->
                        assertEquals(
                            Integer.valueOf(1),
                            SessionUtils.getWithoutCommit(
                                SchemaMetaMapper.class,
                                mapper ->
                                    mapper.softDeleteSchemaMetaBySchemaIdAndVersion(
                                        observedSchemaPO.getSchemaId(),
                                        observedSchemaPO.getCurrentVersion()))),
                    () -> {
                      deleteWritten.countDown();
                      await(allowDeleteCommit);
                    });
                return null;
              } catch (Throwable throwable) {
                return throwable;
              }
            });

    try {
      assertTrue(deleteWritten.await(30, TimeUnit.SECONDS));
      Future<Throwable> createResult =
          executor.submit(
              () -> {
                createStarted.countDown();
                try {
                  FunctionMetaService.getInstance().insertFunction(function, false);
                  return null;
                } catch (Throwable throwable) {
                  return throwable;
                }
              });
      assertTrue(createStarted.await(30, TimeUnit.SECONDS));
      assertThrows(TimeoutException.class, () -> createResult.get(500, TimeUnit.MILLISECONDS));

      allowDeleteCommit.countDown();
      assertNull(deleteResult.get(30, TimeUnit.SECONDS));
      Throwable createFailure = createResult.get(30, TimeUnit.SECONDS);
      assertTrue(createFailure instanceof NoSuchEntityException, String.valueOf(createFailure));
    } finally {
      allowDeleteCommit.countDown();
      executor.shutdownNow();
    }

    assertThrows(
        NoSuchEntityException.class,
        () -> FunctionMetaService.getInstance().getFunctionByIdentifier(function.nameIdentifier()));
  }

  @TestTemplate
  public void testUpdateFunctionFailsWhenSchemaIsDeletedConcurrently() throws IOException {
    String functionName = GravitinoITUtils.genRandomName("test_function");
    Namespace namespace = NamespaceUtil.ofFunction(metalakeName, catalogName, schemaName);
    FunctionEntity function =
        createFunctionEntity(
            RandomIdGenerator.INSTANCE.nextId(), namespace, functionName, AUDIT_INFO);
    FunctionMetaService.getInstance().insertFunction(function, false);

    NameIdentifier functionIdent =
        NameIdentifier.of(metalakeName, catalogName, schemaName, functionName);
    NameIdentifier schemaIdent = NameIdentifier.of(metalakeName, catalogName, schemaName);
    FunctionEntity updatedFunction = copyFunctionWithComment(function, "updated comment");

    assertThrows(
        NoSuchEntityException.class,
        () ->
            FunctionMetaService.getInstance()
                .updateFunction(
                    functionIdent,
                    ignored -> {
                      // Reproduce the exact race deterministically: the update has already read the
                      // function, then the schema cascade commits before the write transaction.
                      assertTrue(SchemaMetaService.getInstance().deleteSchema(schemaIdent, true));
                      return updatedFunction;
                    }));

    Map<Integer, Long> versions = listFunctionVersions(function.id());
    assertEquals(1, versions.size());
    assertVersionSoftDeleted(versions, 1);
    assertFalse(versions.containsKey(2));
  }

  @TestTemplate
  public void testUpdateFunctionReportsNoSuchAfterConcurrentDelete() throws IOException {
    String functionName = GravitinoITUtils.genRandomName("test_function");
    Namespace namespace = NamespaceUtil.ofFunction(metalakeName, catalogName, schemaName);
    FunctionEntity function =
        createFunctionEntity(
            RandomIdGenerator.INSTANCE.nextId(), namespace, functionName, AUDIT_INFO);
    FunctionMetaService.getInstance().insertFunction(function, false);

    NameIdentifier functionIdent =
        NameIdentifier.of(metalakeName, catalogName, schemaName, functionName);
    FunctionEntity updatedFunction = copyFunctionWithComment(function, "updated comment");

    assertThrows(
        NoSuchEntityException.class,
        () ->
            FunctionMetaService.getInstance()
                .updateFunction(
                    functionIdent,
                    ignored -> {
                      // Delete only the function so the parent-schema lock still succeeds. The
                      // compare-and-set below must notice the missing function and roll version 2
                      // back with the transaction.
                      assertTrue(FunctionMetaService.getInstance().deleteFunction(functionIdent));
                      return updatedFunction;
                    }));

    Map<Integer, Long> versions = listFunctionVersions(function.id());
    assertEquals(1, versions.size());
    assertVersionSoftDeleted(versions, 1);
    assertFalse(versions.containsKey(2));
  }

  @TestTemplate
  public void testAlterReportsOptimisticLockConflictAndKeepsWinnerVersion() throws IOException {
    String functionName = GravitinoITUtils.genRandomName("function_alter_conflict");
    Namespace namespace = NamespaceUtil.ofFunction(metalakeName, catalogName, schemaName);
    FunctionEntity function =
        createFunctionEntity(
            RandomIdGenerator.INSTANCE.nextId(), namespace, functionName, AUDIT_INFO);
    FunctionMetaService.getInstance().insertFunction(function, false);

    assertThrows(
        OptimisticLockException.class,
        () ->
            FunctionMetaService.getInstance()
                .updateFunction(
                    function.nameIdentifier(),
                    entity -> {
                      try {
                        FunctionMetaService.getInstance()
                            .updateFunction(
                                function.nameIdentifier(),
                                competing ->
                                    copyFunctionWithComment(
                                        (FunctionEntity) competing, "competing update"));
                      } catch (IOException e) {
                        throw new RuntimeException(e);
                      }
                      return copyFunctionWithComment((FunctionEntity) entity, "requested update");
                    }));

    FunctionEntity current =
        FunctionMetaService.getInstance().getFunctionByIdentifier(function.nameIdentifier());
    assertEquals("competing update", current.comment());
    Map<Integer, Long> versions = listFunctionVersions(function.id());
    assertEquals(2, versions.size());
    assertTrue(versions.containsKey(2));
  }

  @TestTemplate
  public void testAlterRollsBackRootCasWhenVersionInsertFails() throws IOException {
    String functionName = GravitinoITUtils.genRandomName("function_version_insert_failure");
    Namespace namespace = NamespaceUtil.ofFunction(metalakeName, catalogName, schemaName);
    FunctionEntity function =
        createFunctionEntity(
            RandomIdGenerator.INSTANCE.nextId(), namespace, functionName, AUDIT_INFO);
    FunctionMetaService.getInstance().insertFunction(function, false);
    FunctionPO originalPO =
        FunctionMetaService.getInstance().getFunctionPOByIdentifier(function.nameIdentifier());
    FunctionEntity conflictingVersion = copyFunctionWithComment(function, "conflicting version");
    FunctionPO conflictingPO =
        FunctionPO.buildFunctionPO(
            conflictingVersion,
            FunctionPO.builder()
                .withMetalakeId(originalPO.metalakeId())
                .withCatalogId(originalPO.catalogId())
                .withSchemaId(originalPO.schemaId())
                .withFunctionLatestVersion(2)
                .withFunctionCurrentVersion(2),
            2);
    SessionUtils.doWithCommit(
        FunctionVersionMetaMapper.class,
        mapper -> mapper.insertFunctionVersionMeta(conflictingPO.functionVersionPO()));

    assertThrows(
        EntityAlreadyExistsException.class,
        () ->
            FunctionMetaService.getInstance()
                .updateFunction(
                    function.nameIdentifier(),
                    entity -> copyFunctionWithComment((FunctionEntity) entity, "must roll back")));

    FunctionPO currentPO =
        FunctionMetaService.getInstance().getFunctionPOByIdentifier(function.nameIdentifier());
    assertEquals(1, currentPO.functionCurrentVersion());
    assertEquals(1, currentPO.functionLatestVersion());
    assertEquals(
        function.comment(),
        FunctionMetaService.getInstance()
            .getFunctionByIdentifier(function.nameIdentifier())
            .comment());
  }

  @TestTemplate
  public void testAlterReportsNoSuchWhenRenamedConcurrently() throws IOException {
    String functionName = GravitinoITUtils.genRandomName("function_alter_renamed");
    Namespace namespace = NamespaceUtil.ofFunction(metalakeName, catalogName, schemaName);
    FunctionEntity function =
        createFunctionEntity(
            RandomIdGenerator.INSTANCE.nextId(), namespace, functionName, AUDIT_INFO);
    FunctionMetaService.getInstance().insertFunction(function, false);
    NameIdentifier renamedIdentifier = NameIdentifier.of(namespace, functionName + "_winner");

    assertThrows(
        NoSuchEntityException.class,
        () ->
            FunctionMetaService.getInstance()
                .updateFunction(
                    function.nameIdentifier(),
                    entity -> {
                      try {
                        FunctionMetaService.getInstance()
                            .updateFunction(
                                function.nameIdentifier(),
                                competing ->
                                    copyFunction(
                                        (FunctionEntity) competing,
                                        renamedIdentifier.name(),
                                        namespace,
                                        "renamed winner"));
                      } catch (IOException e) {
                        throw new RuntimeException(e);
                      }
                      return copyFunctionWithComment((FunctionEntity) entity, "stale update");
                    }));

    assertThrows(
        NoSuchEntityException.class,
        () -> FunctionMetaService.getInstance().getFunctionByIdentifier(function.nameIdentifier()));
    assertEquals(
        "renamed winner",
        FunctionMetaService.getInstance().getFunctionByIdentifier(renamedIdentifier).comment());
  }

  @TestTemplate
  public void testAlterReportsNoSuchWhenMovedConcurrently() throws IOException {
    String targetCatalogName = GravitinoITUtils.genRandomName("function_target_catalog");
    String targetSchemaName = GravitinoITUtils.genRandomName("function_target_schema");
    createAndInsertCatalog(metalakeName, targetCatalogName);
    createAndInsertSchema(metalakeName, targetCatalogName, targetSchemaName);
    String functionName = GravitinoITUtils.genRandomName("function_alter_moved");
    Namespace namespace = NamespaceUtil.ofFunction(metalakeName, catalogName, schemaName);
    Namespace movedNamespace =
        NamespaceUtil.ofFunction(metalakeName, targetCatalogName, targetSchemaName);
    FunctionEntity function =
        createFunctionEntity(
            RandomIdGenerator.INSTANCE.nextId(), namespace, functionName, AUDIT_INFO);
    FunctionMetaService.getInstance().insertFunction(function, false);
    NameIdentifier movedIdentifier = NameIdentifier.of(movedNamespace, functionName);

    assertThrows(
        NoSuchEntityException.class,
        () ->
            FunctionMetaService.getInstance()
                .updateFunction(
                    function.nameIdentifier(),
                    entity -> {
                      try {
                        FunctionMetaService.getInstance()
                            .updateFunction(
                                function.nameIdentifier(),
                                competing ->
                                    copyFunction(
                                        (FunctionEntity) competing,
                                        functionName,
                                        movedNamespace,
                                        "moved winner"));
                      } catch (IOException e) {
                        throw new RuntimeException(e);
                      }
                      return copyFunctionWithComment((FunctionEntity) entity, "stale update");
                    }));

    assertThrows(
        NoSuchEntityException.class,
        () -> FunctionMetaService.getInstance().getFunctionByIdentifier(function.nameIdentifier()));
    assertEquals(
        "moved winner",
        FunctionMetaService.getInstance().getFunctionByIdentifier(movedIdentifier).comment());
    SchemaPO targetSchemaPO =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper ->
                mapper.selectSchemaByFullQualifiedName(
                    metalakeName, targetCatalogName, targetSchemaName));
    FunctionPO movedPO =
        FunctionMetaService.getInstance().getFunctionPOByIdentifier(movedIdentifier);
    assertEquals(targetSchemaPO.getSchemaId(), movedPO.schemaId());
    assertEquals(targetSchemaPO.getCatalogId(), movedPO.catalogId());
    assertEquals(targetSchemaPO.getMetalakeId(), movedPO.metalakeId());
  }

  @TestTemplate
  public void testMoveWaitsForConcurrentTargetSchemaDelete() throws Exception {
    String targetCatalogName = GravitinoITUtils.genRandomName("function_deleted_target_catalog");
    String targetSchemaName = GravitinoITUtils.genRandomName("function_deleted_target_schema");
    createAndInsertCatalog(metalakeName, targetCatalogName);
    createAndInsertSchema(metalakeName, targetCatalogName, targetSchemaName);
    SchemaPO targetSchemaPO =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper ->
                mapper.selectSchemaByFullQualifiedName(
                    metalakeName, targetCatalogName, targetSchemaName));
    String functionName = GravitinoITUtils.genRandomName("function_target_delete_race");
    Namespace sourceNamespace = NamespaceUtil.ofFunction(metalakeName, catalogName, schemaName);
    Namespace targetNamespace =
        NamespaceUtil.ofFunction(metalakeName, targetCatalogName, targetSchemaName);
    FunctionEntity function =
        createFunctionEntity(
            RandomIdGenerator.INSTANCE.nextId(), sourceNamespace, functionName, AUDIT_INFO);
    FunctionMetaService.getInstance().insertFunction(function, false);
    FunctionEntity moved = copyFunction(function, functionName, targetNamespace, "must not move");

    CountDownLatch deleteWritten = new CountDownLatch(1);
    CountDownLatch allowDeleteCommit = new CountDownLatch(1);
    CountDownLatch moveStarted = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    Future<Throwable> deleteResult =
        executor.submit(
            () -> {
              try {
                SessionUtils.doMultipleWithCommit(
                    () ->
                        assertEquals(
                            Integer.valueOf(1),
                            SessionUtils.getWithoutCommit(
                                SchemaMetaMapper.class,
                                mapper ->
                                    mapper.softDeleteSchemaMetaBySchemaIdAndVersion(
                                        targetSchemaPO.getSchemaId(),
                                        targetSchemaPO.getCurrentVersion()))),
                    () -> {
                      deleteWritten.countDown();
                      await(allowDeleteCommit);
                    });
                return null;
              } catch (Throwable throwable) {
                return throwable;
              }
            });

    try {
      assertTrue(deleteWritten.await(30, TimeUnit.SECONDS));
      Future<Throwable> moveResult =
          executor.submit(
              () -> {
                moveStarted.countDown();
                try {
                  FunctionMetaService.getInstance()
                      .updateFunction(function.nameIdentifier(), ignored -> moved);
                  return null;
                } catch (Throwable throwable) {
                  return throwable;
                }
              });
      assertTrue(moveStarted.await(30, TimeUnit.SECONDS));
      assertThrows(TimeoutException.class, () -> moveResult.get(500, TimeUnit.MILLISECONDS));

      allowDeleteCommit.countDown();
      assertNull(deleteResult.get(30, TimeUnit.SECONDS));
      Throwable moveFailure = moveResult.get(30, TimeUnit.SECONDS);
      assertTrue(moveFailure instanceof NoSuchEntityException, String.valueOf(moveFailure));
    } finally {
      allowDeleteCommit.countDown();
      executor.shutdownNow();
    }

    assertEquals(
        function.comment(),
        FunctionMetaService.getInstance()
            .getFunctionByIdentifier(function.nameIdentifier())
            .comment());
    assertEquals(1, listFunctionVersions(function.id()).size());
  }

  @TestTemplate
  public void testDeleteRejectsStaleVersion() throws IOException {
    String functionName = GravitinoITUtils.genRandomName("function_stale_delete");
    Namespace namespace = NamespaceUtil.ofFunction(metalakeName, catalogName, schemaName);
    FunctionEntity function =
        createFunctionEntity(
            RandomIdGenerator.INSTANCE.nextId(), namespace, functionName, AUDIT_INFO);
    FunctionMetaService.getInstance().insertFunction(function, false);
    TagEntity tag =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("function_occ_tag")
            .withNamespace(NamespaceUtil.ofTag(metalakeName))
            .withAuditInfo(AUDIT_INFO)
            .build();
    TagMetaService.getInstance().insertTag(tag, false);
    TagMetaService.getInstance()
        .associateTagsWithMetadataObject(
            function.nameIdentifier(),
            function.type(),
            new NameIdentifier[] {tag.nameIdentifier()},
            new NameIdentifier[0]);
    FunctionPO stalePO =
        FunctionMetaService.getInstance().getFunctionPOByIdentifier(function.nameIdentifier());

    FunctionMetaService.getInstance()
        .updateFunction(
            function.nameIdentifier(),
            entity -> copyFunctionWithComment((FunctionEntity) entity, "winning update"));

    assertThrows(
        OptimisticLockException.class,
        () ->
            FunctionMetaService.getInstance()
                .deleteFunctionWithVersion(function.nameIdentifier(), stalePO));
    assertEquals(
        "winning update",
        FunctionMetaService.getInstance()
            .getFunctionByIdentifier(function.nameIdentifier())
            .comment());
    assertEquals(1, countActiveTagRelForMetadataObject(function.id(), "FUNCTION"));
  }

  @TestTemplate
  public void testDeleteReportsNoSuchWhenDeletedConcurrently() throws IOException {
    String functionName = GravitinoITUtils.genRandomName("function_delete_deleted");
    Namespace namespace = NamespaceUtil.ofFunction(metalakeName, catalogName, schemaName);
    FunctionEntity function =
        createFunctionEntity(
            RandomIdGenerator.INSTANCE.nextId(), namespace, functionName, AUDIT_INFO);
    FunctionMetaService.getInstance().insertFunction(function, false);
    FunctionPO stalePO =
        FunctionMetaService.getInstance().getFunctionPOByIdentifier(function.nameIdentifier());

    FunctionMetaService.getInstance().deleteFunction(function.nameIdentifier());

    assertThrows(
        NoSuchEntityException.class,
        () ->
            FunctionMetaService.getInstance()
                .deleteFunctionWithVersion(function.nameIdentifier(), stalePO));
  }

  @TestTemplate
  public void testDeleteFunction() throws IOException {
    String functionName = GravitinoITUtils.genRandomName("test_function");
    Namespace ns = NamespaceUtil.ofFunction(metalakeName, catalogName, schemaName);
    FunctionEntity function =
        createFunctionEntity(RandomIdGenerator.INSTANCE.nextId(), ns, functionName, AUDIT_INFO);

    FunctionMetaService.getInstance().insertFunction(function, false);

    // Set up owner relation
    UserEntity user =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user1",
            AUDIT_INFO);
    backend.insert(user, false);
    OwnerMetaService.getInstance()
        .setOwner(function.nameIdentifier(), function.type(), user.nameIdentifier(), user.type());

    // Set up role/securable object relation
    SecurableObject schemaObject =
        SecurableObjects.ofSchema(
            SecurableObjects.ofCatalog(
                catalogName, Lists.newArrayList(Privileges.UseCatalog.allow())),
            schemaName,
            Lists.newArrayList(Privileges.UseSchema.allow()));
    SecurableObject functionObject =
        SecurableObjects.ofFunction(
            schemaObject, functionName, Lists.newArrayList(Privileges.ExecuteFunction.allow()));
    RoleEntity role =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role1",
            AUDIT_INFO,
            Lists.newArrayList(functionObject),
            ImmutableMap.of());
    RoleMetaService.getInstance().insertRole(role, false);

    // Set up tag relation
    TagEntity tag =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag1")
            .withNamespace(NamespaceUtil.ofTag(metalakeName))
            .withAuditInfo(AUDIT_INFO)
            .build();
    TagMetaService.getInstance().insertTag(tag, false);
    TagMetaService.getInstance()
        .associateTagsWithMetadataObject(
            function.nameIdentifier(),
            function.type(),
            new NameIdentifier[] {NameIdentifierUtil.ofTag(metalakeName, tag.name())},
            new NameIdentifier[0]);
    assertEquals(1, countActiveTagRelForMetadataObject(function.id(), "FUNCTION"));

    NameIdentifier functionIdent =
        NameIdentifier.of(metalakeName, catalogName, schemaName, functionName);
    assertTrue(FunctionMetaService.getInstance().deleteFunction(functionIdent));

    // Verify function is soft deleted
    assertThrows(
        NoSuchEntityException.class,
        () -> FunctionMetaService.getInstance().getFunctionByIdentifier(functionIdent));

    // Verify owner relation is cleaned up
    assertEquals(0, countActiveOwnerRelForMetadataObject(function.id(), "FUNCTION"));

    // Verify securable object (role) relation is cleaned up
    assertEquals(0, countActiveObjectRelForRole(role.id()));

    // Verify tag relation is cleaned up
    assertEquals(0, countActiveTagRelForMetadataObject(function.id(), "FUNCTION"));
  }

  @TestTemplate
  public void testDeleteNonExistentFunction() {
    NameIdentifier functionIdent =
        NameIdentifier.of(metalakeName, catalogName, schemaName, "non_existent_function");
    assertThrows(
        NoSuchEntityException.class,
        () -> FunctionMetaService.getInstance().deleteFunction(functionIdent));
  }

  @TestTemplate
  public void testMetaLifeCycleFromCreationToDeletion() throws IOException {
    String functionName = GravitinoITUtils.genRandomName("test_function");
    Namespace ns = NamespaceUtil.ofFunction(metalakeName, catalogName, schemaName);
    FunctionEntity function =
        createFunctionEntity(RandomIdGenerator.INSTANCE.nextId(), ns, functionName, AUDIT_INFO);

    FunctionMetaService.getInstance().insertFunction(function, false);

    // Update function to create version 2 in storage layer
    NameIdentifier functionIdent =
        NameIdentifier.of(metalakeName, catalogName, schemaName, functionName);
    FunctionEntity functionV2 =
        FunctionEntity.builder()
            .withId(function.id())
            .withName(function.name())
            .withNamespace(ns)
            .withComment("version 2 comment")
            .withFunctionType(function.functionType())
            .withDeterministic(function.deterministic())
            .withDefinitions(function.definitions())
            .withAuditInfo(AUDIT_INFO)
            .build();

    FunctionMetaService.getInstance().updateFunction(functionIdent, e -> functionV2);

    // Create another function in a different schema
    String anotherMetalakeName = GravitinoITUtils.genRandomName("another-metalake");
    String anotherCatalogName = GravitinoITUtils.genRandomName("another-catalog");
    String anotherSchemaName = GravitinoITUtils.genRandomName("another-schema");
    createAndInsertMakeLake(anotherMetalakeName);
    createAndInsertCatalog(anotherMetalakeName, anotherCatalogName);
    createAndInsertSchema(anotherMetalakeName, anotherCatalogName, anotherSchemaName);

    FunctionEntity anotherFunction =
        createFunctionEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFunction(anotherMetalakeName, anotherCatalogName, anotherSchemaName),
            "another_function",
            AUDIT_INFO);
    FunctionMetaService.getInstance().insertFunction(anotherFunction, false);

    // Update another function to version 2 and 3
    NameIdentifier anotherFunctionIdent =
        NameIdentifier.of(
            anotherMetalakeName, anotherCatalogName, anotherSchemaName, "another_function");
    Namespace anotherNs =
        NamespaceUtil.ofFunction(anotherMetalakeName, anotherCatalogName, anotherSchemaName);
    FunctionEntity anotherFunctionV2 =
        FunctionEntity.builder()
            .withId(anotherFunction.id())
            .withName(anotherFunction.name())
            .withNamespace(anotherNs)
            .withComment("another function v2")
            .withFunctionType(anotherFunction.functionType())
            .withDeterministic(anotherFunction.deterministic())
            .withDefinitions(anotherFunction.definitions())
            .withAuditInfo(AUDIT_INFO)
            .build();
    FunctionMetaService.getInstance().updateFunction(anotherFunctionIdent, e -> anotherFunctionV2);

    FunctionEntity anotherFunctionV3 =
        FunctionEntity.builder()
            .withId(anotherFunction.id())
            .withName(anotherFunction.name())
            .withNamespace(anotherNs)
            .withComment("another function v3")
            .withFunctionType(anotherFunction.functionType())
            .withDeterministic(anotherFunction.deterministic())
            .withDefinitions(anotherFunction.definitions())
            .withAuditInfo(AUDIT_INFO)
            .build();
    FunctionMetaService.getInstance().updateFunction(anotherFunctionIdent, e -> anotherFunctionV3);

    // Verify list functions
    List<FunctionEntity> functions = FunctionMetaService.getInstance().listFunctionsByNamespace(ns);
    assertEquals(1, functions.size());
    assertEquals(functionV2.name(), functions.get(0).name());

    // Soft delete metalake (cascading delete)
    backend.delete(NameIdentifierUtil.ofMetalake(metalakeName), Entity.EntityType.METALAKE, true);

    // Verify function is deleted in the deleted metalake
    assertThrows(
        NoSuchEntityException.class,
        () -> FunctionMetaService.getInstance().getFunctionByIdentifier(functionIdent));

    // Verify another function still exists
    NameIdentifier anotherFunctionIdentForVerify =
        NameIdentifier.of(
            anotherMetalakeName, anotherCatalogName, anotherSchemaName, "another_function");
    FunctionEntity loadedAnotherFunction =
        FunctionMetaService.getInstance().getFunctionByIdentifier(anotherFunctionIdentForVerify);
    assertNotNull(loadedAnotherFunction);

    // Check legacy record after soft delete
    assertTrue(legacyRecordExistsInDB(function.id(), Entity.EntityType.FUNCTION));
    assertEquals(2, listFunctionVersions(function.id()).size());
    assertEquals(3, listFunctionVersions(anotherFunction.id()).size());

    // Hard delete legacy data
    for (Entity.EntityType entityType : Entity.EntityType.values()) {
      backend.hardDeleteLegacyData(entityType, Instant.now().toEpochMilli() + 1000);
    }
    assertFalse(legacyRecordExistsInDB(function.id(), Entity.EntityType.FUNCTION));
    assertEquals(0, listFunctionVersions(function.id()).size());
    Map<Integer, Long> anotherFunctionVersionsAfterHardDelete =
        listFunctionVersions(anotherFunction.id());
    assertTrue(anotherFunctionVersionsAfterHardDelete.containsKey(3));
    assertEquals(0L, anotherFunctionVersionsAfterHardDelete.get(3));

    // Soft delete old versions
    for (Entity.EntityType entityType : Entity.EntityType.values()) {
      backend.deleteOldVersionData(entityType, 1);
    }
    Map<Integer, Long> versionDeletedMap = listFunctionVersions(anotherFunction.id());
    assertTrue(versionDeletedMap.containsKey(3));
    assertEquals(0L, versionDeletedMap.get(3));
    assertEquals(1, versionDeletedMap.values().stream().filter(value -> value == 0L).count());

    // Hard delete old versions
    backend.hardDeleteLegacyData(Entity.EntityType.FUNCTION, Instant.now().toEpochMilli() + 1000);
    Map<Integer, Long> finalFunctionVersions = listFunctionVersions(anotherFunction.id());
    assertTrue(finalFunctionVersions.containsKey(3));
    assertEquals(0L, finalFunctionVersions.get(3));
    assertEquals(1, finalFunctionVersions.values().stream().filter(value -> value == 0L).count());
  }

  @TestTemplate
  public void testDeleteFunctionVersionsByRetentionCount() throws IOException {
    String functionName = GravitinoITUtils.genRandomName("test_function");
    Namespace ns = NamespaceUtil.ofFunction(metalakeName, catalogName, schemaName);
    FunctionEntity function =
        createFunctionEntity(RandomIdGenerator.INSTANCE.nextId(), ns, functionName, AUDIT_INFO);

    FunctionMetaService.getInstance().insertFunction(function, false);

    // Create multiple versions
    NameIdentifier functionIdent =
        NameIdentifier.of(metalakeName, catalogName, schemaName, functionName);

    for (int v = 2; v <= 5; v++) {
      final int version = v;
      FunctionEntity updatedFunction =
          FunctionEntity.builder()
              .withId(function.id())
              .withName(function.name())
              .withNamespace(ns)
              .withComment("version " + version)
              .withFunctionType(function.functionType())
              .withDeterministic(function.deterministic())
              .withDefinitions(function.definitions())
              .withAuditInfo(AUDIT_INFO)
              .build();
      FunctionMetaService.getInstance().updateFunction(functionIdent, e -> updatedFunction);
    }

    // Verify all 5 versions are active before retention cleanup
    Map<Integer, Long> versionDeletedMap = listFunctionVersions(function.id());
    assertEquals(5, versionDeletedMap.size());
    for (int version = 1; version <= 5; version++) {
      assertVersionActive(versionDeletedMap, version);
    }

    // Soft delete versions by retention count (keep only 2)
    FunctionMetaService.getInstance().deleteFunctionVersionsByRetentionCount(2L, 100);

    // Verify versions 1-3 are soft deleted and versions 4-5 remain active
    versionDeletedMap = listFunctionVersions(function.id());
    assertEquals(5, versionDeletedMap.size());
    for (int version = 1; version <= 3; version++) {
      assertVersionSoftDeleted(versionDeletedMap, version);
    }
    for (int version = 4; version <= 5; version++) {
      assertVersionActive(versionDeletedMap, version);
    }
  }

  @TestTemplate
  public void testGetNonExistentFunction() {
    NameIdentifier functionIdent =
        NameIdentifier.of(metalakeName, catalogName, schemaName, "non_existent_function");
    assertThrows(
        NoSuchEntityException.class,
        () -> FunctionMetaService.getInstance().getFunctionByIdentifier(functionIdent));
  }

  @TestTemplate
  public void testInsertFunctionWithOverwrite() throws IOException {
    String functionName = GravitinoITUtils.genRandomName("test_function");
    Namespace ns = NamespaceUtil.ofFunction(metalakeName, catalogName, schemaName);
    FunctionEntity function =
        createFunctionEntity(RandomIdGenerator.INSTANCE.nextId(), ns, functionName, AUDIT_INFO);

    FunctionMetaService.getInstance().insertFunction(function, false);

    NameIdentifier functionIdent =
        NameIdentifier.of(metalakeName, catalogName, schemaName, functionName);

    // Insert with overwrite=true should succeed
    FunctionEntity newFunction =
        FunctionEntity.builder()
            .withId(function.id())
            .withName(function.name())
            .withNamespace(ns)
            .withComment("overwritten comment")
            .withFunctionType(function.functionType())
            .withDeterministic(true)
            .withDefinitions(function.definitions())
            .withAuditInfo(AUDIT_INFO)
            .build();

    FunctionMetaService.getInstance().insertFunction(newFunction, true);

    // Verify the function was updated
    FunctionEntity loadedFunction =
        FunctionMetaService.getInstance().getFunctionByIdentifier(functionIdent);
    assertEquals("overwritten comment", loadedFunction.comment());
    assertTrue(loadedFunction.deterministic());
    FunctionPO overwrittenPO =
        FunctionMetaService.getInstance().getFunctionPOByIdentifier(functionIdent);
    assertEquals(2, overwrittenPO.functionCurrentVersion());
    assertEquals(2, overwrittenPO.functionLatestVersion());
    assertEquals(2, listFunctionVersions(function.id()).size());
  }

  @TestTemplate
  public void testNaturalKeyOverwriteUsesPersistedFunctionId() throws IOException {
    String functionName = GravitinoITUtils.genRandomName("function_natural_key_overwrite");
    Namespace namespace = NamespaceUtil.ofFunction(metalakeName, catalogName, schemaName);
    FunctionEntity original =
        createFunctionEntity(
            RandomIdGenerator.INSTANCE.nextId(), namespace, functionName, AUDIT_INFO);
    FunctionMetaService.getInstance().insertFunction(original, false);
    FunctionEntity replacement =
        copyFunction(
            createFunctionEntity(
                RandomIdGenerator.INSTANCE.nextId(), namespace, functionName, AUDIT_INFO),
            functionName,
            namespace,
            "replacement");

    FunctionMetaService.getInstance().insertFunction(replacement, true);

    FunctionEntity stored =
        FunctionMetaService.getInstance().getFunctionByIdentifier(original.nameIdentifier());
    FunctionPO storedPO =
        FunctionMetaService.getInstance().getFunctionPOByIdentifier(original.nameIdentifier());
    assertEquals(original.id(), stored.id());
    assertEquals("replacement", stored.comment());
    assertEquals(2, storedPO.functionCurrentVersion());
    assertEquals(2, listFunctionVersions(original.id()).size());
    assertTrue(listFunctionVersions(replacement.id()).isEmpty());
  }

  @TestTemplate
  public void testNormalReadRequiresCurrentVersionRow() throws IOException {
    String functionName = GravitinoITUtils.genRandomName("function_missing_current_version");
    Namespace namespace = NamespaceUtil.ofFunction(metalakeName, catalogName, schemaName);
    FunctionEntity function =
        createFunctionEntity(
            RandomIdGenerator.INSTANCE.nextId(), namespace, functionName, AUDIT_INFO);
    FunctionMetaService.getInstance().insertFunction(function, false);

    SessionUtils.doWithCommit(
        FunctionVersionMetaMapper.class,
        mapper -> mapper.softDeleteFunctionVersionsByFunctionId(function.id()));

    assertThrows(
        NoSuchEntityException.class,
        () -> FunctionMetaService.getInstance().getFunctionByIdentifier(function.nameIdentifier()));
  }

  @TestTemplate
  public void testNaturalKeyOverwriteWaitsForConcurrentRename() throws Exception {
    String functionName = GravitinoITUtils.genRandomName("function_overwrite_rename_race");
    Namespace namespace = NamespaceUtil.ofFunction(metalakeName, catalogName, schemaName);
    FunctionEntity original =
        createFunctionEntity(
            RandomIdGenerator.INSTANCE.nextId(), namespace, functionName, AUDIT_INFO);
    FunctionMetaService.getInstance().insertFunction(original, false);
    FunctionPO observedPO =
        FunctionMetaService.getInstance().getFunctionPOByIdentifier(original.nameIdentifier());
    FunctionEntity renamed =
        copyFunction(original, functionName + "_winner", namespace, "rename winner");
    FunctionPO renamedPO =
        FunctionPO.buildFunctionPO(
            renamed,
            FunctionPO.builder()
                .withMetalakeId(observedPO.metalakeId())
                .withCatalogId(observedPO.catalogId())
                .withSchemaId(observedPO.schemaId())
                .withFunctionLatestVersion(2)
                .withFunctionCurrentVersion(2),
            2);
    FunctionEntity replacement =
        createFunctionEntity(
            RandomIdGenerator.INSTANCE.nextId(), namespace, functionName, AUDIT_INFO);

    CountDownLatch renameWritten = new CountDownLatch(1);
    CountDownLatch allowRenameCommit = new CountDownLatch(1);
    CountDownLatch overwriteStarted = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    Future<Throwable> renameResult =
        executor.submit(
            () -> {
              try {
                SessionUtils.doMultipleWithCommit(
                    () ->
                        assertEquals(
                            Integer.valueOf(1),
                            SessionUtils.getWithoutCommit(
                                FunctionMetaMapper.class,
                                mapper -> mapper.updateFunctionMeta(renamedPO, observedPO))),
                    () ->
                        SessionUtils.doWithoutCommit(
                            FunctionVersionMetaMapper.class,
                            mapper ->
                                mapper.insertFunctionVersionMeta(renamedPO.functionVersionPO())),
                    () -> {
                      renameWritten.countDown();
                      await(allowRenameCommit);
                    });
                return null;
              } catch (Throwable throwable) {
                return throwable;
              }
            });

    try {
      assertTrue(renameWritten.await(30, TimeUnit.SECONDS));
      Future<Throwable> overwriteResult =
          executor.submit(
              () -> {
                overwriteStarted.countDown();
                try {
                  FunctionMetaService.getInstance().insertFunction(replacement, true);
                  return null;
                } catch (Throwable throwable) {
                  return throwable;
                }
              });
      assertTrue(overwriteStarted.await(30, TimeUnit.SECONDS));
      assertThrows(TimeoutException.class, () -> overwriteResult.get(500, TimeUnit.MILLISECONDS));

      allowRenameCommit.countDown();
      assertNull(renameResult.get(30, TimeUnit.SECONDS));
      assertNull(overwriteResult.get(30, TimeUnit.SECONDS));
    } finally {
      allowRenameCommit.countDown();
      executor.shutdownNow();
    }

    assertEquals(
        original.id(),
        FunctionMetaService.getInstance().getFunctionByIdentifier(renamed.nameIdentifier()).id());
    assertEquals(
        replacement.id(),
        FunctionMetaService.getInstance()
            .getFunctionByIdentifier(replacement.nameIdentifier())
            .id());
  }

  private int countActiveOwnerRelForMetadataObject(
      Long metadataObjectId, String metadataObjectType) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                String.format(
                    "SELECT count(*) FROM owner_meta"
                        + " WHERE metadata_object_id = %d AND metadata_object_type = '%s'"
                        + " AND deleted_at = 0",
                    metadataObjectId, metadataObjectType))) {
      if (rs.next()) {
        return rs.getInt(1);
      }
      throw new RuntimeException("No result for countActiveOwnerRelForMetadataObject");
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
  }

  private int countActiveObjectRelForRole(Long roleId) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                String.format(
                    "SELECT count(*) FROM role_meta_securable_object"
                        + " WHERE role_id = %d AND deleted_at = 0",
                    roleId))) {
      if (rs.next()) {
        return rs.getInt(1);
      }
      throw new RuntimeException("No result for countActiveObjectRelForRole");
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
  }

  private int countActiveTagRelForMetadataObject(Long metadataObjectId, String metadataObjectType) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                String.format(
                    "SELECT count(*) FROM tag_relation_meta"
                        + " WHERE metadata_object_id = %d AND metadata_object_type = '%s'"
                        + " AND deleted_at = 0",
                    metadataObjectId, metadataObjectType))) {
      if (rs.next()) {
        return rs.getInt(1);
      }
      throw new RuntimeException("No result for countActiveTagRelForMetadataObject");
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
  }

  private Map<Integer, Long> listFunctionVersions(Long functionId) {
    Map<Integer, Long> versionDeletedTime = new HashMap<>();
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                String.format(
                    "SELECT version, deleted_at FROM function_version_info WHERE function_id = %d",
                    functionId))) {
      while (rs.next()) {
        versionDeletedTime.put(rs.getInt("version"), rs.getLong("deleted_at"));
      }
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
    return versionDeletedTime;
  }

  private FunctionEntity copyFunctionWithComment(FunctionEntity function, String comment) {
    return copyFunction(function, function.name(), function.namespace(), comment);
  }

  private FunctionEntity copyFunction(
      FunctionEntity function, String name, Namespace namespace, String comment) {
    return FunctionEntity.builder()
        .withId(function.id())
        .withName(name)
        .withNamespace(namespace)
        .withComment(comment)
        .withFunctionType(function.functionType())
        .withDeterministic(function.deterministic())
        .withDefinitions(function.definitions())
        .withAuditInfo(function.auditInfo())
        .build();
  }

  private void await(CountDownLatch latch) {
    try {
      assertTrue(latch.await(30, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private void assertVersionActive(Map<Integer, Long> versionDeletedMap, int version) {
    assertTrue(versionDeletedMap.containsKey(version));
    assertEquals(0L, versionDeletedMap.get(version));
  }

  private void assertVersionSoftDeleted(Map<Integer, Long> versionDeletedMap, int version) {
    assertTrue(versionDeletedMap.containsKey(version));
    assertTrue(versionDeletedMap.get(version) > 0L);
  }
}
