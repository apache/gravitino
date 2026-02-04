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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionDefinitions;
import org.apache.gravitino.function.FunctionImpl;
import org.apache.gravitino.function.FunctionImpls;
import org.apache.gravitino.function.FunctionParam;
import org.apache.gravitino.function.FunctionParams;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.FunctionEntity;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

@SuppressWarnings("deprecation")
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
  public void testDeleteFunction() throws IOException {
    String functionName = GravitinoITUtils.genRandomName("test_function");
    Namespace ns = NamespaceUtil.ofFunction(metalakeName, catalogName, schemaName);
    FunctionEntity function =
        createFunctionEntity(RandomIdGenerator.INSTANCE.nextId(), ns, functionName, AUDIT_INFO);

    FunctionMetaService.getInstance().insertFunction(function, false);

    NameIdentifier functionIdent =
        NameIdentifier.of(metalakeName, catalogName, schemaName, functionName);
    assertTrue(FunctionMetaService.getInstance().deleteFunction(functionIdent));

    // Verify function is soft deleted
    assertThrows(
        NoSuchEntityException.class,
        () -> FunctionMetaService.getInstance().getFunctionByIdentifier(functionIdent));
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
    assertEquals(3, listFunctionVersions(anotherFunction.id()).size());

    // Soft delete old versions
    for (Entity.EntityType entityType : Entity.EntityType.values()) {
      backend.deleteOldVersionData(entityType, 1);
    }
    Map<Integer, Long> versionDeletedMap = listFunctionVersions(anotherFunction.id());
    assertEquals(3, versionDeletedMap.size());
    assertEquals(1, versionDeletedMap.values().stream().filter(value -> value == 0L).count());
    assertEquals(2, versionDeletedMap.values().stream().filter(value -> value != 0L).count());

    // Hard delete old versions
    backend.hardDeleteLegacyData(Entity.EntityType.FUNCTION, Instant.now().toEpochMilli() + 1000);
    assertEquals(1, listFunctionVersions(anotherFunction.id()).size());
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

    // Verify all 5 versions exist
    assertEquals(5, listFunctionVersions(function.id()).size());

    // Soft delete versions by retention count (keep only 2)
    FunctionMetaService.getInstance().deleteFunctionVersionsByRetentionCount(2L, 100);

    // Verify 3 versions are soft deleted
    Map<Integer, Long> versionDeletedMap = listFunctionVersions(function.id());
    assertEquals(5, versionDeletedMap.size());
    assertEquals(2, versionDeletedMap.values().stream().filter(value -> value == 0L).count());
    assertEquals(3, versionDeletedMap.values().stream().filter(value -> value != 0L).count());
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
  }

  private FunctionEntity createFunctionEntity(
      Long id, Namespace namespace, String name, AuditInfo auditInfo) {
    FunctionParam param1 = FunctionParams.of("param1", Types.IntegerType.get());
    FunctionParam param2 = FunctionParams.of("param2", Types.StringType.get());
    FunctionImpl impl = FunctionImpls.ofSql(FunctionImpl.RuntimeType.SPARK, "SELECT param1 + 1");
    FunctionDefinition definition =
        FunctionDefinitions.of(
            new FunctionParam[] {param1, param2},
            Types.IntegerType.get(),
            new FunctionImpl[] {impl});

    return FunctionEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withComment("test function comment")
        .withFunctionType(FunctionType.SCALAR)
        .withDeterministic(false)
        .withDefinitions(new FunctionDefinition[] {definition})
        .withAuditInfo(auditInfo)
        .build();
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
}
