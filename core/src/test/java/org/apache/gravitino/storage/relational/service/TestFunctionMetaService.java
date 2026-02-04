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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
            .withReturnType(function.returnType())
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
        FunctionDefinitions.of(new FunctionParam[] {param1, param2}, new FunctionImpl[] {impl});

    return FunctionEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withComment("test function comment")
        .withFunctionType(FunctionType.SCALAR)
        .withDeterministic(false)
        .withReturnType(Types.IntegerType.get())
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
