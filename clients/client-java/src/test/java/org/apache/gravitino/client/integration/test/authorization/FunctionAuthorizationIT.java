/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.client.integration.test.authorization;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionCatalog;
import org.apache.gravitino.function.FunctionChange;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionDefinitions;
import org.apache.gravitino.function.FunctionImpl;
import org.apache.gravitino.function.FunctionImpls;
import org.apache.gravitino.function.FunctionParam;
import org.apache.gravitino.function.FunctionParams;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@Tag("gravitino-docker-test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class FunctionAuthorizationIT extends BaseRestApiAuthorizationIT {

  private static final String CATALOG = "catalog";

  private static final String SCHEMA = "schema";

  private static final String ROLE = "role";

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    super.startIntegrationTest();
    client
        .loadMetalake(METALAKE)
        .createCatalog(CATALOG, Catalog.Type.MODEL, "model", "comment", new HashMap<>())
        .asSchemas()
        .createSchema(SCHEMA, "test", new HashMap<>());

    // grant tester USE_CATALOG so the normal user can at least reach the catalog
    List<SecurableObject> securableObjects = new ArrayList<>();
    GravitinoMetalake metalake = client.loadMetalake(METALAKE);
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(CATALOG, ImmutableList.of(Privileges.UseCatalog.allow()));
    securableObjects.add(catalogObject);
    metalake.createRole(ROLE, new HashMap<>(), securableObjects);
    metalake.grantRolesToUser(ImmutableList.of(ROLE), NORMAL_USER);
  }

  @Test
  @Order(1)
  public void testRegisterFunction() {
    FunctionCatalog functionCatalog =
        client.loadMetalake(METALAKE).loadCatalog(CATALOG).asFunctionCatalog();
    functionCatalog.registerFunction(
        NameIdentifier.of(SCHEMA, "func1"), "", FunctionType.SCALAR, true, newDefinitions());

    FunctionCatalog normalUserCatalog =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG).asFunctionCatalog();

    // normal user cannot register without REGISTER_FUNCTION
    assertThrows(
        ForbiddenException.class,
        () ->
            normalUserCatalog.registerFunction(
                NameIdentifier.of(SCHEMA, "func2"),
                "",
                FunctionType.SCALAR,
                true,
                newDefinitions()));

    // normal user cannot list functions without USE_SCHEMA privilege
    assertThrows(
        ForbiddenException.class, () -> normalUserCatalog.listFunctions(Namespace.of(SCHEMA)));

    GravitinoMetalake metalake = client.loadMetalake(METALAKE);

    // Grant REGISTER_FUNCTION + USE_SCHEMA on the catalog
    metalake.grantPrivilegesToRole(
        ROLE,
        MetadataObjects.of(null, CATALOG, MetadataObject.Type.CATALOG),
        ImmutableList.of(Privileges.UseSchema.allow(), Privileges.RegisterFunction.allow()));
    normalUserCatalog.registerFunction(
        NameIdentifier.of(SCHEMA, "func2"), "", FunctionType.SCALAR, true, newDefinitions());

    // Revoke REGISTER_FUNCTION — normal user should no longer register
    metalake.revokePrivilegesFromRole(
        ROLE,
        MetadataObjects.of(null, CATALOG, MetadataObject.Type.CATALOG),
        ImmutableSet.of(Privileges.RegisterFunction.allow()));
    assertThrows(
        ForbiddenException.class,
        () ->
            normalUserCatalog.registerFunction(
                NameIdentifier.of(SCHEMA, "func3"),
                "",
                FunctionType.SCALAR,
                true,
                newDefinitions()));
  }

  @Test
  @Order(2)
  public void testListFunction() {
    FunctionCatalog normalUserCatalog =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG).asFunctionCatalog();
    assertFunctionNames(normalUserCatalog.listFunctions(Namespace.of(SCHEMA)), "func2");

    FunctionCatalog adminCatalog =
        client.loadMetalake(METALAKE).loadCatalog(CATALOG).asFunctionCatalog();
    assertFunctionNames(adminCatalog.listFunctions(Namespace.of(SCHEMA)), "func1", "func2");

    GravitinoMetalake metalake = client.loadMetalake(METALAKE);
    metalake.grantPrivilegesToRole(
        ROLE,
        MetadataObjects.of(CATALOG, SCHEMA, MetadataObject.Type.SCHEMA),
        ImmutableList.of(Privileges.ExecuteFunction.allow()));
    assertFunctionNames(normalUserCatalog.listFunctions(Namespace.of(SCHEMA)), "func1", "func2");

    metalake.revokePrivilegesFromRole(
        ROLE,
        MetadataObjects.of(CATALOG, SCHEMA, MetadataObject.Type.SCHEMA),
        ImmutableSet.of(Privileges.ExecuteFunction.allow()));
    assertFunctionNames(normalUserCatalog.listFunctions(Namespace.of(SCHEMA)), "func2");
  }

  @Test
  @Order(3)
  public void testGetFunction() {
    FunctionCatalog normalUserCatalog =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG).asFunctionCatalog();
    // cannot load func1 without EXECUTE_FUNCTION / MODIFY_FUNCTION / ownership
    assertThrows(
        ForbiddenException.class,
        () -> normalUserCatalog.getFunction(NameIdentifier.of(SCHEMA, "func1")));

    // Grant EXECUTE_FUNCTION on schema
    GravitinoMetalake metalake = client.loadMetalake(METALAKE);
    metalake.grantPrivilegesToRole(
        ROLE,
        MetadataObjects.of(CATALOG, SCHEMA, MetadataObject.Type.SCHEMA),
        ImmutableList.of(Privileges.ExecuteFunction.allow()));
    Function func = normalUserCatalog.getFunction(NameIdentifier.of(SCHEMA, "func1"));
    assertEquals("func1", func.name());

    // revoke and confirm
    metalake.revokePrivilegesFromRole(
        ROLE,
        MetadataObjects.of(CATALOG, SCHEMA, MetadataObject.Type.SCHEMA),
        ImmutableSet.of(Privileges.ExecuteFunction.allow()));
    assertThrows(
        ForbiddenException.class,
        () -> normalUserCatalog.getFunction(NameIdentifier.of(SCHEMA, "func1")));

    metalake.grantPrivilegesToRole(
        ROLE,
        MetadataObjects.of(
            ImmutableList.of(CATALOG, SCHEMA, "func1"), MetadataObject.Type.FUNCTION),
        ImmutableList.of(Privileges.ModifyFunction.allow()));
    func = normalUserCatalog.getFunction(NameIdentifier.of(SCHEMA, "func1"));
    assertEquals("func1", func.name());

    metalake.revokePrivilegesFromRole(
        ROLE,
        MetadataObjects.of(
            ImmutableList.of(CATALOG, SCHEMA, "func1"), MetadataObject.Type.FUNCTION),
        ImmutableSet.of(Privileges.ModifyFunction.allow()));
    assertThrows(
        ForbiddenException.class,
        () -> normalUserCatalog.getFunction(NameIdentifier.of(SCHEMA, "func1")));
  }

  @Test
  @Order(4)
  public void testAlterFunction() {
    FunctionCatalog normalUserCatalog =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG).asFunctionCatalog();
    // cannot alter func1 (owned by admin) without MODIFY_FUNCTION or ownership
    assertThrows(
        ForbiddenException.class,
        () ->
            normalUserCatalog.alterFunction(
                NameIdentifier.of(SCHEMA, "func1"), FunctionChange.updateComment("new-comment")));

    // Grant MODIFY_FUNCTION at function level
    GravitinoMetalake metalake = client.loadMetalake(METALAKE);
    metalake.grantPrivilegesToRole(
        ROLE,
        MetadataObjects.of(
            ImmutableList.of(CATALOG, SCHEMA, "func1"), MetadataObject.Type.FUNCTION),
        ImmutableList.of(Privileges.ModifyFunction.allow()));
    Function altered =
        normalUserCatalog.alterFunction(
            NameIdentifier.of(SCHEMA, "func1"), FunctionChange.updateComment("new-comment"));
    assertEquals("new-comment", altered.comment());

    // Revoke MODIFY_FUNCTION
    metalake.revokePrivilegesFromRole(
        ROLE,
        MetadataObjects.of(
            ImmutableList.of(CATALOG, SCHEMA, "func1"), MetadataObject.Type.FUNCTION),
        ImmutableSet.of(Privileges.ModifyFunction.allow()));
    assertThrows(
        ForbiddenException.class,
        () ->
            normalUserCatalog.alterFunction(
                NameIdentifier.of(SCHEMA, "func1"),
                FunctionChange.updateComment("another-comment")));
  }

  @Test
  @Order(5)
  public void testDropFunction() {
    FunctionCatalog adminCatalog =
        client.loadMetalake(METALAKE).loadCatalog(CATALOG).asFunctionCatalog();
    adminCatalog.registerFunction(
        NameIdentifier.of(SCHEMA, "func_to_drop"), "", FunctionType.SCALAR, true, newDefinitions());

    FunctionCatalog normalUserCatalog =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG).asFunctionCatalog();
    // MODIFY_FUNCTION alone does not grant DROP — only OWNER can drop
    GravitinoMetalake metalake = client.loadMetalake(METALAKE);
    metalake.grantPrivilegesToRole(
        ROLE,
        MetadataObjects.of(
            ImmutableList.of(CATALOG, SCHEMA, "func_to_drop"), MetadataObject.Type.FUNCTION),
        ImmutableList.of(Privileges.ModifyFunction.allow()));
    assertThrows(
        ForbiddenException.class,
        () -> normalUserCatalog.dropFunction(NameIdentifier.of(SCHEMA, "func_to_drop")));

    // Normal user can drop functions they own (func2 registered by them in test 1)
    assertEquals(true, normalUserCatalog.dropFunction(NameIdentifier.of(SCHEMA, "func2")));

    // Admin can drop func1 and func_to_drop
    assertEquals(true, adminCatalog.dropFunction(NameIdentifier.of(SCHEMA, "func1")));
    assertEquals(true, adminCatalog.dropFunction(NameIdentifier.of(SCHEMA, "func_to_drop")));
  }

  private static FunctionDefinition[] newDefinitions() {
    FunctionParam param = FunctionParams.of("x", Types.IntegerType.get());
    FunctionImpl impl = FunctionImpls.ofSql(FunctionImpl.RuntimeType.SPARK, "SELECT x + 1");
    return new FunctionDefinition[] {
      FunctionDefinitions.of(
          new FunctionParam[] {param}, Types.IntegerType.get(), new FunctionImpl[] {impl})
    };
  }

  private static void assertFunctionNames(NameIdentifier[] functions, String... expectedNames) {
    List<String> actualNames =
        Arrays.stream(functions).map(NameIdentifier::name).sorted().collect(Collectors.toList());
    List<String> expectedFunctionNames = Arrays.asList(expectedNames);
    Collections.sort(expectedFunctionNames);
    assertEquals(expectedFunctionNames, actualNames);
  }
}
