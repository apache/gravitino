/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.lance.service.authorization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Set;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.lance.service.authorization.annotations.LanceRootNamespace;
import org.apache.gravitino.lance.service.rest.LanceNamespaceOperations;
import org.apache.gravitino.lance.service.rest.LanceTableOperations;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lance.namespace.model.AlterTableAlterColumnsRequest;
import org.lance.namespace.model.AlterTableDropColumnsRequest;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.DeclareTableRequest;
import org.lance.namespace.model.DeregisterTableRequest;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DropNamespaceRequest;
import org.lance.namespace.model.DropTableRequest;
import org.lance.namespace.model.ErrorResponse;
import org.lance.namespace.model.RegisterTableRequest;
import org.lance.namespace.model.TableExistsRequest;
import org.mockito.MockedStatic;

/** Tests the Lance-specific target resolution and response mapping around the shared pipeline. */
class TestLanceMetadataAuthorizationMethodInterceptor {

  private static final String METALAKE = "test_metalake";
  private static final String CATALOG = "test_catalog";
  private static final String SCHEMA = "test_schema";
  private static final String TABLE = "test_table";
  private static final String PROCEEDED = "PROCEEDED";

  private GravitinoAuthorizer authorizer;
  private LanceMetadataAuthorizationMethodInterceptor interceptor;
  private MockedStatic<PrincipalUtils> principalUtils;
  private MockedStatic<AuthorizationUtils> authorizationUtils;
  private MockedStatic<GravitinoAuthorizerProvider> authorizerProvider;

  @BeforeEach
  void setUp() {
    authorizer = mock(GravitinoAuthorizer.class);
    GravitinoAuthorizerProvider provider = mock(GravitinoAuthorizerProvider.class);
    principalUtils = mockStatic(PrincipalUtils.class);
    authorizationUtils = mockStatic(AuthorizationUtils.class);
    authorizerProvider = mockStatic(GravitinoAuthorizerProvider.class);

    principalUtils
        .when(PrincipalUtils::getCurrentPrincipal)
        .thenReturn(new UserPrincipal("tester"));
    principalUtils.when(PrincipalUtils::getCurrentUserName).thenReturn("tester");
    authorizerProvider.when(GravitinoAuthorizerProvider::getInstance).thenReturn(provider);
    when(provider.getGravitinoAuthorizer()).thenReturn(authorizer);
    when(authorizer.deny(any(), any(), any(), any(), any())).thenReturn(false);
    when(authorizer.isOwner(any(), any(), any(), any())).thenReturn(false);
    when(authorizer.findUnheldRoles(any(), any(), any(), any())).thenReturn(Set.of());
    interceptor = new LanceMetadataAuthorizationMethodInterceptor(METALAKE);
  }

  @AfterEach
  void tearDown() {
    authorizerProvider.close();
    authorizationUtils.close();
    principalUtils.close();
  }

  @Test
  void testCreateSchemaPrivilegeAllowsProbeButNotDescribe() throws Throwable {
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.CREATE_SCHEMA);

    assertEquals(
        PROCEEDED,
        interceptor.invoke(invocation(namespaceMethod("namespaceExists"), CATALOG, "$")));
    assertEquals(
        PROCEEDED,
        interceptor.invoke(
            invocation(namespaceMethod("namespaceExists"), CATALOG + "." + SCHEMA, ".")));

    Object describe =
        interceptor.invoke(
            invocation(namespaceMethod("describeNamespace"), CATALOG + "." + SCHEMA, "."));
    assertErrorResponse(describe, Response.Status.FORBIDDEN);
  }

  @Test
  void testDenyOverridesSchemaProbePrivileges() throws Throwable {
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.CREATE_SCHEMA);
    when(authorizer.deny(any(), any(), any(), any(), any())).thenReturn(true);

    Object result =
        interceptor.invoke(
            invocation(namespaceMethod("namespaceExists"), CATALOG + "$" + SCHEMA, "$"));
    assertErrorResponse(result, Response.Status.FORBIDDEN);
  }

  @Test
  void testRootValidatesUserButSkipsPrivilegeExpression() throws Throwable {
    Method rootMethod =
        LanceNamespaceOperations.class.getMethod(
            "listNamespacesOnRoot", String.class, String.class, Integer.class);
    MethodInvocation invocation = invocation(rootMethod, "$", null, null);
    assertEquals(PROCEEDED, interceptor.invoke(invocation));
    verify(authorizer, never()).authorize(any(), any(), any(), any(), any());
  }

  @Test
  void testProtocolAndOperationFailuresUseLanceResponses() throws Throwable {
    // A namespace operation stops at a schema, so a table identifier is not merely unauthorized
    // there, it is not a namespace at all and is rejected before any expression is evaluated.
    MethodInvocation invalid =
        invocation(namespaceMethod("describeNamespace"), CATALOG + "$" + SCHEMA + "$extra", "$");
    assertErrorResponse(interceptor.invoke(invalid), Response.Status.BAD_REQUEST);
    verify(invalid, never()).proceed();

    MethodInvocation empty = invocation(namespaceMethod("describeNamespace"), "", "$");
    assertErrorResponse(interceptor.invoke(empty), Response.Status.BAD_REQUEST);

    MethodInvocation missing =
        invocation(TestOperations.class.getMethod("missingTarget", String.class), "$");
    assertErrorResponse(interceptor.invoke(missing), Response.Status.INTERNAL_SERVER_ERROR);

    MethodInvocation conflicting =
        invocation(TestOperations.class.getMethod("conflictingRoot", String.class), CATALOG);
    assertErrorResponse(interceptor.invoke(conflicting), Response.Status.INTERNAL_SERVER_ERROR);

    MethodInvocation operation =
        invocation(TestOperations.class.getMethod("unannotated"), new Object[0]);
    when(operation.proceed()).thenThrow(new IllegalArgumentException("bad request"));
    assertErrorResponse(interceptor.invoke(operation), Response.Status.BAD_REQUEST);
  }

  @Test
  void testCreateNamespaceRequiresCreatePrivilegeAtEachLevel() throws Throwable {
    allow(Privilege.Name.CREATE_CATALOG);
    assertEquals(PROCEEDED, interceptor.invoke(createInvocation(CATALOG, "$", "create")));
    assertErrorResponse(
        interceptor.invoke(createInvocation(CATALOG + "$" + SCHEMA, "$", "create")),
        Response.Status.FORBIDDEN);

    allow(Privilege.Name.USE_CATALOG, Privilege.Name.CREATE_SCHEMA);
    assertEquals(
        PROCEEDED, interceptor.invoke(createInvocation(CATALOG + "$" + SCHEMA, "$", "create")));
    assertErrorResponse(
        interceptor.invoke(createInvocation(CATALOG, "$", "create")), Response.Status.FORBIDDEN);
  }

  @Test
  void testCreatePrivilegeCannotOverwriteAnExistingNamespace() throws Throwable {
    allow(Privilege.Name.CREATE_CATALOG, Privilege.Name.USE_CATALOG, Privilege.Name.CREATE_SCHEMA);

    // The same privileges that authorize a create must not authorize an overwrite, which replaces
    // the properties of a namespace the caller does not own.
    assertEquals(PROCEEDED, interceptor.invoke(createInvocation(CATALOG, "$", "exist_ok")));
    assertErrorResponse(
        interceptor.invoke(createInvocation(CATALOG, "$", "overwrite")), Response.Status.FORBIDDEN);
    assertErrorResponse(
        interceptor.invoke(createInvocation(CATALOG + "$" + SCHEMA, "$", "OVERWRITE")),
        Response.Status.FORBIDDEN);
  }

  @Test
  void testOverwriteIsRecognizedWhateverSpacingTheClientSends() throws Throwable {
    allow(Privilege.Name.CREATE_CATALOG, Privilege.Name.USE_CATALOG, Privilege.Name.CREATE_SCHEMA);

    // The create operation reads the mode through CommonUtil.normalizeToken, which trims and
    // upper-cases, so all of these reach it as OVERWRITE. Authorization has to read the mode the
    // same way: a token this interceptor fails to recognize is authorized as a plain create, and
    // the create privileges above would then be enough to replace a namespace owned by somebody
    // else.
    for (String mode : new String[] {" overwrite", "overwrite ", " OverWrite ", "\toverwrite\n"}) {
      assertErrorResponse(
          interceptor.invoke(createInvocation(CATALOG, "$", mode)),
          Response.Status.FORBIDDEN,
          "mode '" + mode + "' must be authorized as an overwrite");
      assertErrorResponse(
          interceptor.invoke(createInvocation(CATALOG + "$" + SCHEMA, "$", mode)),
          Response.Status.FORBIDDEN,
          "mode '" + mode + "' must be authorized as an overwrite");
    }
  }

  @Test
  void testOwnerMayOverwriteAndDropNamespaces() throws Throwable {
    when(authorizer.isOwner(any(), any(), any(), any())).thenReturn(true);
    allow(Privilege.Name.USE_CATALOG);

    assertEquals(PROCEEDED, interceptor.invoke(createInvocation(CATALOG, "$", "overwrite")));
    assertEquals(
        PROCEEDED, interceptor.invoke(createInvocation(CATALOG + "$" + SCHEMA, "$", "overwrite")));
    assertEquals(PROCEEDED, interceptor.invoke(dropInvocation(CATALOG, "$")));
    assertEquals(PROCEEDED, interceptor.invoke(dropInvocation(CATALOG + "$" + SCHEMA, "$")));
  }

  @Test
  void testDropNamespaceRequiresOwnership() throws Throwable {
    allow(
        Privilege.Name.USE_CATALOG,
        Privilege.Name.CREATE_CATALOG,
        Privilege.Name.CREATE_SCHEMA,
        Privilege.Name.USE_SCHEMA);

    MethodInvocation dropCatalog = dropInvocation(CATALOG, "$");
    assertErrorResponse(interceptor.invoke(dropCatalog), Response.Status.FORBIDDEN);
    verify(dropCatalog, never()).proceed();

    MethodInvocation dropSchema = dropInvocation(CATALOG + "$" + SCHEMA, "$");
    assertErrorResponse(interceptor.invoke(dropSchema), Response.Status.FORBIDDEN);
    verify(dropSchema, never()).proceed();
  }

  @Test
  void testTableReadRequiresTablePrivileges() throws Throwable {
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.SELECT_TABLE);
    assertEquals(PROCEEDED, interceptor.invoke(describeTableInvocation(tableId())));
    assertEquals(PROCEEDED, interceptor.invoke(tableExistsInvocation(tableId())));

    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.MODIFY_TABLE);
    assertEquals(PROCEEDED, interceptor.invoke(describeTableInvocation(tableId())));

    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA);
    MethodInvocation describe = describeTableInvocation(tableId());
    assertErrorResponse(interceptor.invoke(describe), Response.Status.FORBIDDEN);
    verify(describe, never()).proceed();
  }

  @Test
  void testCreateTablePrivilegeProbesButDoesNotRead() throws Throwable {
    // Clients probe for a table right before creating it, so CREATE_TABLE authorizes the probe.
    // It must not expose the table schema or properties through describe.
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.CREATE_TABLE);
    assertEquals(PROCEEDED, interceptor.invoke(tableExistsInvocation(tableId())));
    assertErrorResponse(
        interceptor.invoke(describeTableInvocation(tableId())), Response.Status.FORBIDDEN);
  }

  @Test
  void testProbeTableLikePrivilegeProbesButDoesNotRead() throws Throwable {
    // PROBE_TABLE_LIKE is the privilege that means "may learn whether this exists", so it carries
    // the existence endpoint. It must not reach describe, which returns the table itself.
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.PROBE_TABLE_LIKE);
    assertEquals(PROCEEDED, interceptor.invoke(tableExistsInvocation(tableId())));
    assertErrorResponse(
        interceptor.invoke(describeTableInvocation(tableId())), Response.Status.FORBIDDEN);
  }

  @Test
  void testDenyOverridesTablePrivileges() throws Throwable {
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.SELECT_TABLE);
    when(authorizer.deny(any(), any(), any(), any(), any())).thenReturn(true);

    assertErrorResponse(
        interceptor.invoke(describeTableInvocation(tableId())), Response.Status.FORBIDDEN);
    assertErrorResponse(
        interceptor.invoke(tableExistsInvocation(tableId())), Response.Status.FORBIDDEN);
  }

  @Test
  void testTableExpressionsRejectIdentifiersOfTheWrongDepth() throws Throwable {
    when(authorizer.isOwner(any(), any(), any(), any())).thenReturn(true);
    allow(Privilege.Name.values());

    // A catalog or schema identifier must not be authorized as if it addressed a table, on either
    // the read expression or the wider probe expression.
    assertErrorResponse(
        interceptor.invoke(describeTableInvocation(CATALOG)), Response.Status.FORBIDDEN);
    assertErrorResponse(
        interceptor.invoke(describeTableInvocation(CATALOG + "$" + SCHEMA)),
        Response.Status.FORBIDDEN);
    assertErrorResponse(
        interceptor.invoke(tableExistsInvocation(CATALOG)), Response.Status.FORBIDDEN);
    assertErrorResponse(
        interceptor.invoke(tableExistsInvocation(CATALOG + "$" + SCHEMA)),
        Response.Status.FORBIDDEN);
    assertErrorResponse(
        interceptor.invoke(describeTableInvocation(tableId() + "$extra")),
        Response.Status.BAD_REQUEST);

    // The bound is per resource: the namespace resource does not accept a table identifier at all,
    // so listTables rejects it rather than authorizing it against the table it names.
    assertErrorResponse(
        interceptor.invoke(listTablesInvocation(tableId())), Response.Status.BAD_REQUEST);

    assertEquals(PROCEEDED, interceptor.invoke(listTablesInvocation(CATALOG + "$" + SCHEMA)));
  }

  @Test
  void testTableCreationRequiresCreateTablePrivilege() throws Throwable {
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.CREATE_TABLE);
    assertEquals(PROCEEDED, interceptor.invoke(createTableInvocation(tableId(), "create")));
    assertEquals(PROCEEDED, interceptor.invoke(declareTableInvocation(tableId())));
    assertEquals(PROCEEDED, interceptor.invoke(registerTableInvocation(tableId(), "create")));

    // Reading a table is not creating one.
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.SELECT_TABLE);
    MethodInvocation create = createTableInvocation(tableId(), "create");
    assertErrorResponse(interceptor.invoke(create), Response.Status.FORBIDDEN);
    verify(create, never()).proceed();
    assertErrorResponse(
        interceptor.invoke(declareTableInvocation(tableId())), Response.Status.FORBIDDEN);
    assertErrorResponse(
        interceptor.invoke(registerTableInvocation(tableId(), "create")),
        Response.Status.FORBIDDEN);
  }

  @Test
  void testCreateTablePrivilegeCannotOverwriteAnExistingTable() throws Throwable {
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.CREATE_TABLE);

    assertEquals(PROCEEDED, interceptor.invoke(createTableInvocation(tableId(), "exist_ok")));
    MethodInvocation overwrite = createTableInvocation(tableId(), "overwrite");
    assertErrorResponse(interceptor.invoke(overwrite), Response.Status.FORBIDDEN);
    verify(overwrite, never()).proceed();
    assertErrorResponse(
        interceptor.invoke(registerTableInvocation(tableId(), "OVERWRITE")),
        Response.Status.FORBIDDEN);
  }

  @Test
  void testTableOverwriteRejectsIdentifiersOfTheWrongDepth() throws Throwable {
    when(authorizer.isOwner(any(), any(), any(), any())).thenReturn(true);
    allow(Privilege.Name.values());

    // Overwrite authorization must retain the table operation's target type. Deriving the
    // expression from the identifier depth would authorize these as catalog or schema overwrites.
    assertErrorResponse(
        interceptor.invoke(createTableInvocation(CATALOG, "overwrite")), Response.Status.FORBIDDEN);
    assertErrorResponse(
        interceptor.invoke(createTableInvocation(CATALOG + "$" + SCHEMA, "overwrite")),
        Response.Status.FORBIDDEN);
    assertErrorResponse(
        interceptor.invoke(registerTableInvocation(CATALOG, "overwrite")),
        Response.Status.FORBIDDEN);
    assertErrorResponse(
        interceptor.invoke(registerTableInvocation(CATALOG + "$" + SCHEMA, "overwrite")),
        Response.Status.FORBIDDEN);
  }

  @Test
  void testModifyTablePrivilegeAndOwnershipAuthorizeOverwrite() throws Throwable {
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.MODIFY_TABLE);
    assertEquals(PROCEEDED, interceptor.invoke(createTableInvocation(tableId(), "overwrite")));
    assertEquals(PROCEEDED, interceptor.invoke(registerTableInvocation(tableId(), "overwrite")));

    when(authorizer.isOwner(any(), any(), any(), any())).thenReturn(true);
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA);
    assertEquals(PROCEEDED, interceptor.invoke(createTableInvocation(tableId(), "overwrite")));
  }

  @Test
  void testColumnMutationRequiresModifyTable() throws Throwable {
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.MODIFY_TABLE);
    assertEquals(PROCEEDED, interceptor.invoke(dropColumnsInvocation(tableId())));
    assertEquals(PROCEEDED, interceptor.invoke(alterColumnsInvocation(tableId())));

    // Reading a table does not authorize changing its columns.
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.SELECT_TABLE);
    MethodInvocation dropColumns = dropColumnsInvocation(tableId());
    assertErrorResponse(interceptor.invoke(dropColumns), Response.Status.FORBIDDEN);
    verify(dropColumns, never()).proceed();
    MethodInvocation alterColumns = alterColumnsInvocation(tableId());
    assertErrorResponse(interceptor.invoke(alterColumns), Response.Status.FORBIDDEN);
    verify(alterColumns, never()).proceed();
  }

  @Test
  void testExplicitDenyOverridesModifyTable() throws Throwable {
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.MODIFY_TABLE);
    doAnswer(
            invocation ->
                Privilege.Name.MODIFY_TABLE.equals(invocation.<Privilege.Name>getArgument(3)))
        .when(authorizer)
        .deny(any(), any(), any(), any(), any());

    MethodInvocation dropColumns = dropColumnsInvocation(tableId());
    assertErrorResponse(interceptor.invoke(dropColumns), Response.Status.FORBIDDEN);
    verify(dropColumns, never()).proceed();
    MethodInvocation alterColumns = alterColumnsInvocation(tableId());
    assertErrorResponse(interceptor.invoke(alterColumns), Response.Status.FORBIDDEN);
    verify(alterColumns, never()).proceed();
  }

  @Test
  void testRemovingATableRequiresOwnership() throws Throwable {
    // MODIFY_TABLE alters a table but never removes it.
    allow(
        Privilege.Name.USE_CATALOG,
        Privilege.Name.USE_SCHEMA,
        Privilege.Name.MODIFY_TABLE,
        Privilege.Name.CREATE_TABLE);
    MethodInvocation drop = dropTableInvocation(tableId());
    assertErrorResponse(interceptor.invoke(drop), Response.Status.FORBIDDEN);
    verify(drop, never()).proceed();
    MethodInvocation deregister = deregisterTableInvocation(tableId());
    assertErrorResponse(interceptor.invoke(deregister), Response.Status.FORBIDDEN);
    verify(deregister, never()).proceed();

    when(authorizer.isOwner(any(), any(), any(), any())).thenReturn(true);
    assertEquals(PROCEEDED, interceptor.invoke(dropTableInvocation(tableId())));
    assertEquals(PROCEEDED, interceptor.invoke(deregisterTableInvocation(tableId())));
  }

  @Test
  void testMutationExpressionsRejectIdentifiersOfTheWrongDepth() throws Throwable {
    when(authorizer.isOwner(any(), any(), any(), any())).thenReturn(true);
    allow(Privilege.Name.values());

    // A schema identifier must not be authorized as if it addressed a table.
    assertErrorResponse(
        interceptor.invoke(dropTableInvocation(CATALOG + "$" + SCHEMA)), Response.Status.FORBIDDEN);
    assertErrorResponse(
        interceptor.invoke(dropColumnsInvocation(CATALOG)), Response.Status.FORBIDDEN);
  }

  private MethodInvocation dropTableInvocation(String tableId) throws Throwable {
    Method method =
        LanceTableOperations.class.getMethod(
            "dropTable", String.class, String.class, HttpHeaders.class, DropTableRequest.class);
    return invocation(method, tableId, "$", null, new DropTableRequest());
  }

  private MethodInvocation deregisterTableInvocation(String tableId) throws Throwable {
    Method method =
        LanceTableOperations.class.getMethod(
            "deregisterTable",
            String.class,
            String.class,
            HttpHeaders.class,
            DeregisterTableRequest.class);
    return invocation(method, tableId, "$", null, new DeregisterTableRequest());
  }

  private MethodInvocation dropColumnsInvocation(String tableId) throws Throwable {
    Method method =
        LanceTableOperations.class.getMethod(
            "dropColumns",
            String.class,
            String.class,
            HttpHeaders.class,
            AlterTableDropColumnsRequest.class);
    return invocation(method, tableId, "$", null, new AlterTableDropColumnsRequest());
  }

  private MethodInvocation alterColumnsInvocation(String tableId) throws Throwable {
    Method method =
        LanceTableOperations.class.getMethod(
            "alterColumns",
            String.class,
            String.class,
            HttpHeaders.class,
            AlterTableAlterColumnsRequest.class);
    return invocation(method, tableId, "$", null, new AlterTableAlterColumnsRequest());
  }

  private MethodInvocation createTableInvocation(String tableId, String mode) throws Throwable {
    Method method =
        LanceTableOperations.class.getMethod(
            "createTable",
            String.class,
            String.class,
            String.class,
            String.class,
            String.class,
            HttpHeaders.class,
            byte[].class);
    return invocation(method, tableId, mode, "$", null, null, null, new byte[0]);
  }

  private MethodInvocation declareTableInvocation(String tableId) throws Throwable {
    Method method =
        LanceTableOperations.class.getMethod(
            "declareTable",
            String.class,
            String.class,
            DeclareTableRequest.class,
            HttpHeaders.class);
    return invocation(method, tableId, "$", new DeclareTableRequest(), null);
  }

  private MethodInvocation registerTableInvocation(String tableId, String mode) throws Throwable {
    Method method =
        LanceTableOperations.class.getMethod(
            "registerTable",
            String.class,
            String.class,
            HttpHeaders.class,
            RegisterTableRequest.class);
    RegisterTableRequest request = new RegisterTableRequest();
    request.setMode(mode);
    return invocation(method, tableId, "$", null, request);
  }

  private String tableId() {
    return CATALOG + "$" + SCHEMA + "$" + TABLE;
  }

  private MethodInvocation describeTableInvocation(String tableId) throws Throwable {
    Method method =
        LanceTableOperations.class.getMethod(
            "describeTable", String.class, String.class, Boolean.class, DescribeTableRequest.class);
    return invocation(method, tableId, "$", null, new DescribeTableRequest());
  }

  private MethodInvocation tableExistsInvocation(String tableId) throws Throwable {
    Method method =
        LanceTableOperations.class.getMethod(
            "tableExists", String.class, String.class, HttpHeaders.class, TableExistsRequest.class);
    return invocation(method, tableId, "$", null, new TableExistsRequest());
  }

  private MethodInvocation listTablesInvocation(String namespaceId) throws Throwable {
    Method method =
        LanceNamespaceOperations.class.getMethod(
            "listTables", String.class, String.class, String.class, Integer.class);
    return invocation(method, namespaceId, "$", null, null);
  }

  private MethodInvocation createInvocation(String namespaceId, String delimiter, String mode)
      throws Throwable {
    Method method =
        LanceNamespaceOperations.class.getMethod(
            "createNamespace", String.class, String.class, CreateNamespaceRequest.class);
    CreateNamespaceRequest request = new CreateNamespaceRequest();
    request.setMode(mode);
    return invocation(method, namespaceId, delimiter, request);
  }

  private MethodInvocation dropInvocation(String namespaceId, String delimiter) throws Throwable {
    Method method =
        LanceNamespaceOperations.class.getMethod(
            "dropNamespace", String.class, String.class, DropNamespaceRequest.class);
    return invocation(method, namespaceId, delimiter, new DropNamespaceRequest());
  }

  private void allow(Privilege.Name... privileges) {
    Set<Privilege.Name> allowed = Set.of(privileges);
    // doAnswer, not when(...): a test that narrows the allowed privileges calls this twice, and
    // when(...) would invoke the already-stubbed answer with null arguments.
    doAnswer(invocation -> allowed.contains(invocation.getArgument(3)))
        .when(authorizer)
        .authorize(any(), any(), any(), any(), any());
  }

  private Method namespaceMethod(String name) throws NoSuchMethodException {
    return LanceNamespaceOperations.class.getMethod(name, String.class, String.class);
  }

  private MethodInvocation invocation(Method method, Object... args) throws Throwable {
    MethodInvocation invocation = mock(MethodInvocation.class);
    when(invocation.getMethod()).thenReturn(method);
    when(invocation.getArguments()).thenReturn(args);
    when(invocation.proceed()).thenReturn(PROCEEDED);
    return invocation;
  }

  private void assertErrorResponse(Object result, Response.Status expectedStatus) {
    assertErrorResponse(result, expectedStatus, null);
  }

  private void assertErrorResponse(Object result, Response.Status expectedStatus, String message) {
    Response response = assertInstanceOf(Response.class, result);
    assertEquals(expectedStatus.getStatusCode(), response.getStatus(), message);
    assertInstanceOf(ErrorResponse.class, response.getEntity());
  }

  public static class TestOperations {
    @AuthorizationExpression(expression = "CAN_ACCESS_METADATA")
    public void missingTarget(@QueryParam("delimiter") String delimiter) {}

    @AuthorizationExpression(expression = "CAN_ACCESS_METADATA")
    @LanceRootNamespace
    public void conflictingRoot(@PathParam("id") String namespaceId) {}

    public void unannotated() {}
  }
}
