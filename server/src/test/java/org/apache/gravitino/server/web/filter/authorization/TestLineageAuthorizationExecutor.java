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

package org.apache.gravitino.server.web.filter.authorization;

import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants.CAN_ACCESS_METADATA;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.openlineage.server.OpenLineage.DatasetFacet;
import io.openlineage.server.OpenLineage.DatasetFacets;
import io.openlineage.server.OpenLineage.InputDataset;
import io.openlineage.server.OpenLineage.Job;
import io.openlineage.server.OpenLineage.OutputDataset;
import io.openlineage.server.OpenLineage.Run;
import io.openlineage.server.OpenLineage.RunEvent;
import io.openlineage.server.OpenLineage.RunEvent.EventType;
import java.lang.reflect.Method;
import java.net.URI;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import javax.ws.rs.core.Response;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.ActiveRoles;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.exceptions.BadRequestException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.lineage.source.rest.LineageOperations;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.gravitino.server.authorization.annotations.AuthorizationRequest;
import org.apache.gravitino.server.authorization.annotations.ExpressionCondition;
import org.apache.gravitino.server.web.filter.GravitinoInterceptionService;
import org.apache.gravitino.utils.PrincipalUtils;
import org.glassfish.hk2.api.Descriptor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

class TestLineageAuthorizationExecutor {

  private static final String METALAKE = "metalake";
  private static final String DATASET_NAME = "catalog.schema.object";
  private static final URI PRODUCER = URI.create("https://gravitino.apache.org/test");
  private static final URI SCHEMA_URL =
      URI.create("https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent");

  @Test
  void testRegisterWithStandardInterceptionFlow() throws Exception {
    GravitinoInterceptionService service = new GravitinoInterceptionService();
    Descriptor descriptor = mock(Descriptor.class);
    when(descriptor.getImplementation()).thenReturn(LineageOperations.class.getName());
    Method method = lineageMethod();

    Assertions.assertTrue(service.getDescriptorFilter().matches(descriptor));
    Assertions.assertEquals(1, service.getMethodInterceptors(method).size());
    Assertions.assertTrue(
        service
            .getMethodInterceptors(method)
            .get(0)
            .getClass()
            .getSimpleName()
            .contains("MetadataAuthorization"));
    Assertions.assertInstanceOf(LineageAuthorizationExecutor.class, createWithFactory(event()));
  }

  @Test
  void testExposeEventOrganizationAsAuthorizationMetalake() throws Exception {
    Assertions.assertEquals(Optional.of(METALAKE), executor(event()).getAuthorizationMetalake());
  }

  @Test
  void testInterceptorRejectsUserOutsideDynamicMetalake() throws Throwable {
    MethodInvocation invocation = invocation(event());

    try (MockedStatic<PrincipalUtils> principalUtils = mockStatic(PrincipalUtils.class);
        MockedStatic<AuthorizationUtils> authorizationUtils =
            mockStatic(AuthorizationUtils.class)) {
      principalUtils.when(PrincipalUtils::getCurrentPrincipal).thenReturn(principal());
      principalUtils.when(PrincipalUtils::getCurrentUserName).thenReturn("tester");
      authorizationUtils
          .when(
              () ->
                  AuthorizationUtils.checkCurrentUser(
                      eq(METALAKE), eq("tester"), any(AuthorizationRequestContext.class)))
          .thenThrow(new ForbiddenException("User tester is not a member"));

      Response response = (Response) lineageInterceptor().invoke(invocation);

      Assertions.assertEquals(Response.Status.FORBIDDEN.getStatusCode(), response.getStatus());
      verify(invocation, never()).proceed();
    }
  }

  @Test
  void testRejectNonexistentDynamicMetalakeAsBadRequest() throws Throwable {
    MethodInvocation invocation = invocation(event());

    try (MockedStatic<PrincipalUtils> principalUtils = mockStatic(PrincipalUtils.class);
        MockedStatic<AuthorizationUtils> authorizationUtils =
            mockStatic(AuthorizationUtils.class)) {
      principalUtils.when(PrincipalUtils::getCurrentPrincipal).thenReturn(principal());
      principalUtils.when(PrincipalUtils::getCurrentUserName).thenReturn("tester");
      authorizationUtils
          .when(
              () ->
                  AuthorizationUtils.checkCurrentUser(
                      eq(METALAKE), eq("tester"), any(AuthorizationRequestContext.class)))
          .thenThrow(new NoSuchMetalakeException("Metalake does not exist"));

      Response response = (Response) lineageInterceptor().invoke(invocation);

      Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
      ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
      Assertions.assertTrue(
          errorResponse.getMessage().contains("job.namespace"),
          "The response should identify the invalid field");
      verify(invocation, never()).proceed();
    }
  }

  @Test
  void testInterceptorRejectsUnheldActiveRoleForDynamicMetalake() throws Throwable {
    GravitinoAuthorizer authorizer = mock(GravitinoAuthorizer.class);
    when(authorizer.findUnheldRoles(any(), eq(METALAKE), any(), any()))
        .thenReturn(Set.of("ghostRole"));
    UserPrincipal principal =
        new UserPrincipal("tester")
            .withActiveRoles(ActiveRoles.of(Collections.singletonList("ghostRole")));
    MethodInvocation invocation = invocation(event());

    try (MockedStatic<PrincipalUtils> principalUtils = mockStatic(PrincipalUtils.class);
        MockedStatic<AuthorizationUtils> authorizationUtils = mockStatic(AuthorizationUtils.class);
        MockedStatic<GravitinoAuthorizerProvider> providerStatic =
            mockStatic(GravitinoAuthorizerProvider.class)) {
      principalUtils.when(PrincipalUtils::getCurrentPrincipal).thenReturn(principal);
      principalUtils.when(PrincipalUtils::getCurrentUserName).thenReturn("tester");
      GravitinoAuthorizerProvider provider = mock(GravitinoAuthorizerProvider.class);
      providerStatic.when(GravitinoAuthorizerProvider::getInstance).thenReturn(provider);
      when(provider.getGravitinoAuthorizer()).thenReturn(authorizer);

      Response response = (Response) lineageInterceptor().invoke(invocation);

      Assertions.assertEquals(Response.Status.FORBIDDEN.getStatusCode(), response.getStatus());
      verify(invocation, never()).proceed();
      authorizationUtils.verify(
          () ->
              AuthorizationUtils.checkCurrentUser(
                  eq(METALAKE), eq("tester"), any(AuthorizationRequestContext.class)));
    }
  }

  @ParameterizedTest
  @CsvSource({"inputs,0", "inputs,1", "outputs,0", "outputs,1"})
  void testRejectCrossOrganizationDatasetAtEveryPosition(String field, int index) throws Throwable {
    List<InputDataset> inputs =
        new ArrayList<>(List.of(input("catalog.schema.input0"), input("catalog.schema.input1")));
    List<OutputDataset> outputs =
        new ArrayList<>(
            List.of(output("catalog.schema.output0"), output("catalog.schema.output1")));
    if (field.equals("inputs")) {
      inputs.set(
          index, new InputDataset("anotherMetalake", inputs.get(index).getName(), null, null));
    } else {
      outputs.set(
          index, new OutputDataset("anotherMetalake", outputs.get(index).getName(), null, null));
    }

    assertBadRequest(event(METALAKE, inputs, outputs));
  }

  @ParameterizedTest
  @ValueSource(strings = {"UNKNOWN", "MODEL_VERSION"})
  void testRejectUnsupportedDatasetTypeAsBadRequest(String datasetType) throws Throwable {
    assertBadRequest(
        event(METALAKE, List.of(dataset(datasetType, METALAKE, DATASET_NAME)), List.of()));
  }

  @Test
  void testRejectMalformedDatasetNameAsBadRequest() throws Throwable {
    assertBadRequest(
        event(METALAKE, List.of(dataset(null, METALAKE, "catalog.object")), List.of()));
  }

  @Test
  void testRejectMalformedEventAsBadRequest() throws Throwable {
    assertBadRequest(null);
  }

  @Test
  void testResolveTargetsOnlyDuringExecution() throws Exception {
    LineageAuthorizationExecutor executor =
        executor(event(METALAKE, List.of(input("catalog.object")), List.of()));

    Assertions.assertEquals(Optional.of(METALAKE), executor.getAuthorizationMetalake());
    Assertions.assertThrows(
        BadRequestException.class,
        () -> execute(executor, mock(GravitinoAuthorizer.class), principal()));
  }

  @Test
  void testAllowDatasetlessEvent() throws Exception {
    GravitinoAuthorizer authorizer = mock(GravitinoAuthorizer.class);
    LineageAuthorizationExecutor executor = executor(event(METALAKE, List.of(), List.of()));

    Assertions.assertEquals(Optional.of(METALAKE), executor.getAuthorizationMetalake());
    Assertions.assertTrue(execute(executor, authorizer, principal()));
    verifyNoInteractions(authorizer);
  }

  @Test
  void testOutputRequiresMetadataVisibilityOnly() throws Exception {
    MetadataObject outputObject = MetadataObjects.parse(DATASET_NAME, MetadataObject.Type.TABLE);
    GravitinoAuthorizer authorizer = mock(GravitinoAuthorizer.class);
    when(authorizer.authorize(
            any(),
            eq(METALAKE),
            any(),
            eq(Privilege.Name.USE_CATALOG),
            any(AuthorizationRequestContext.class)))
        .thenReturn(true);
    when(authorizer.authorize(
            any(),
            eq(METALAKE),
            any(),
            eq(Privilege.Name.USE_SCHEMA),
            any(AuthorizationRequestContext.class)))
        .thenReturn(true);
    when(authorizer.authorize(
            any(),
            eq(METALAKE),
            eq(outputObject),
            eq(Privilege.Name.SELECT_TABLE),
            any(AuthorizationRequestContext.class)))
        .thenReturn(true);

    Assertions.assertTrue(
        execute(
            executor(event(METALAKE, List.of(), List.of(output(DATASET_NAME)))),
            authorizer,
            principal()));
    verify(authorizer)
        .authorize(
            any(),
            eq(METALAKE),
            eq(outputObject),
            eq(Privilege.Name.SELECT_TABLE),
            any(AuthorizationRequestContext.class));
  }

  @Test
  void testPermissionDenialReturnsForbidden() throws Throwable {
    GravitinoAuthorizer authorizer = mock(GravitinoAuthorizer.class);

    Response response = intercept(event(), authorizer);

    Assertions.assertEquals(Response.Status.FORBIDDEN.getStatusCode(), response.getStatus());
  }

  @Test
  void testAuthorizerIllegalArgumentExceptionReturnsInternalError() throws Throwable {
    GravitinoAuthorizer authorizer = mock(GravitinoAuthorizer.class);
    when(authorizer.isOwner(any(), any(), any(), any()))
        .thenThrow(new IllegalArgumentException("Authorizer backend failure"));

    Response response = intercept(event(), authorizer);

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
  }

  @Test
  void testAuthorizeAllInputAndOutputDatasets() throws Exception {
    GravitinoAuthorizer authorizer = mock(GravitinoAuthorizer.class);
    when(authorizer.isOwner(any(), any(), any(), any())).thenReturn(true);
    RunEvent runEvent =
        event(
            METALAKE,
            List.of(input("catalog.schema.input")),
            List.of(output("catalog.schema.output")));

    Assertions.assertTrue(execute(executor(runEvent, "TABLE::OWNER"), authorizer, principal()));
  }

  @ParameterizedTest
  @CsvSource({"inputs,0", "inputs,1", "outputs,0", "outputs,1"})
  void testPermissionFailureAtEveryDatasetPosition(String field, int index) throws Exception {
    List<InputDataset> inputs =
        List.of(input("catalog.schema.input0"), input("catalog.schema.input1"));
    List<OutputDataset> outputs =
        List.of(output("catalog.schema.output0"), output("catalog.schema.output1"));
    String deniedName =
        field.equals("inputs") ? inputs.get(index).getName() : outputs.get(index).getName();
    MetadataObject denied = MetadataObjects.parse(deniedName, MetadataObject.Type.TABLE);
    GravitinoAuthorizer authorizer = mock(GravitinoAuthorizer.class);
    when(authorizer.isOwner(any(), eq(METALAKE), any(), any()))
        .thenAnswer(call -> !denied.equals(call.getArgument(2)));

    Assertions.assertFalse(
        execute(
            executor(event(METALAKE, inputs, outputs), "TABLE::OWNER"), authorizer, principal()));
    verify(authorizer).isOwner(any(), eq(METALAKE), eq(denied), any());
  }

  @ParameterizedTest
  @CsvSource({
    "DEFAULT,TABLE",
    "TABLE,TABLE",
    "VIEW,VIEW",
    "FILE,FILESET",
    "Fileset,FILESET",
    "MODEL,MODEL",
    "TOPIC,TOPIC"
  })
  void testResolveExactAuthorizationTarget(String datasetType, MetadataObject.Type expectedType)
      throws Exception {
    String facetType = datasetType.equals("DEFAULT") ? null : datasetType;
    InputDataset dataset = dataset(facetType, METALAKE, DATASET_NAME);
    MetadataObject expected = MetadataObjects.parse(DATASET_NAME, expectedType);
    GravitinoAuthorizer authorizer = mock(GravitinoAuthorizer.class);
    when(authorizer.isOwner(any(), eq(METALAKE), any(), any()))
        .thenAnswer(call -> expected.equals(call.getArgument(2)));

    Assertions.assertEquals(expectedType, LineageAuthorizationExecutor.getMetadataType(dataset));
    Assertions.assertTrue(
        execute(
            executor(event(METALAKE, List.of(dataset), List.of()), expectedType.name() + "::OWNER"),
            authorizer,
            principal()));
    verify(authorizer).isOwner(any(), eq(METALAKE), eq(expected), any());
  }

  private static void assertBadRequest(RunEvent event) throws Throwable {
    MethodInvocation invocation = invocation(event);
    try (MockedStatic<PrincipalUtils> principalUtils = mockStatic(PrincipalUtils.class);
        MockedStatic<AuthorizationUtils> authorizationUtils =
            mockStatic(AuthorizationUtils.class)) {
      principalUtils.when(PrincipalUtils::getCurrentPrincipal).thenReturn(principal());
      principalUtils.when(PrincipalUtils::getCurrentUserName).thenReturn("tester");

      Response response = (Response) lineageInterceptor().invoke(invocation);

      Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
      verify(invocation, never()).proceed();
      if (event == null) {
        authorizationUtils.verifyNoInteractions();
      } else {
        authorizationUtils.verify(
            () ->
                AuthorizationUtils.checkCurrentUser(
                    eq(METALAKE), eq("tester"), any(AuthorizationRequestContext.class)));
      }
    }
  }

  private static Response intercept(RunEvent event, GravitinoAuthorizer authorizer)
      throws Throwable {
    MethodInvocation invocation = invocation(event);
    try (MockedStatic<PrincipalUtils> principalUtils = mockStatic(PrincipalUtils.class);
        MockedStatic<AuthorizationUtils> authorizationUtils = mockStatic(AuthorizationUtils.class);
        MockedStatic<GravitinoAuthorizerProvider> providerStatic =
            mockStatic(GravitinoAuthorizerProvider.class)) {
      principalUtils.when(PrincipalUtils::getCurrentPrincipal).thenReturn(principal());
      principalUtils.when(PrincipalUtils::getCurrentUserName).thenReturn("tester");
      GravitinoAuthorizerProvider provider = mock(GravitinoAuthorizerProvider.class);
      providerStatic.when(GravitinoAuthorizerProvider::getInstance).thenReturn(provider);
      when(provider.getGravitinoAuthorizer()).thenReturn(authorizer);

      Response response = (Response) lineageInterceptor().invoke(invocation);
      verify(invocation, never()).proceed();
      return response;
    }
  }

  private static boolean execute(
      LineageAuthorizationExecutor executor,
      GravitinoAuthorizer authorizer,
      UserPrincipal principal)
      throws Exception {
    try (MockedStatic<GravitinoAuthorizerProvider> providerStatic =
        mockStatic(GravitinoAuthorizerProvider.class)) {
      GravitinoAuthorizerProvider provider = mock(GravitinoAuthorizerProvider.class);
      providerStatic.when(GravitinoAuthorizerProvider::getInstance).thenReturn(provider);
      when(provider.getGravitinoAuthorizer()).thenReturn(authorizer);
      return PrincipalUtils.doAs(
          principal, () -> executor.execute(new AuthorizationRequestContext()));
    }
  }

  private static LineageAuthorizationExecutor executor(RunEvent event) throws Exception {
    return executor(event, CAN_ACCESS_METADATA);
  }

  private static LineageAuthorizationExecutor executor(RunEvent event, String expression)
      throws Exception {
    return new LineageAuthorizationExecutor(
        lineageMethod().getParameters(), new Object[] {event}, expression);
  }

  private static AuthorizationExecutor createWithFactory(RunEvent event) throws Exception {
    Map<Entity.EntityType, NameIdentifier> metadataContext = new HashMap<>();
    return AuthorizeExecutorFactory.create(
        CAN_ACCESS_METADATA,
        AuthorizationRequest.RequestType.LINEAGE,
        metadataContext,
        Map.of(),
        Optional.empty(),
        lineageMethod().getParameters(),
        new Object[] {event},
        "",
        ExpressionCondition.NEVER,
        "");
  }

  private static Method lineageMethod() throws NoSuchMethodException {
    return LineageOperations.class.getMethod("postLineage", RunEvent.class);
  }

  private static MethodInterceptor lineageInterceptor() throws NoSuchMethodException {
    return new GravitinoInterceptionService().getMethodInterceptors(lineageMethod()).get(0);
  }

  private static MethodInvocation invocation(RunEvent event) throws NoSuchMethodException {
    MethodInvocation invocation = mock(MethodInvocation.class);
    when(invocation.getMethod()).thenReturn(lineageMethod());
    when(invocation.getArguments()).thenReturn(new Object[] {event});
    return invocation;
  }

  private static UserPrincipal principal() {
    return new UserPrincipal("tester");
  }

  private static InputDataset input(String name) {
    return new InputDataset(METALAKE, name, null, null);
  }

  private static OutputDataset output(String name) {
    return new OutputDataset(METALAKE, name, null, null);
  }

  private static InputDataset dataset(String datasetType, String namespace, String name) {
    if (datasetType == null) {
      return new InputDataset(namespace, name, null, null);
    }

    DatasetFacets facets = new DatasetFacets();
    DatasetFacet typeFacet = new DatasetFacet(PRODUCER, SCHEMA_URL);
    typeFacet.getAdditionalProperties().put("datasetType", datasetType);
    facets.getAdditionalProperties().put("datasetType", typeFacet);
    return new InputDataset(namespace, name, facets, null);
  }

  private static RunEvent event() {
    return event(METALAKE, List.of(input(DATASET_NAME)), List.of());
  }

  private static RunEvent event(
      String jobNamespace, List<InputDataset> inputs, List<OutputDataset> outputs) {
    return new RunEvent(
        ZonedDateTime.now(ZoneOffset.UTC),
        PRODUCER,
        SCHEMA_URL,
        EventType.START,
        new Run(UUID.randomUUID(), null),
        new Job(jobNamespace, "job", null),
        inputs,
        outputs);
  }
}
