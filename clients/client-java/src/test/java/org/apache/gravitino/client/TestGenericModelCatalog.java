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
package org.apache.gravitino.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogProvider;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.CatalogDTO;
import org.apache.gravitino.dto.model.ModelDTO;
import org.apache.gravitino.dto.model.ModelVersionDTO;
import org.apache.gravitino.dto.requests.CatalogCreateRequest;
import org.apache.gravitino.dto.requests.ModelRegisterRequest;
import org.apache.gravitino.dto.requests.ModelUpdateRequest;
import org.apache.gravitino.dto.requests.ModelUpdatesRequest;
import org.apache.gravitino.dto.requests.ModelVersionLinkRequest;
import org.apache.gravitino.dto.requests.ModelVersionUpdateRequest;
import org.apache.gravitino.dto.requests.ModelVersionUpdatesRequest;
import org.apache.gravitino.dto.responses.BaseResponse;
import org.apache.gravitino.dto.responses.CatalogResponse;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.ModelResponse;
import org.apache.gravitino.dto.responses.ModelVersionInfoListResponse;
import org.apache.gravitino.dto.responses.ModelVersionListResponse;
import org.apache.gravitino.dto.responses.ModelVersionResponse;
import org.apache.gravitino.dto.responses.ModelVersionUriResponse;
import org.apache.gravitino.exceptions.ModelAlreadyExistsException;
import org.apache.gravitino.exceptions.ModelVersionAliasesAlreadyExistException;
import org.apache.gravitino.exceptions.NoSuchModelException;
import org.apache.gravitino.exceptions.NoSuchModelVersionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.model.ModelVersion;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestGenericModelCatalog extends TestBase {

  private static final String METALAKE_NAME = "metalake_for_model_test";

  private static final String CATALOG_NAME = "catalog_for_model_test";

  private static Catalog catalog;

  private static GravitinoMetalake metalake;

  @BeforeAll
  public static void setUp() throws Exception {
    TestBase.setUp();

    metalake = TestGravitinoMetalake.createMetalake(client, METALAKE_NAME);

    CatalogDTO mockCatalog =
        CatalogDTO.builder()
            .withName(CATALOG_NAME)
            .withType(Catalog.Type.MODEL)
            .withProvider(CatalogProvider.shortNameForManagedCatalog(Catalog.Type.MODEL))
            .withComment("comment")
            .withProperties(Collections.emptyMap())
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();

    CatalogCreateRequest request =
        new CatalogCreateRequest(
            CATALOG_NAME, Catalog.Type.MODEL, null, "comment", Collections.emptyMap());
    CatalogResponse resp = new CatalogResponse(mockCatalog);
    buildMockResource(
        Method.POST,
        "/api/metalakes/" + METALAKE_NAME + "/catalogs",
        request,
        resp,
        HttpStatus.SC_OK);

    catalog =
        metalake.createCatalog(CATALOG_NAME, Catalog.Type.MODEL, "comment", Collections.emptyMap());
  }

  @ParameterizedTest
  @ValueSource(strings = {"schema1/model1/model2", "스키마1/모델1/모델2"})
  public void testListModels(String input) throws JsonProcessingException {
    String[] split = input.split("/");
    String schemaName = split[0];
    String[] modelNames = new String[] {split[1], split[2]};
    NameIdentifier modelId1 = NameIdentifier.of(schemaName, modelNames[0]);
    NameIdentifier modelId2 = NameIdentifier.of(schemaName, modelNames[1]);

    NameIdentifier resultModelId1 =
        NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaName, modelNames[0]);
    NameIdentifier resultModelId2 =
        NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaName, modelNames[1]);

    String modelPath =
        withSlash(
            GenericModelCatalog.formatModelRequestPath(
                Namespace.of(METALAKE_NAME, CATALOG_NAME, schemaName)));

    EntityListResponse resp =
        new EntityListResponse(new NameIdentifier[] {resultModelId1, resultModelId2});
    buildMockResource(Method.GET, modelPath, null, resp, HttpStatus.SC_OK);

    NameIdentifier[] modelIds = catalog.asModelCatalog().listModels(modelId1.namespace());
    Assertions.assertEquals(2, modelIds.length);
    Assertions.assertEquals(modelId1, modelIds[0]);
    Assertions.assertEquals(modelId2, modelIds[1]);

    // Throw schema not found exception
    ErrorResponse errResp =
        ErrorResponse.notFound(NoSuchSchemaException.class.getSimpleName(), "schema not found");
    buildMockResource(Method.GET, modelPath, null, errResp, HttpStatus.SC_NOT_FOUND);
    Assertions.assertThrows(
        NoSuchSchemaException.class,
        () -> catalog.asModelCatalog().listModels(modelId1.namespace()),
        "schema not found");

    // Throw RuntimeException
    ErrorResponse errResp2 = ErrorResponse.internalError("internal error");
    buildMockResource(Method.GET, modelPath, null, errResp2, HttpStatus.SC_INTERNAL_SERVER_ERROR);
    Assertions.assertThrows(
        RuntimeException.class,
        () -> catalog.asModelCatalog().listModels(modelId1.namespace()),
        "internal error");
  }

  @ParameterizedTest
  @ValueSource(strings = {"schema1/model1", "스키마1/모델1"})
  public void testGetModel(String input) throws JsonProcessingException {
    String[] split = input.split("/");
    String schemaName = split[0];
    String modelName = split[1];

    NameIdentifier modelId = NameIdentifier.of(schemaName, modelName);
    String modelPath =
        withSlash(
            GenericModelCatalog.formatModelRequestPath(
                    Namespace.of(METALAKE_NAME, CATALOG_NAME, schemaName))
                + "/"
                + RESTUtils.encodeString(modelId.name()));

    ModelDTO modelDTO = mockModelDTO("model1", 0, "model comment", Collections.emptyMap());
    ModelResponse resp = new ModelResponse(modelDTO);
    buildMockResource(Method.GET, modelPath, null, resp, HttpStatus.SC_OK);

    Model model = catalog.asModelCatalog().getModel(modelId);
    compareModel(modelDTO, model);

    // Throw model not found exception
    ErrorResponse errResp =
        ErrorResponse.notFound(NoSuchModelException.class.getSimpleName(), "model not found");
    buildMockResource(Method.GET, modelPath, null, errResp, HttpStatus.SC_NOT_FOUND);

    Assertions.assertThrows(
        NoSuchModelException.class,
        () -> catalog.asModelCatalog().getModel(modelId),
        "model not found");

    // Throw RuntimeException
    ErrorResponse errResp2 = ErrorResponse.internalError("internal error");
    buildMockResource(Method.GET, modelPath, null, errResp2, HttpStatus.SC_INTERNAL_SERVER_ERROR);

    Assertions.assertThrows(
        RuntimeException.class, () -> catalog.asModelCatalog().getModel(modelId), "internal error");
  }

  @ParameterizedTest
  @ValueSource(strings = {"schema1/model1", "스키마1/모델1"})
  public void testRegisterModel(String input) throws JsonProcessingException {
    String[] split = input.split("/");
    String schemaName = split[0];
    String modelName = split[1];
    NameIdentifier modelId = NameIdentifier.of(schemaName, modelName);
    ModelDTO modelDTO = mockModelDTO(schemaName, 0, "model comment", Collections.emptyMap());
    ModelResponse resp = new ModelResponse(modelDTO);
    ModelRegisterRequest request =
        new ModelRegisterRequest(modelId.name(), "model comment", Collections.emptyMap());

    String modelPath =
        withSlash(
            GenericModelCatalog.formatModelRequestPath(
                Namespace.of(METALAKE_NAME, CATALOG_NAME, schemaName)));

    buildMockResource(Method.POST, modelPath, request, resp, HttpStatus.SC_OK);

    Model model =
        catalog.asModelCatalog().registerModel(modelId, "model comment", Collections.emptyMap());
    compareModel(modelDTO, model);

    // Throw schema not found exception
    ErrorResponse errResp =
        ErrorResponse.notFound(NoSuchSchemaException.class.getSimpleName(), "schema not found");
    buildMockResource(Method.POST, modelPath, request, errResp, HttpStatus.SC_NOT_FOUND);

    Assertions.assertThrows(
        NoSuchSchemaException.class,
        () ->
            catalog
                .asModelCatalog()
                .registerModel(modelId, "model comment", Collections.emptyMap()),
        "schema not found");

    // Throw model already exists exception
    ErrorResponse errResp2 =
        ErrorResponse.alreadyExists(
            ModelAlreadyExistsException.class.getSimpleName(), "model already exists");
    buildMockResource(Method.POST, modelPath, request, errResp2, HttpStatus.SC_CONFLICT);

    Assertions.assertThrows(
        ModelAlreadyExistsException.class,
        () ->
            catalog
                .asModelCatalog()
                .registerModel(modelId, "model comment", Collections.emptyMap()),
        "model already exists");

    // Throw RuntimeException
    ErrorResponse errResp3 = ErrorResponse.internalError("internal error");
    buildMockResource(
        Method.POST, modelPath, request, errResp3, HttpStatus.SC_INTERNAL_SERVER_ERROR);

    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            catalog
                .asModelCatalog()
                .registerModel(modelId, "model comment", Collections.emptyMap()),
        "internal error");
  }

  @ParameterizedTest
  @ValueSource(strings = {"schema1/model1", "스키마1/모델1"})
  public void testDeleteModel(String input) throws JsonProcessingException {
    String[] split = input.split("/");
    String schemaName = split[0];
    String modelName = split[1];

    NameIdentifier modelId = NameIdentifier.of(schemaName, modelName);
    String modelPath =
        withSlash(
            GenericModelCatalog.formatModelRequestPath(
                    Namespace.of(METALAKE_NAME, CATALOG_NAME, schemaName))
                + "/"
                + RESTUtils.encodeString(modelId.name()));

    DropResponse resp = new DropResponse(true);
    buildMockResource(Method.DELETE, modelPath, null, resp, HttpStatus.SC_OK);

    Assertions.assertTrue(catalog.asModelCatalog().deleteModel(modelId));

    // Test RuntimeException
    ErrorResponse errResp = ErrorResponse.internalError("internal error");
    buildMockResource(Method.DELETE, modelPath, null, errResp, HttpStatus.SC_INTERNAL_SERVER_ERROR);

    Assertions.assertThrows(
        RuntimeException.class,
        () -> catalog.asModelCatalog().deleteModel(modelId),
        "internal error");
  }

  @ParameterizedTest
  @ValueSource(strings = {"schema1/model1", "스키마1/모델1"})
  public void testListModelVersions(String input) throws JsonProcessingException {
    String split[] = input.split("/");
    String schemaName = split[0];
    String modelName = split[1];
    NameIdentifier modelId = NameIdentifier.of(schemaName, modelName);
    String modelVersionPath =
        withSlash(
            GenericModelCatalog.formatModelVersionRequestPath(
                    NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaName, modelName))
                + "/versions");

    int[] expectedVersions = new int[] {0, 1, 2};
    ModelVersionListResponse resp = new ModelVersionListResponse(expectedVersions);
    buildMockResource(Method.GET, modelVersionPath, null, resp, HttpStatus.SC_OK);

    int[] versions = catalog.asModelCatalog().listModelVersions(modelId);
    Assertions.assertArrayEquals(expectedVersions, versions);

    // Throw model not found exception
    ErrorResponse errResp =
        ErrorResponse.notFound(NoSuchModelException.class.getSimpleName(), "model not found");
    buildMockResource(Method.GET, modelVersionPath, null, errResp, HttpStatus.SC_NOT_FOUND);

    Assertions.assertThrows(
        NoSuchModelException.class,
        () -> catalog.asModelCatalog().listModelVersions(modelId),
        "model not found");

    // Throw RuntimeException
    ErrorResponse errResp2 = ErrorResponse.internalError("internal error");
    buildMockResource(
        Method.GET, modelVersionPath, null, errResp2, HttpStatus.SC_INTERNAL_SERVER_ERROR);

    Assertions.assertThrows(
        RuntimeException.class,
        () -> catalog.asModelCatalog().listModelVersions(modelId),
        "internal error");
  }

  @ParameterizedTest
  @ValueSource(strings = {"schema1/model1", "스키마1/모델1"})
  public void testListModelVersionInfos(String input) throws JsonProcessingException {
    String[] split = input.split("/");
    String schemaName = split[0];
    String modelName = split[1];
    NameIdentifier modelId = NameIdentifier.of(schemaName, modelName);
    String modelVersionPath =
        withSlash(
            GenericModelCatalog.formatModelVersionRequestPath(
                    NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaName, modelName))
                + "/versions");

    ModelVersionDTO[] expectedVersions = {
      mockModelVersion(
          0, "uri", new String[] {"alias1", "alias2"}, "comment", Collections.emptyMap()),
      mockModelVersion(
          1, "uri", new String[] {"alias3", "alias4"}, "comment", Collections.emptyMap())
    };
    ModelVersionInfoListResponse resp = new ModelVersionInfoListResponse(expectedVersions);
    buildMockResource(
        Method.GET,
        modelVersionPath,
        ImmutableMap.of("details", "true"),
        null,
        resp,
        HttpStatus.SC_OK);

    ModelVersion[] versions = catalog.asModelCatalog().listModelVersionInfos(modelId);
    Assertions.assertArrayEquals(expectedVersions, versions);

    // Throw model not found exception
    ErrorResponse errResp =
        ErrorResponse.notFound(NoSuchModelException.class.getSimpleName(), "model not found");
    buildMockResource(
        Method.GET,
        modelVersionPath,
        ImmutableMap.of("details", "true"),
        null,
        errResp,
        HttpStatus.SC_NOT_FOUND);

    Assertions.assertThrows(
        NoSuchModelException.class,
        () -> catalog.asModelCatalog().listModelVersionInfos(modelId),
        "model not found");

    // Throw RuntimeException
    ErrorResponse errResp2 = ErrorResponse.internalError("internal error");
    buildMockResource(
        Method.GET,
        modelVersionPath,
        ImmutableMap.of("details", "true"),
        null,
        errResp2,
        HttpStatus.SC_INTERNAL_SERVER_ERROR);

    Assertions.assertThrows(
        RuntimeException.class,
        () -> catalog.asModelCatalog().listModelVersionInfos(modelId),
        "internal error");
  }

  @ParameterizedTest
  @ValueSource(strings = {"schema1/model1", "스키마1/모델1"})
  public void testGetModelVersion(String input) throws JsonProcessingException {
    String[] split = input.split("/");
    String schemaName = split[0];
    String modelName = split[1];
    NameIdentifier modelId = NameIdentifier.of(schemaName, modelName);
    String modelVersionPath =
        withSlash(
            GenericModelCatalog.formatModelVersionRequestPath(
                    NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaName, modelName))
                + "/versions/0");

    ModelVersionDTO mockModelVersion =
        mockModelVersion(
            0, "uri", new String[] {"alias1", "alias2"}, "comment", Collections.emptyMap());
    ModelVersionResponse resp = new ModelVersionResponse(mockModelVersion);
    buildMockResource(Method.GET, modelVersionPath, null, resp, HttpStatus.SC_OK);

    ModelVersion modelVersion = catalog.asModelCatalog().getModelVersion(modelId, 0);
    compareModelVersion(mockModelVersion, modelVersion);

    // Throw model version not found exception
    ErrorResponse errResp =
        ErrorResponse.notFound(
            NoSuchModelVersionException.class.getSimpleName(), "model version not found");
    buildMockResource(Method.GET, modelVersionPath, null, errResp, HttpStatus.SC_NOT_FOUND);

    Assertions.assertThrows(
        NoSuchModelVersionException.class,
        () -> catalog.asModelCatalog().getModelVersion(modelId, 0),
        "model version not found");

    // Throw RuntimeException
    ErrorResponse errResp2 = ErrorResponse.internalError("internal error");
    buildMockResource(
        Method.GET, modelVersionPath, null, errResp2, HttpStatus.SC_INTERNAL_SERVER_ERROR);

    Assertions.assertThrows(
        RuntimeException.class,
        () -> catalog.asModelCatalog().getModelVersion(modelId, 0),
        "internal error");
  }

  @ParameterizedTest
  @ValueSource(strings = {"schema1/model1/alias1/alias2", "스키마1/모델1/별칭1/별칭2"})
  public void testGetModelVersionByAlias(String input) throws JsonProcessingException {
    String[] split = input.split("/");
    String schemaName = split[0];
    String modelName = split[1];
    String[] aliasNames = new String[] {split[2], split[3]};
    NameIdentifier modelId = NameIdentifier.of(schemaName, modelName);
    String modelVersionPath =
        withSlash(
            GenericModelCatalog.formatModelVersionRequestPath(
                    NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaName, modelName))
                + "/aliases/"
                + RESTUtils.encodeString(aliasNames[0]));

    ModelVersionDTO mockModelVersion =
        mockModelVersion(0, "uri", aliasNames, "comment", Collections.emptyMap());
    ModelVersionResponse resp = new ModelVersionResponse(mockModelVersion);
    buildMockResource(Method.GET, modelVersionPath, null, resp, HttpStatus.SC_OK);

    ModelVersion modelVersion = catalog.asModelCatalog().getModelVersion(modelId, aliasNames[0]);
    compareModelVersion(mockModelVersion, modelVersion);

    // Throw model version not found exception
    ErrorResponse errResp =
        ErrorResponse.notFound(
            NoSuchModelVersionException.class.getSimpleName(), "model version not found");
    buildMockResource(Method.GET, modelVersionPath, null, errResp, HttpStatus.SC_NOT_FOUND);

    Assertions.assertThrows(
        NoSuchModelVersionException.class,
        () -> catalog.asModelCatalog().getModelVersion(modelId, aliasNames[0]),
        "model version not found");

    // Throw RuntimeException
    ErrorResponse errResp2 = ErrorResponse.internalError("internal error");
    buildMockResource(
        Method.GET, modelVersionPath, null, errResp2, HttpStatus.SC_INTERNAL_SERVER_ERROR);

    Assertions.assertThrows(
        RuntimeException.class,
        () -> catalog.asModelCatalog().getModelVersion(modelId, aliasNames[0]),
        "internal error");
  }

  @ParameterizedTest
  @ValueSource(strings = {"schema1/model1", "스키마1/모델1"})
  public void testLinkModelVersion(String input) throws JsonProcessingException {
    String[] split = input.split("/");
    String schemaName = split[0];
    String modelName = split[1];
    NameIdentifier modelId = NameIdentifier.of(schemaName, modelName);
    String modelVersionPath =
        withSlash(
            GenericModelCatalog.formatModelVersionRequestPath(
                    NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaName, modelName))
                + "/versions");

    ModelVersionLinkRequest request =
        new ModelVersionLinkRequest(
            ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "uri"),
            new String[] {"alias1", "alias2"},
            "comment",
            Collections.emptyMap());
    BaseResponse resp = new BaseResponse(0);
    buildMockResource(Method.POST, modelVersionPath, request, resp, HttpStatus.SC_OK);

    Assertions.assertDoesNotThrow(
        () ->
            catalog
                .asModelCatalog()
                .linkModelVersion(
                    modelId,
                    "uri",
                    new String[] {"alias1", "alias2"},
                    "comment",
                    Collections.emptyMap()));

    // Throw model not found exception
    ErrorResponse errResp =
        ErrorResponse.notFound(NoSuchModelException.class.getSimpleName(), "model not found");
    buildMockResource(Method.POST, modelVersionPath, request, errResp, HttpStatus.SC_NOT_FOUND);

    Assertions.assertThrows(
        NoSuchModelException.class,
        () ->
            catalog
                .asModelCatalog()
                .linkModelVersion(
                    modelId,
                    "uri",
                    new String[] {"alias1", "alias2"},
                    "comment",
                    Collections.emptyMap()),
        "model not found");

    // Throw ModelVersionAliasesAlreadyExistException
    ErrorResponse errResp2 =
        ErrorResponse.alreadyExists(
            ModelVersionAliasesAlreadyExistException.class.getSimpleName(),
            "model version already exists");
    buildMockResource(Method.POST, modelVersionPath, request, errResp2, HttpStatus.SC_CONFLICT);

    Assertions.assertThrows(
        ModelVersionAliasesAlreadyExistException.class,
        () ->
            catalog
                .asModelCatalog()
                .linkModelVersion(
                    modelId,
                    "uri",
                    new String[] {"alias1", "alias2"},
                    "comment",
                    Collections.emptyMap()),
        "model version already exists");

    // Throw RuntimeException
    ErrorResponse errResp3 = ErrorResponse.internalError("internal error");
    buildMockResource(
        Method.POST, modelVersionPath, request, errResp3, HttpStatus.SC_INTERNAL_SERVER_ERROR);

    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            catalog
                .asModelCatalog()
                .linkModelVersion(
                    modelId,
                    "uri",
                    new String[] {"alias1", "alias2"},
                    "comment",
                    Collections.emptyMap()),
        "internal error");
  }

  @ParameterizedTest
  @ValueSource(strings = {"schema1/model1", "스키마1/모델1"})
  public void testLinkModelVersionWithMultipleUris(String input) throws JsonProcessingException {
    String[] split = input.split("/");
    String schemaName = split[0];
    String modelName = split[1];
    NameIdentifier modelId = NameIdentifier.of(schemaName, modelName);
    String modelVersionPath =
        withSlash(
            GenericModelCatalog.formatModelVersionRequestPath(
                    NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaName, modelName))
                + "/versions");

    Map<String, String> uris = ImmutableMap.of("n1", "u1", "n2", "u2");
    ModelVersionLinkRequest request =
        new ModelVersionLinkRequest(
            uris, new String[] {"alias1", "alias2"}, "comment", Collections.emptyMap());
    BaseResponse resp = new BaseResponse(0);
    buildMockResource(Method.POST, modelVersionPath, request, resp, HttpStatus.SC_OK);

    Assertions.assertDoesNotThrow(
        () ->
            catalog
                .asModelCatalog()
                .linkModelVersion(
                    modelId,
                    uris,
                    new String[] {"alias1", "alias2"},
                    "comment",
                    Collections.emptyMap()));

    // Throw model not found exception
    ErrorResponse errResp =
        ErrorResponse.notFound(NoSuchModelException.class.getSimpleName(), "model not found");
    buildMockResource(Method.POST, modelVersionPath, request, errResp, HttpStatus.SC_NOT_FOUND);

    Assertions.assertThrows(
        NoSuchModelException.class,
        () ->
            catalog
                .asModelCatalog()
                .linkModelVersion(
                    modelId,
                    uris,
                    new String[] {"alias1", "alias2"},
                    "comment",
                    Collections.emptyMap()),
        "model not found");

    // Throw ModelVersionAliasesAlreadyExistException
    ErrorResponse errResp2 =
        ErrorResponse.alreadyExists(
            ModelVersionAliasesAlreadyExistException.class.getSimpleName(),
            "model version already exists");
    buildMockResource(Method.POST, modelVersionPath, request, errResp2, HttpStatus.SC_CONFLICT);

    Assertions.assertThrows(
        ModelVersionAliasesAlreadyExistException.class,
        () ->
            catalog
                .asModelCatalog()
                .linkModelVersion(
                    modelId,
                    uris,
                    new String[] {"alias1", "alias2"},
                    "comment",
                    Collections.emptyMap()),
        "model version already exists");

    // Throw RuntimeException
    ErrorResponse errResp3 = ErrorResponse.internalError("internal error");
    buildMockResource(
        Method.POST, modelVersionPath, request, errResp3, HttpStatus.SC_INTERNAL_SERVER_ERROR);

    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            catalog
                .asModelCatalog()
                .linkModelVersion(
                    modelId,
                    uris,
                    new String[] {"alias1", "alias2"},
                    "comment",
                    Collections.emptyMap()),
        "internal error");
  }

  @ParameterizedTest
  @ValueSource(strings = {"schema1/model1", "스키마1/모델1"})
  public void testDeleteModelVersion(String input) throws JsonProcessingException {
    String[] split = input.split("/");
    String schemaName = split[0];
    String modelName = split[1];
    NameIdentifier modelId = NameIdentifier.of(schemaName, modelName);
    String modelVersionPath =
        withSlash(
            GenericModelCatalog.formatModelVersionRequestPath(
                    NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaName, modelName))
                + "/versions/0");

    DropResponse resp = new DropResponse(true);
    buildMockResource(Method.DELETE, modelVersionPath, null, resp, HttpStatus.SC_OK);

    Assertions.assertTrue(catalog.asModelCatalog().deleteModelVersion(modelId, 0));

    // Test RuntimeException
    ErrorResponse errResp = ErrorResponse.internalError("internal error");
    buildMockResource(
        Method.DELETE, modelVersionPath, null, errResp, HttpStatus.SC_INTERNAL_SERVER_ERROR);

    Assertions.assertThrows(
        RuntimeException.class,
        () -> catalog.asModelCatalog().deleteModelVersion(modelId, 0),
        "internal error");
  }

  @ParameterizedTest
  @ValueSource(strings = {"schema1/model1/alias1", "스키마1/모델1/별칭1"})
  public void testDeleteModelVersionByAlias(String input) throws JsonProcessingException {
    String[] split = input.split("/");
    String schemaName = split[0];
    String modelName = split[1];
    String aliasName = split[2];
    NameIdentifier modelId = NameIdentifier.of(schemaName, modelName);
    String modelVersionPath =
        withSlash(
            GenericModelCatalog.formatModelVersionRequestPath(
                    NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaName, modelName))
                + "/aliases/"
                + RESTUtils.encodeString(aliasName));

    DropResponse resp = new DropResponse(true);
    buildMockResource(Method.DELETE, modelVersionPath, null, resp, HttpStatus.SC_OK);

    Assertions.assertTrue(catalog.asModelCatalog().deleteModelVersion(modelId, aliasName));

    // Test RuntimeException
    ErrorResponse errResp = ErrorResponse.internalError("internal error");
    buildMockResource(
        Method.DELETE, modelVersionPath, null, errResp, HttpStatus.SC_INTERNAL_SERVER_ERROR);

    Assertions.assertThrows(
        RuntimeException.class,
        () -> catalog.asModelCatalog().deleteModelVersion(modelId, aliasName),
        "internal error");
  }

  @ParameterizedTest
  @ValueSource(strings = {"schema1/model1/new_model1", "스키마1/모델1/새로운_모델1"})
  public void testAlterModel(String input) throws JsonProcessingException {
    String[] split = input.split("/");
    String schema = split[0];
    String oldName = split[1];
    String newName = split[2];
    String comment = "comment";

    NameIdentifier modelId = NameIdentifier.of(schema, oldName);
    String modelPath =
        withSlash(
            GenericModelCatalog.formatModelRequestPath(
                    Namespace.of(METALAKE_NAME, CATALOG_NAME, schema))
                + "/"
                + RESTUtils.encodeString(modelId.name()));

    // Test rename the model
    ModelUpdateRequest.RenameModelRequest renameModelReq =
        new ModelUpdateRequest.RenameModelRequest(newName);
    ModelDTO renamedDTO = mockModelDTO(newName, 0, comment, Collections.emptyMap());
    ModelResponse commentResp = new ModelResponse(renamedDTO);
    buildMockResource(
        Method.PUT,
        modelPath,
        new ModelUpdatesRequest(ImmutableList.of(renameModelReq)),
        commentResp,
        HttpStatus.SC_OK);
    Model renamedModel = catalog.asModelCatalog().alterModel(modelId, renameModelReq.modelChange());
    compareModel(renamedDTO, renamedModel);

    Assertions.assertEquals(newName, renamedModel.name());
    Assertions.assertEquals(comment, renamedModel.comment());
    Assertions.assertEquals(Collections.emptyMap(), renamedModel.properties());

    // Test NoSuchModelException
    ErrorResponse notFoundResp =
        ErrorResponse.notFound(NoSuchModelException.class.getSimpleName(), "model not found");
    buildMockResource(
        Method.PUT,
        modelPath,
        new ModelUpdatesRequest(ImmutableList.of(renameModelReq)),
        notFoundResp,
        HttpStatus.SC_NOT_FOUND);
    Assertions.assertThrows(
        NoSuchModelException.class,
        () -> catalog.asModelCatalog().alterModel(modelId, renameModelReq.modelChange()),
        "model not found");

    // Test RuntimeException
    ErrorResponse internalErrorResp = ErrorResponse.internalError("internal error");
    buildMockResource(
        Method.PUT,
        modelPath,
        new ModelUpdatesRequest(ImmutableList.of(renameModelReq)),
        internalErrorResp,
        HttpStatus.SC_INTERNAL_SERVER_ERROR);
    Assertions.assertThrows(
        RuntimeException.class,
        () -> catalog.asModelCatalog().alterModel(modelId, renameModelReq.modelChange()),
        "internal error");
  }

  @ParameterizedTest
  @ValueSource(strings = {"schema1/model1/alias1/alias2", "스키마1/모델1/별칭1/별칭2"})
  void testUpdateModelVersionComment(String input) throws JsonProcessingException {
    String[] split = input.split("/");
    String schemaName = split[0];
    String modelName = split[1];
    String[] aliases = new String[] {split[2], split[3]};
    String newComment = "new comment";
    String uri = "uri";
    int version = 0;

    NameIdentifier modelId = NameIdentifier.of(schemaName, modelName);
    String modelVersionPath =
        withSlash(
            GenericModelCatalog.formatModelVersionRequestPath(
                    NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaName, modelName))
                + "/versions/0");

    ModelVersionDTO mockModelVersion =
        mockModelVersion(version, uri, aliases, newComment, Collections.emptyMap());
    ModelVersionResponse resp = new ModelVersionResponse(mockModelVersion);
    ModelVersionUpdateRequest.UpdateModelVersionComment updateCommentReq =
        new ModelVersionUpdateRequest.UpdateModelVersionComment(newComment);

    buildMockResource(
        Method.PUT,
        modelVersionPath,
        new ModelVersionUpdatesRequest(ImmutableList.of(updateCommentReq)),
        resp,
        HttpStatus.SC_OK);

    ModelVersion updatedModelVersion =
        catalog
            .asModelCatalog()
            .alterModelVersion(modelId, version, updateCommentReq.modelVersionChange());
    compareModelVersion(mockModelVersion, updatedModelVersion);

    Assertions.assertEquals(uri, updatedModelVersion.uri());
    Assertions.assertEquals(newComment, updatedModelVersion.comment());
    Assertions.assertEquals(Collections.emptyMap(), updatedModelVersion.properties());
    Assertions.assertEquals(version, updatedModelVersion.version());
    Assertions.assertArrayEquals(aliases, updatedModelVersion.aliases());

    // Test NoSuchModelException
    ErrorResponse notFoundResp =
        ErrorResponse.notFound(
            NoSuchModelVersionException.class.getSimpleName(), "model version not found");
    buildMockResource(
        Method.PUT,
        modelVersionPath,
        new ModelVersionUpdatesRequest(ImmutableList.of(updateCommentReq)),
        notFoundResp,
        HttpStatus.SC_NOT_FOUND);
    Assertions.assertThrows(
        NoSuchModelVersionException.class,
        () ->
            catalog
                .asModelCatalog()
                .alterModelVersion(modelId, version, updateCommentReq.modelVersionChange()),
        "model not found");

    // Test RuntimeException
    ErrorResponse internalErrorResp = ErrorResponse.internalError("internal error");
    buildMockResource(
        Method.PUT,
        modelVersionPath,
        new ModelVersionUpdatesRequest(ImmutableList.of(updateCommentReq)),
        internalErrorResp,
        HttpStatus.SC_INTERNAL_SERVER_ERROR);
    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            catalog
                .asModelCatalog()
                .alterModelVersion(modelId, version, updateCommentReq.modelVersionChange()),
        "internal error");
  }

  @ParameterizedTest
  @ValueSource(strings = {"schema1/model1/alias1/alias2", "스키마1/모델1/별칭1/별칭2"})
  void testUpdateModelVersionCommentByAlias(String input) throws JsonProcessingException {
    String[] split = input.split("/");
    String schemaName = split[0];
    String modelName = split[1];
    String[] aliases = new String[] {split[2], split[3]};

    String newComment = "new comment";
    String uri = "uri";
    int version = 0;

    NameIdentifier modelId = NameIdentifier.of(schemaName, modelName);
    String modelVersionPath =
        withSlash(
            GenericModelCatalog.formatModelVersionRequestPath(
                    NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaName, modelName))
                + "/aliases/"
                + RESTUtils.encodeString(aliases[0]));

    ModelVersionDTO mockModelVersion =
        mockModelVersion(version, uri, aliases, newComment, Collections.emptyMap());
    ModelVersionResponse resp = new ModelVersionResponse(mockModelVersion);
    ModelVersionUpdateRequest.UpdateModelVersionComment updateCommentReq =
        new ModelVersionUpdateRequest.UpdateModelVersionComment(newComment);

    buildMockResource(
        Method.PUT,
        modelVersionPath,
        new ModelVersionUpdatesRequest(ImmutableList.of(updateCommentReq)),
        resp,
        HttpStatus.SC_OK);

    ModelVersion updatedModelVersion =
        catalog
            .asModelCatalog()
            .alterModelVersion(modelId, aliases[0], updateCommentReq.modelVersionChange());
    compareModelVersion(mockModelVersion, updatedModelVersion);

    Assertions.assertEquals(uri, updatedModelVersion.uri());
    Assertions.assertEquals(newComment, updatedModelVersion.comment());
    Assertions.assertEquals(Collections.emptyMap(), updatedModelVersion.properties());
    Assertions.assertEquals(version, updatedModelVersion.version());
    Assertions.assertArrayEquals(aliases, updatedModelVersion.aliases());

    // Test NoSuchModelException
    ErrorResponse notFoundResp =
        ErrorResponse.notFound(
            NoSuchModelVersionException.class.getSimpleName(), "model version not found");
    buildMockResource(
        Method.PUT,
        modelVersionPath,
        new ModelVersionUpdatesRequest(ImmutableList.of(updateCommentReq)),
        notFoundResp,
        HttpStatus.SC_NOT_FOUND);
    Assertions.assertThrows(
        NoSuchModelVersionException.class,
        () ->
            catalog
                .asModelCatalog()
                .alterModelVersion(modelId, aliases[0], updateCommentReq.modelVersionChange()),
        "model not found");

    // Test RuntimeException
    ErrorResponse internalErrorResp = ErrorResponse.internalError("internal error");
    buildMockResource(
        Method.PUT,
        modelVersionPath,
        new ModelVersionUpdatesRequest(ImmutableList.of(updateCommentReq)),
        internalErrorResp,
        HttpStatus.SC_INTERNAL_SERVER_ERROR);
    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            catalog
                .asModelCatalog()
                .alterModelVersion(modelId, aliases[0], updateCommentReq.modelVersionChange()),
        "internal error");
  }

  @ParameterizedTest
  @ValueSource(strings = {"schema1/model1/alias1/alias2", "스키마1/모델1/별칭1/별칭2"})
  void testUpdateModelVersionWithMultipleUris(String input) throws JsonProcessingException {
    String[] split = input.split("/");
    String schemaName = split[0];
    String modelName = split[1];
    String[] aliases = new String[] {split[2], split[3]};
    String comment = "comment";
    int version = 0;

    NameIdentifier modelId = NameIdentifier.of(schemaName, modelName);
    String modelVersionPath =
        withSlash(
            GenericModelCatalog.formatModelVersionRequestPath(
                    NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaName, modelName))
                + "/aliases/"
                + RESTUtils.encodeString(aliases[0]));

    // Test update uri
    Map<String, String> uris = ImmutableMap.of("n1", "u1", "n2", "u2");
    ModelVersionDTO mockModelVersion =
        mockModelVersion(version, uris, aliases, comment, Collections.emptyMap());
    ModelVersionResponse resp = new ModelVersionResponse(mockModelVersion);
    ModelVersionUpdateRequest.UpdateModelVersionUriRequest updateUri =
        new ModelVersionUpdateRequest.UpdateModelVersionUriRequest("n2", "u2");
    buildMockResource(
        Method.PUT,
        modelVersionPath,
        new ModelVersionUpdatesRequest(ImmutableList.of(updateUri)),
        resp,
        HttpStatus.SC_OK);
    ModelVersion updatedModelVersion =
        catalog
            .asModelCatalog()
            .alterModelVersion(modelId, aliases[0], updateUri.modelVersionChange());
    compareModelVersion(mockModelVersion, updatedModelVersion);
    Assertions.assertEquals(uris, updatedModelVersion.uris());
    Assertions.assertEquals(comment, updatedModelVersion.comment());
    Assertions.assertEquals(Collections.emptyMap(), updatedModelVersion.properties());
    Assertions.assertEquals(version, updatedModelVersion.version());
    Assertions.assertArrayEquals(aliases, updatedModelVersion.aliases());

    // Test add uri
    uris = ImmutableMap.of("n1", "u1", "n2", "u2");
    mockModelVersion = mockModelVersion(version, uris, aliases, comment, Collections.emptyMap());
    resp = new ModelVersionResponse(mockModelVersion);
    ModelVersionUpdateRequest.AddModelVersionUriRequest addUri =
        new ModelVersionUpdateRequest.AddModelVersionUriRequest("n2", "u2");
    buildMockResource(
        Method.PUT,
        modelVersionPath,
        new ModelVersionUpdatesRequest(ImmutableList.of(addUri)),
        resp,
        HttpStatus.SC_OK);
    updatedModelVersion =
        catalog
            .asModelCatalog()
            .alterModelVersion(modelId, aliases[0], addUri.modelVersionChange());
    compareModelVersion(mockModelVersion, updatedModelVersion);
    Assertions.assertEquals(uris, updatedModelVersion.uris());
    Assertions.assertEquals(comment, updatedModelVersion.comment());
    Assertions.assertEquals(Collections.emptyMap(), updatedModelVersion.properties());
    Assertions.assertEquals(version, updatedModelVersion.version());
    Assertions.assertArrayEquals(aliases, updatedModelVersion.aliases());

    // Test remove uri
    uris = ImmutableMap.of("n1", "u1");
    mockModelVersion = mockModelVersion(version, uris, aliases, comment, Collections.emptyMap());
    resp = new ModelVersionResponse(mockModelVersion);
    ModelVersionUpdateRequest.RemoveModelVersionUriRequest removeUri =
        new ModelVersionUpdateRequest.RemoveModelVersionUriRequest("n2");
    buildMockResource(
        Method.PUT,
        modelVersionPath,
        new ModelVersionUpdatesRequest(ImmutableList.of(removeUri)),
        resp,
        HttpStatus.SC_OK);
    updatedModelVersion =
        catalog
            .asModelCatalog()
            .alterModelVersion(modelId, aliases[0], removeUri.modelVersionChange());
    compareModelVersion(mockModelVersion, updatedModelVersion);
    Assertions.assertEquals(uris, updatedModelVersion.uris());
    Assertions.assertEquals(comment, updatedModelVersion.comment());
    Assertions.assertEquals(Collections.emptyMap(), updatedModelVersion.properties());
    Assertions.assertEquals(version, updatedModelVersion.version());
    Assertions.assertArrayEquals(aliases, updatedModelVersion.aliases());
  }

  @ParameterizedTest
  @ValueSource(strings = {"schema1/model1", "스키마1/모델1"})
  public void testGetModelVersionUri(String input) throws JsonProcessingException {
    String[] split = input.split("/");
    String schemaName = split[0];
    String modelName = split[1];
    NameIdentifier modelId = NameIdentifier.of(schemaName, modelName);

    int version = 0;
    String modelVersionUriPath =
        withSlash(
            GenericModelCatalog.formatModelVersionRequestPath(
                    NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaName, modelName))
                + "/versions/"
                + version
                + "/uri");
    String uriName = "name-s3";
    String uri = "s3://path/to/model";
    Map<String, String> params = ImmutableMap.of("uriName", uriName);
    ModelVersionUriResponse resp = new ModelVersionUriResponse(uri);
    buildMockResource(Method.GET, modelVersionUriPath, params, null, resp, HttpStatus.SC_OK);

    Assertions.assertEquals(
        uri, catalog.asModelCatalog().getModelVersionUri(modelId, version, uriName));
  }

  @ParameterizedTest
  @ValueSource(strings = {"schema1/model1", "스키마1/모델1"})
  public void testGetModelVersionUriByAlias(String input) throws JsonProcessingException {
    String[] split = input.split("/");
    String schemaName = split[0];
    String modelName = split[1];
    NameIdentifier modelId = NameIdentifier.of(schemaName, modelName);

    String alias = "alias1";
    String modelVersionUriPath =
        withSlash(
            GenericModelCatalog.formatModelVersionRequestPath(
                    NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaName, modelName))
                + "/aliases/"
                + alias
                + "/uri");
    String uriName = "name-s3";
    String uri = "s3://path/to/model";
    Map<String, String> params = ImmutableMap.of("uriName", uriName);
    ModelVersionUriResponse resp = new ModelVersionUriResponse(uri);
    buildMockResource(Method.GET, modelVersionUriPath, params, null, resp, HttpStatus.SC_OK);

    Assertions.assertEquals(
        uri, catalog.asModelCatalog().getModelVersionUri(modelId, alias, uriName));
  }

  private ModelDTO mockModelDTO(
      String modelName, int latestVersion, String comment, Map<String, String> properties) {
    return ModelDTO.builder()
        .withName(modelName)
        .withLatestVersion(latestVersion)
        .withComment(comment)
        .withProperties(properties)
        .withAudit(AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
        .build();
  }

  private ModelVersionDTO mockModelVersion(
      int version, String uri, String[] aliases, String comment, Map<String, String> properties) {
    return mockModelVersion(
        version, ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, uri), aliases, comment, properties);
  }

  private ModelVersionDTO mockModelVersion(
      int version,
      Map<String, String> uris,
      String[] aliases,
      String comment,
      Map<String, String> properties) {
    return ModelVersionDTO.builder()
        .withVersion(version)
        .withUris(uris)
        .withAliases(aliases)
        .withComment(comment)
        .withProperties(properties)
        .withAudit(AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
        .build();
  }

  private void compareModel(Model expect, Model result) {
    Assertions.assertEquals(expect.name(), result.name());
    Assertions.assertEquals(expect.latestVersion(), result.latestVersion());
    Assertions.assertEquals(expect.comment(), result.comment());
    Assertions.assertEquals(expect.properties(), result.properties());
  }

  private void compareModelVersion(ModelVersion expect, ModelVersion result) {
    Assertions.assertEquals(expect.version(), result.version());
    Assertions.assertEquals(expect.uri(), result.uri());
    Assertions.assertEquals(expect.uris(), result.uris());
    Assertions.assertArrayEquals(expect.aliases(), result.aliases());
    Assertions.assertEquals(expect.comment(), result.comment());
    Assertions.assertEquals(expect.properties(), result.properties());
  }
}
