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
import org.apache.gravitino.dto.requests.ModelVersionLinkRequest;
import org.apache.gravitino.dto.responses.BaseResponse;
import org.apache.gravitino.dto.responses.CatalogResponse;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.ModelResponse;
import org.apache.gravitino.dto.responses.ModelVersionListResponse;
import org.apache.gravitino.dto.responses.ModelVersionResponse;
import org.apache.gravitino.exceptions.ModelAlreadyExistsException;
import org.apache.gravitino.exceptions.ModelVersionAliasesAlreadyExistException;
import org.apache.gravitino.exceptions.NoSuchModelException;
import org.apache.gravitino.exceptions.NoSuchModelVersionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.model.ModelVersion;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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

  @Test
  public void testListModels() throws JsonProcessingException {
    NameIdentifier modelId1 = NameIdentifier.of("schema1", "model1");
    NameIdentifier modelId2 = NameIdentifier.of("schema1", "model2");

    NameIdentifier resultModelId1 =
        NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, "schema1", "model1");
    NameIdentifier resultModelId2 =
        NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, "schema1", "model2");

    String modelPath =
        withSlash(
            GenericModelCatalog.formatModelRequestPath(
                Namespace.of(METALAKE_NAME, CATALOG_NAME, "schema1")));

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

  @Test
  public void testGetModel() throws JsonProcessingException {
    NameIdentifier modelId = NameIdentifier.of("schema1", "model1");
    String modelPath =
        withSlash(
            GenericModelCatalog.formatModelRequestPath(
                    Namespace.of(METALAKE_NAME, CATALOG_NAME, "schema1"))
                + "/"
                + modelId.name());

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

  @Test
  public void testRegisterModel() throws JsonProcessingException {
    NameIdentifier modelId = NameIdentifier.of("schema1", "model1");
    ModelDTO modelDTO = mockModelDTO("model1", 0, "model comment", Collections.emptyMap());
    ModelResponse resp = new ModelResponse(modelDTO);
    ModelRegisterRequest request =
        new ModelRegisterRequest(modelId.name(), "model comment", Collections.emptyMap());

    String modelPath =
        withSlash(
            GenericModelCatalog.formatModelRequestPath(
                Namespace.of(METALAKE_NAME, CATALOG_NAME, "schema1")));
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

  @Test
  public void testDeleteModel() throws JsonProcessingException {
    NameIdentifier modelId = NameIdentifier.of("schema1", "model1");
    String modelPath =
        withSlash(
            GenericModelCatalog.formatModelRequestPath(
                    Namespace.of(METALAKE_NAME, CATALOG_NAME, "schema1"))
                + "/"
                + modelId.name());

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

  @Test
  public void testListModelVersions() throws JsonProcessingException {
    NameIdentifier modelId = NameIdentifier.of("schema1", "model1");
    String modelVersionPath =
        withSlash(
            GenericModelCatalog.formatModelVersionRequestPath(
                    NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, "schema1", "model1"))
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

  @Test
  public void testGetModelVersion() throws JsonProcessingException {
    NameIdentifier modelId = NameIdentifier.of("schema1", "model1");
    String modelVersionPath =
        withSlash(
            GenericModelCatalog.formatModelVersionRequestPath(
                    NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, "schema1", "model1"))
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

  @Test
  public void testGetModelVersionByAlias() throws JsonProcessingException {
    NameIdentifier modelId = NameIdentifier.of("schema1", "model1");
    String modelVersionPath =
        withSlash(
            GenericModelCatalog.formatModelVersionRequestPath(
                    NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, "schema1", "model1"))
                + "/aliases/alias1");

    ModelVersionDTO mockModelVersion =
        mockModelVersion(
            0, "uri", new String[] {"alias1", "alias2"}, "comment", Collections.emptyMap());
    ModelVersionResponse resp = new ModelVersionResponse(mockModelVersion);
    buildMockResource(Method.GET, modelVersionPath, null, resp, HttpStatus.SC_OK);

    ModelVersion modelVersion = catalog.asModelCatalog().getModelVersion(modelId, "alias1");
    compareModelVersion(mockModelVersion, modelVersion);

    // Throw model version not found exception
    ErrorResponse errResp =
        ErrorResponse.notFound(
            NoSuchModelVersionException.class.getSimpleName(), "model version not found");
    buildMockResource(Method.GET, modelVersionPath, null, errResp, HttpStatus.SC_NOT_FOUND);

    Assertions.assertThrows(
        NoSuchModelVersionException.class,
        () -> catalog.asModelCatalog().getModelVersion(modelId, "alias1"),
        "model version not found");

    // Throw RuntimeException
    ErrorResponse errResp2 = ErrorResponse.internalError("internal error");
    buildMockResource(
        Method.GET, modelVersionPath, null, errResp2, HttpStatus.SC_INTERNAL_SERVER_ERROR);

    Assertions.assertThrows(
        RuntimeException.class,
        () -> catalog.asModelCatalog().getModelVersion(modelId, "alias1"),
        "internal error");
  }

  @Test
  public void testLinkModelVersion() throws JsonProcessingException {
    NameIdentifier modelId = NameIdentifier.of("schema1", "model1");
    String modelVersionPath =
        withSlash(
            GenericModelCatalog.formatModelVersionRequestPath(
                NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, "schema1", "model1")));

    ModelVersionLinkRequest request =
        new ModelVersionLinkRequest(
            "uri", new String[] {"alias1", "alias2"}, "comment", Collections.emptyMap());
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

  @Test
  public void testDeleteModelVersion() throws JsonProcessingException {
    NameIdentifier modelId = NameIdentifier.of("schema1", "model1");
    String modelVersionPath =
        withSlash(
            GenericModelCatalog.formatModelVersionRequestPath(
                    NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, "schema1", "model1"))
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

  @Test
  public void testDeleteModelVersionByAlias() throws JsonProcessingException {
    NameIdentifier modelId = NameIdentifier.of("schema1", "model1");
    String modelVersionPath =
        withSlash(
            GenericModelCatalog.formatModelVersionRequestPath(
                    NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, "schema1", "model1"))
                + "/aliases/alias1");

    DropResponse resp = new DropResponse(true);
    buildMockResource(Method.DELETE, modelVersionPath, null, resp, HttpStatus.SC_OK);

    Assertions.assertTrue(catalog.asModelCatalog().deleteModelVersion(modelId, "alias1"));

    // Test RuntimeException
    ErrorResponse errResp = ErrorResponse.internalError("internal error");
    buildMockResource(
        Method.DELETE, modelVersionPath, null, errResp, HttpStatus.SC_INTERNAL_SERVER_ERROR);

    Assertions.assertThrows(
        RuntimeException.class,
        () -> catalog.asModelCatalog().deleteModelVersion(modelId, "alias1"),
        "internal error");
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
    return ModelVersionDTO.builder()
        .withVersion(version)
        .withUri(uri)
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
    Assertions.assertArrayEquals(expect.aliases(), result.aliases());
    Assertions.assertEquals(expect.comment(), result.comment());
    Assertions.assertEquals(expect.properties(), result.properties());
  }
}
