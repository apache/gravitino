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
package org.apache.gravitino.server.web.rest;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.ModelDispatcher;
import org.apache.gravitino.dto.requests.ModelRegisterRequest;
import org.apache.gravitino.dto.requests.ModelVersionLinkRequest;
import org.apache.gravitino.dto.responses.BaseResponse;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.ModelResponse;
import org.apache.gravitino.dto.responses.ModelVersionListResponse;
import org.apache.gravitino.dto.responses.ModelVersionResponse;
import org.apache.gravitino.exceptions.ModelAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchModelException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.model.ModelVersion;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestModelOperations extends JerseyTest {

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {

    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

  private ModelDispatcher modelDispatcher = mock(ModelDispatcher.class);

  private AuditInfo testAuditInfo =
      AuditInfo.builder().withCreator("user1").withCreateTime(Instant.now()).build();

  private Map<String, String> properties = ImmutableMap.of("key1", "value");

  private String metalake = "metalake_for_model_test";

  private String catalog = "catalog_for_model_test";

  private String schema = "schema_for_model_test";

  private Namespace modelNs = NamespaceUtil.ofModel(metalake, catalog, schema);

  @Override
  protected Application configure() {
    try {
      forceSet(
          TestProperties.CONTAINER_PORT, String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(ModelOperations.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(modelDispatcher).to(ModelDispatcher.class).ranked(2);
            bindFactory(TestModelOperations.MockServletRequestFactory.class)
                .to(HttpServletRequest.class);
          }
        });

    return resourceConfig;
  }

  @Test
  public void testListModels() {
    NameIdentifier modelId1 = NameIdentifierUtil.ofModel(metalake, catalog, schema, "model1");
    NameIdentifier modelId2 = NameIdentifierUtil.ofModel(metalake, catalog, schema, "model2");
    NameIdentifier[] modelIds = new NameIdentifier[] {modelId1, modelId2};
    when(modelDispatcher.listModels(modelNs)).thenReturn(modelIds);

    Response response =
        target(modelPath())
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMediaType());

    EntityListResponse resp = response.readEntity(EntityListResponse.class);
    Assertions.assertEquals(0, resp.getCode());
    Assertions.assertArrayEquals(modelIds, resp.identifiers());

    // Test mock return null for listModels
    when(modelDispatcher.listModels(modelNs)).thenReturn(null);
    Response resp1 =
        target(modelPath())
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    EntityListResponse resp2 = resp1.readEntity(EntityListResponse.class);
    Assertions.assertEquals(0, resp2.getCode());
    Assertions.assertEquals(0, resp2.identifiers().length);

    // Test mock return empty array for listModels
    when(modelDispatcher.listModels(modelNs)).thenReturn(new NameIdentifier[0]);
    Response resp3 =
        target(modelPath())
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp3.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp3.getMediaType());

    EntityListResponse resp4 = resp3.readEntity(EntityListResponse.class);
    Assertions.assertEquals(0, resp4.getCode());
    Assertions.assertEquals(0, resp4.identifiers().length);

    // Test mock throw NoSuchSchemaException
    doThrow(new NoSuchSchemaException("mock error")).when(modelDispatcher).listModels(modelNs);
    Response resp5 =
        target(modelPath())
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp5.getStatus());

    ErrorResponse errorResp = resp5.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchSchemaException.class.getSimpleName(), errorResp.getType());

    // Test mock throw RuntimeException
    doThrow(new RuntimeException("mock error")).when(modelDispatcher).listModels(modelNs);
    Response resp6 =
        target(modelPath())
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp6.getStatus());

    ErrorResponse errorResp1 = resp6.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp1.getType());
  }

  @Test
  public void testGetModel() {
    Model mockModel = mockModel("model1", "comment1", 0);
    NameIdentifier modelId = NameIdentifierUtil.ofModel(metalake, catalog, schema, "model1");
    when(modelDispatcher.getModel(modelId)).thenReturn(mockModel);

    Response resp =
        target(modelPath())
            .path("model1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    ModelResponse modelResp = resp.readEntity(ModelResponse.class);
    Assertions.assertEquals(0, modelResp.getCode());

    Model resultModel = modelResp.getModel();
    compare(mockModel, resultModel);

    // Test mock throw NoSuchModelException
    doThrow(new NoSuchModelException("mock error")).when(modelDispatcher).getModel(modelId);
    Response resp1 =
        target(modelPath())
            .path("model1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResp = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchModelException.class.getSimpleName(), errorResp.getType());

    // Test mock throw RuntimeException
    doThrow(new RuntimeException("mock error")).when(modelDispatcher).getModel(modelId);
    Response resp2 =
        target(modelPath())
            .path("model1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp1 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp1.getType());
  }

  @Test
  public void testRegisterModel() {
    NameIdentifier modelId = NameIdentifierUtil.ofModel(metalake, catalog, schema, "model1");
    Model mockModel = mockModel("model1", "comment1", 0);
    when(modelDispatcher.registerModel(modelId, "comment1", properties)).thenReturn(mockModel);

    ModelRegisterRequest req = new ModelRegisterRequest("model1", "comment1", properties);
    Response resp =
        target(modelPath())
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    ModelResponse modelResp = resp.readEntity(ModelResponse.class);
    Assertions.assertEquals(0, modelResp.getCode());
    compare(mockModel, modelResp.getModel());

    // Test mock throw NoSuchSchemaException
    doThrow(new NoSuchSchemaException("mock error"))
        .when(modelDispatcher)
        .registerModel(modelId, "comment1", properties);

    Response resp1 =
        target(modelPath())
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResp = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchSchemaException.class.getSimpleName(), errorResp.getType());

    // Test mock throw ModelAlreadyExistsException
    doThrow(new ModelAlreadyExistsException("mock error"))
        .when(modelDispatcher)
        .registerModel(modelId, "comment1", properties);

    Response resp2 =
        target(modelPath())
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp1 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ALREADY_EXISTS_CODE, errorResp1.getCode());
    Assertions.assertEquals(
        ModelAlreadyExistsException.class.getSimpleName(), errorResp1.getType());

    // Test mock throw RuntimeException
    doThrow(new RuntimeException("mock error"))
        .when(modelDispatcher)
        .registerModel(modelId, "comment1", properties);

    Response resp3 =
        target(modelPath())
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResp2 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp2.getType());
  }

  @Test
  public void testDeleteModel() {
    NameIdentifier modelId = NameIdentifierUtil.ofModel(metalake, catalog, schema, "model1");
    when(modelDispatcher.deleteModel(modelId)).thenReturn(true);

    Response resp =
        target(modelPath())
            .path("model1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    DropResponse dropResp = resp.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResp.getCode());
    Assertions.assertTrue(dropResp.dropped());

    // Test mock return false for deleteModel
    when(modelDispatcher.deleteModel(modelId)).thenReturn(false);
    Response resp1 =
        target(modelPath())
            .path("model1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    DropResponse dropResp1 = resp1.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResp1.getCode());
    Assertions.assertFalse(dropResp1.dropped());

    // Test mock throw RuntimeException
    doThrow(new RuntimeException("mock error")).when(modelDispatcher).deleteModel(modelId);
    Response resp2 =
        target(modelPath())
            .path("model1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp1 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp1.getType());
  }

  @Test
  public void testListModelVersions() {
    NameIdentifier modelId = NameIdentifierUtil.ofModel(metalake, catalog, schema, "model1");
    int[] versions = new int[] {0, 1, 2};
    when(modelDispatcher.listModelVersions(modelId)).thenReturn(versions);

    Response resp =
        target(modelPath())
            .path("model1")
            .path("versions")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    ModelVersionListResponse versionListResp = resp.readEntity(ModelVersionListResponse.class);
    Assertions.assertEquals(0, versionListResp.getCode());
    Assertions.assertArrayEquals(versions, versionListResp.getVersions());

    // Test mock return null for listModelVersions
    when(modelDispatcher.listModelVersions(modelId)).thenReturn(null);
    Response resp1 =
        target(modelPath())
            .path("model1")
            .path("versions")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    ModelVersionListResponse versionListResp1 = resp1.readEntity(ModelVersionListResponse.class);
    Assertions.assertEquals(0, versionListResp1.getCode());
    Assertions.assertEquals(0, versionListResp1.getVersions().length);

    // Test mock return empty array for listModelVersions
    when(modelDispatcher.listModelVersions(modelId)).thenReturn(new int[0]);
    Response resp2 =
        target(modelPath())
            .path("model1")
            .path("versions")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp2.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp2.getMediaType());

    ModelVersionListResponse versionListResp2 = resp2.readEntity(ModelVersionListResponse.class);
    Assertions.assertEquals(0, versionListResp2.getCode());
    Assertions.assertEquals(0, versionListResp2.getVersions().length);

    // Test mock throw NoSuchModelException
    doThrow(new NoSuchModelException("mock error"))
        .when(modelDispatcher)
        .listModelVersions(modelId);
    Response resp3 =
        target(modelPath())
            .path("model1")
            .path("versions")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResp = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchModelException.class.getSimpleName(), errorResp.getType());

    // Test mock throw RuntimeException
    doThrow(new RuntimeException("mock error")).when(modelDispatcher).listModelVersions(modelId);
    Response resp4 =
        target(modelPath())
            .path("model1")
            .path("versions")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp4.getStatus());

    ErrorResponse errorResp1 = resp4.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp1.getType());
  }

  @Test
  public void testGetModelVersion() {
    NameIdentifier modelIdent = NameIdentifierUtil.ofModel(metalake, catalog, schema, "model1");
    ModelVersion mockModelVersion =
        mockModelVersion(0, "uri1", new String[] {"alias1"}, "comment1");
    when(modelDispatcher.getModelVersion(modelIdent, 0)).thenReturn(mockModelVersion);

    Response resp =
        target(modelPath())
            .path("model1")
            .path("versions")
            .path("0")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    ModelVersionResponse versionResp = resp.readEntity(ModelVersionResponse.class);
    Assertions.assertEquals(0, versionResp.getCode());
    compare(mockModelVersion, versionResp.getModelVersion());

    // Test mock throw NoSuchModelVersionException
    doThrow(new NoSuchModelException("mock error"))
        .when(modelDispatcher)
        .getModelVersion(modelIdent, 0);

    Response resp1 =
        target(modelPath())
            .path("model1")
            .path("versions")
            .path("0")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResp = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchModelException.class.getSimpleName(), errorResp.getType());

    // Test mock throw RuntimeException
    doThrow(new RuntimeException("mock error"))
        .when(modelDispatcher)
        .getModelVersion(modelIdent, 0);

    Response resp2 =
        target(modelPath())
            .path("model1")
            .path("versions")
            .path("0")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp1 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp1.getType());

    // Test get model version by alias
    when(modelDispatcher.getModelVersion(modelIdent, "alias1")).thenReturn(mockModelVersion);

    Response resp3 =
        target(modelPath())
            .path("model1")
            .path("aliases")
            .path("alias1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp3.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp3.getMediaType());

    ModelVersionResponse versionResp1 = resp3.readEntity(ModelVersionResponse.class);
    Assertions.assertEquals(0, versionResp1.getCode());
    compare(mockModelVersion, versionResp1.getModelVersion());

    // Test mock throw NoSuchModelVersionException
    doThrow(new NoSuchModelException("mock error"))
        .when(modelDispatcher)
        .getModelVersion(modelIdent, "alias1");

    Response resp4 =
        target(modelPath())
            .path("model1")
            .path("aliases")
            .path("alias1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp4.getStatus());

    ErrorResponse errorResp2 = resp4.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp2.getCode());
    Assertions.assertEquals(NoSuchModelException.class.getSimpleName(), errorResp2.getType());

    // Test mock throw RuntimeException
    doThrow(new RuntimeException("mock error"))
        .when(modelDispatcher)
        .getModelVersion(modelIdent, "alias1");

    Response resp5 =
        target(modelPath())
            .path("model1")
            .path("aliases")
            .path("alias1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp5.getStatus());

    ErrorResponse errorResp3 = resp5.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp3.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp3.getType());
  }

  @Test
  public void testLinkModelVersion() {
    NameIdentifier modelIdent = NameIdentifierUtil.ofModel(metalake, catalog, schema, "model1");
    doNothing()
        .when(modelDispatcher)
        .linkModelVersion(modelIdent, "uri1", new String[] {"alias1"}, "comment1", properties);

    ModelVersionLinkRequest req =
        new ModelVersionLinkRequest("uri1", new String[] {"alias1"}, "comment1", properties);

    Response resp =
        target(modelPath())
            .path("model1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    BaseResponse baseResponse = resp.readEntity(BaseResponse.class);
    Assertions.assertEquals(0, baseResponse.getCode());

    // Test mock throw NoSuchModelException
    doThrow(new NoSuchModelException("mock error"))
        .when(modelDispatcher)
        .linkModelVersion(modelIdent, "uri1", new String[] {"alias1"}, "comment1", properties);

    Response resp1 =
        target(modelPath())
            .path("model1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResp = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchModelException.class.getSimpleName(), errorResp.getType());

    // Test mock throw ModelVersionAliasesAlreadyExistException
    doThrow(new ModelAlreadyExistsException("mock error"))
        .when(modelDispatcher)
        .linkModelVersion(modelIdent, "uri1", new String[] {"alias1"}, "comment1", properties);

    Response resp2 =
        target(modelPath())
            .path("model1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp1 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ALREADY_EXISTS_CODE, errorResp1.getCode());
    Assertions.assertEquals(
        ModelAlreadyExistsException.class.getSimpleName(), errorResp1.getType());

    // Test mock throw RuntimeException
    doThrow(new RuntimeException("mock error"))
        .when(modelDispatcher)
        .linkModelVersion(modelIdent, "uri1", new String[] {"alias1"}, "comment1", properties);

    Response resp3 =
        target(modelPath())
            .path("model1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResp2 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp2.getType());
  }

  @Test
  public void testDeleteModelVersion() {
    NameIdentifier modelIdent = NameIdentifierUtil.ofModel(metalake, catalog, schema, "model1");
    when(modelDispatcher.deleteModelVersion(modelIdent, 0)).thenReturn(true);

    Response resp =
        target(modelPath())
            .path("model1")
            .path("versions")
            .path("0")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    DropResponse dropResp = resp.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResp.getCode());
    Assertions.assertTrue(dropResp.dropped());

    // Test mock return false for deleteModelVersion
    when(modelDispatcher.deleteModelVersion(modelIdent, 0)).thenReturn(false);

    Response resp1 =
        target(modelPath())
            .path("model1")
            .path("versions")
            .path("0")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    DropResponse dropResp1 = resp1.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResp1.getCode());
    Assertions.assertFalse(dropResp1.dropped());

    // Test mock return true for deleteModelVersion using alias
    when(modelDispatcher.deleteModelVersion(modelIdent, "alias1")).thenReturn(true);

    Response resp2 =
        target(modelPath())
            .path("model1")
            .path("aliases")
            .path("alias1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp2.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp2.getMediaType());

    DropResponse dropResp2 = resp2.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResp2.getCode());

    // Test mock return false for deleteModelVersion using alias
    when(modelDispatcher.deleteModelVersion(modelIdent, "alias1")).thenReturn(false);

    Response resp3 =
        target(modelPath())
            .path("model1")
            .path("aliases")
            .path("alias1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp3.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp3.getMediaType());

    DropResponse dropResp3 = resp3.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResp3.getCode());
    Assertions.assertFalse(dropResp3.dropped());

    // Test mock throw RuntimeException
    doThrow(new RuntimeException("mock error"))
        .when(modelDispatcher)
        .deleteModelVersion(modelIdent, 0);

    Response resp4 =
        target(modelPath())
            .path("model1")
            .path("versions")
            .path("0")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp4.getStatus());

    ErrorResponse errorResp1 = resp4.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp1.getType());

    // Test mock throw RuntimeException using alias
    doThrow(new RuntimeException("mock error"))
        .when(modelDispatcher)
        .deleteModelVersion(modelIdent, "alias1");

    Response resp5 =
        target(modelPath())
            .path("model1")
            .path("aliases")
            .path("alias1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp5.getStatus());

    ErrorResponse errorResp2 = resp5.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp2.getType());
  }

  private String modelPath() {
    return "/metalakes/" + metalake + "/catalogs/" + catalog + "/schemas/" + schema + "/models";
  }

  private Model mockModel(String modelName, String comment, int latestVersion) {
    Model mockModel = mock(Model.class);
    when(mockModel.name()).thenReturn(modelName);
    when(mockModel.comment()).thenReturn(comment);
    when(mockModel.latestVersion()).thenReturn(latestVersion);
    when(mockModel.properties()).thenReturn(properties);
    when(mockModel.auditInfo()).thenReturn(testAuditInfo);
    return mockModel;
  }

  private ModelVersion mockModelVersion(int version, String uri, String[] aliases, String comment) {
    ModelVersion mockModelVersion = mock(ModelVersion.class);
    when(mockModelVersion.version()).thenReturn(version);
    when(mockModelVersion.uri()).thenReturn(uri);
    when(mockModelVersion.aliases()).thenReturn(aliases);
    when(mockModelVersion.comment()).thenReturn(comment);
    when(mockModelVersion.properties()).thenReturn(properties);
    when(mockModelVersion.auditInfo()).thenReturn(testAuditInfo);
    return mockModelVersion;
  }

  private void compare(Model left, Model right) {
    Assertions.assertEquals(left.name(), right.name());
    Assertions.assertEquals(left.comment(), right.comment());
    Assertions.assertEquals(left.properties(), right.properties());

    Assertions.assertNotNull(right.auditInfo());
    Assertions.assertEquals(left.auditInfo().creator(), right.auditInfo().creator());
    Assertions.assertEquals(left.auditInfo().createTime(), right.auditInfo().createTime());
    Assertions.assertEquals(left.auditInfo().lastModifier(), right.auditInfo().lastModifier());
    Assertions.assertEquals(
        left.auditInfo().lastModifiedTime(), right.auditInfo().lastModifiedTime());
  }

  private void compare(ModelVersion left, ModelVersion right) {
    Assertions.assertEquals(left.version(), right.version());
    Assertions.assertEquals(left.uri(), right.uri());
    Assertions.assertArrayEquals(left.aliases(), right.aliases());
    Assertions.assertEquals(left.comment(), right.comment());
    Assertions.assertEquals(left.properties(), right.properties());

    Assertions.assertNotNull(right.auditInfo());
    Assertions.assertEquals(left.auditInfo().creator(), right.auditInfo().creator());
    Assertions.assertEquals(left.auditInfo().createTime(), right.auditInfo().createTime());
    Assertions.assertEquals(left.auditInfo().lastModifier(), right.auditInfo().lastModifier());
    Assertions.assertEquals(
        left.auditInfo().lastModifiedTime(), right.auditInfo().lastModifiedTime());
  }
}
