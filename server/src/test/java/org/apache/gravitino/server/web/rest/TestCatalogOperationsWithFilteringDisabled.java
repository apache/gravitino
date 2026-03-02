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

import static org.apache.gravitino.Catalog.PROPERTY_IN_USE;
import static org.apache.gravitino.Configs.CACHE_ENABLED;
import static org.apache.gravitino.Configs.ENABLE_AUTHORIZATION;
import static org.apache.gravitino.Configs.FILTER_SENSITIVE_PROPERTIES;
import static org.apache.gravitino.Configs.GRAVITINO_AUTHORIZATION_THREAD_POOL_SIZE;
import static org.apache.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static org.apache.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static org.apache.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.security.Principal;
import java.time.Instant;
import java.util.Collections;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.dto.CatalogDTO;
import org.apache.gravitino.dto.responses.CatalogListResponse;
import org.apache.gravitino.dto.responses.CatalogResponse;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Test class for {@link CatalogOperations} with filterSensitiveProperties=false.
 *
 * <p>This test verifies that when sensitive property filtering is disabled, authorization filtering
 * still works correctly and all authorized users see all properties (including sensitive ones).
 */
public class TestCatalogOperationsWithFilteringDisabled extends JerseyTest {

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {
    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

  private CatalogManager manager = mock(CatalogManager.class);

  @BeforeAll
  public static void setup() throws IllegalAccessException {
    Config config = mock(Config.class);
    Mockito.doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    Mockito.doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    Mockito.doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
    // Explicitly set filterSensitiveProperties to false for this test
    Mockito.doReturn(false).when(config).get(FILTER_SENSITIVE_PROPERTIES);
    Mockito.doReturn(false).when(config).get(CACHE_ENABLED);
    Mockito.doReturn(true).when(config).get(ENABLE_AUTHORIZATION);
    // Mock thread pool size - required when ENABLE_AUTHORIZATION is true
    Mockito.doReturn(100).when(config).get(GRAVITINO_AUTHORIZATION_THREAD_POOL_SIZE);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", config, true);

    // Initialize GravitinoAuthorizerProvider with a mock authorizer
    GravitinoAuthorizerProvider authorizerProvider = GravitinoAuthorizerProvider.getInstance();
    GravitinoAuthorizer mockAuthorizer = mock(GravitinoAuthorizer.class);

    // Mock authorize to filter catalogs based on name (simulating real authorization)
    // Only allow access to catalog1 and catalog3, deny catalog2
    when(mockAuthorizer.authorize(
            any(Principal.class),
            any(String.class),
            any(MetadataObject.class),
            any(Privilege.Name.class),
            any(AuthorizationRequestContext.class)))
        .thenAnswer(
            invocation -> {
              MetadataObject metadataObject = invocation.getArgument(2);
              String catalogName = metadataObject.name();
              // Simulate authorization: allow catalog1 and catalog3, deny catalog2
              return "catalog1".equals(catalogName) || "catalog3".equals(catalogName);
            });

    // Mock isOwner to return true only for catalog1 (owner has full access including sensitive
    // props)
    // catalog3 will have USE_CATALOG permission but not owner
    when(mockAuthorizer.isOwner(
            any(Principal.class),
            any(String.class),
            any(MetadataObject.class),
            any(AuthorizationRequestContext.class)))
        .thenAnswer(
            invocation -> {
              MetadataObject metadataObject = invocation.getArgument(2);
              String catalogName = metadataObject.name();
              // Only catalog1 owner, catalog3 is not owner
              return "catalog1".equals(catalogName);
            });

    try {
      FieldUtils.writeField(authorizerProvider, "gravitinoAuthorizer", mockAuthorizer, true);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Failed to set mock authorizer", e);
    }
  }

  @AfterAll
  public static void cleanup() throws IOException {
    GravitinoAuthorizerProvider.getInstance().close();
  }

  @Override
  protected Application configure() {
    try {
      forceSet(
          TestProperties.CONTAINER_PORT, String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(CatalogOperations.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(manager).to(CatalogDispatcher.class).ranked(2);
            bindFactory(MockServletRequestFactory.class).to(HttpServletRequest.class);
          }
        });

    return resourceConfig;
  }

  @Test
  public void testListCatalogsInfoWithFilteringDisabled() {
    // Create catalogs with sensitive properties
    TestCatalog catalog1 = buildCatalogWithSensitiveProps("metalake1", "catalog1");
    TestCatalog catalog2 = buildCatalogWithSensitiveProps("metalake1", "catalog2");

    when(manager.listCatalogsInfo(any())).thenReturn(new Catalog[] {catalog1, catalog2});

    Response resp =
        target("/metalakes/metalake1/catalogs")
            .queryParam("details", "true")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    CatalogListResponse catalogResponse = resp.readEntity(CatalogListResponse.class);
    Assertions.assertEquals(0, catalogResponse.getCode());

    CatalogDTO[] catalogDTOs = catalogResponse.getCatalogs();
    // Authorization filtering: catalog2 is filtered out (no permission), only catalog1 is returned
    Assertions.assertEquals(
        1,
        catalogDTOs.length,
        "Should only return authorized catalogs (catalog1), catalog2 should be filtered out");

    // Verify catalog1 - all properties should be visible including sensitive ones
    // (catalog1 owner has full access)
    CatalogDTO catalogDTO1 = catalogDTOs[0];
    Assertions.assertEquals("catalog1", catalogDTO1.name());
    Assertions.assertEquals(Catalog.Type.RELATIONAL, catalogDTO1.type());
    Assertions.assertEquals("comment", catalogDTO1.comment());

    // When filterSensitiveProperties=false, all properties should be present
    Assertions.assertNotNull(catalogDTO1.properties());
    Assertions.assertTrue(
        catalogDTO1.properties().containsKey("jdbc-password"),
        "Sensitive property 'jdbc-password' should be visible when filtering is disabled");
    Assertions.assertTrue(
        catalogDTO1.properties().containsKey("s3-secret-key"),
        "Sensitive property 's3-secret-key' should be visible when filtering is disabled");
    Assertions.assertEquals("secret123", catalogDTO1.properties().get("jdbc-password"));
    Assertions.assertEquals("aws-secret", catalogDTO1.properties().get("s3-secret-key"));
  }

  @Test
  public void testLoadCatalogWithFilteringDisabled() {
    // Create catalog with sensitive properties
    TestCatalog catalog = buildCatalogWithSensitiveProps("metalake1", "catalog1");

    when(manager.loadCatalog(any())).thenReturn(catalog);

    Response resp =
        target("/metalakes/metalake1/catalogs/catalog1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    CatalogResponse catalogResponse = resp.readEntity(CatalogResponse.class);
    Assertions.assertEquals(0, catalogResponse.getCode());

    CatalogDTO catalogDTO = catalogResponse.getCatalog();
    Assertions.assertEquals("catalog1", catalogDTO.name());

    // When filterSensitiveProperties=false, all properties should be present
    Assertions.assertNotNull(catalogDTO.properties());
    Assertions.assertTrue(
        catalogDTO.properties().containsKey("jdbc-password"),
        "Sensitive property 'jdbc-password' should be visible when filtering is disabled");
    Assertions.assertTrue(
        catalogDTO.properties().containsKey("s3-secret-key"),
        "Sensitive property 's3-secret-key' should be visible when filtering is disabled");
    Assertions.assertEquals("secret123", catalogDTO.properties().get("jdbc-password"));
    Assertions.assertEquals("aws-secret", catalogDTO.properties().get("s3-secret-key"));
  }

  @Test
  public void testAuthorizationStillWorksWithFilteringDisabled() {
    // This test verifies that authorization filtering is not bypassed when
    // filterSensitiveProperties=false
    // Mock authorizer allows catalog1 (owner) and catalog3 (USE_CATALOG), but denies catalog2

    // Create multiple catalogs
    TestCatalog catalog1 = buildCatalogWithSensitiveProps("metalake1", "catalog1");
    TestCatalog catalog2 = buildCatalogWithSensitiveProps("metalake1", "catalog2");
    TestCatalog catalog3 = buildCatalogWithSensitiveProps("metalake1", "catalog3");

    when(manager.listCatalogsInfo(any())).thenReturn(new Catalog[] {catalog1, catalog2, catalog3});

    Response resp =
        target("/metalakes/metalake1/catalogs")
            .queryParam("details", "true")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    CatalogListResponse catalogResponse = resp.readEntity(CatalogListResponse.class);

    CatalogDTO[] catalogDTOs = catalogResponse.getCatalogs();
    // Authorization filtering: only catalog1 and catalog3 should be returned
    // catalog2 is filtered out (no permission)
    Assertions.assertEquals(
        2,
        catalogDTOs.length,
        "Should return 2 catalogs (catalog1 and catalog3), catalog2 should be filtered by authorization");

    // Verify returned catalogs are catalog1 and catalog3
    Assertions.assertEquals("catalog1", catalogDTOs[0].name());
    Assertions.assertEquals("catalog3", catalogDTOs[1].name());

    // Verify that all returned catalogs have full properties (no sensitive property filtering)
    for (CatalogDTO dto : catalogDTOs) {
      Assertions.assertNotNull(dto.properties());
      // When filterSensitiveProperties=false, sensitive properties should be visible
      Assertions.assertTrue(
          dto.properties().containsKey("jdbc-password"),
          "Sensitive properties should be visible when filterSensitiveProperties=false");
      Assertions.assertEquals("secret123", dto.properties().get("jdbc-password"));
      Assertions.assertTrue(
          dto.properties().containsKey("s3-secret-key"),
          "Sensitive properties should be visible when filterSensitiveProperties=false");
      Assertions.assertEquals("aws-secret", dto.properties().get("s3-secret-key"));
    }
  }

  private static TestCatalog buildCatalogWithSensitiveProps(String metalake, String catalogName) {
    // Include both regular and sensitive properties
    ImmutableMap<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put("key", "value")
            .put(PROPERTY_IN_USE, "true")
            .put("jdbc-url", "jdbc:mysql://localhost:3306/db")
            .put("jdbc-user", "admin")
            .put("jdbc-password", "secret123") // sensitive
            .put("s3-endpoint", "https://s3.amazonaws.com")
            .put("s3-access-key-id", "AKIAIOSFODNN7EXAMPLE")
            .put("s3-secret-key", "aws-secret") // sensitive
            .build();

    CatalogEntity entity =
        CatalogEntity.builder()
            .withId(1L)
            .withName(catalogName)
            .withComment("comment")
            .withNamespace(Namespace.of(metalake))
            .withProperties(properties)
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("test")
            .withAuditInfo(
                AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();

    return new TestCatalog().withCatalogConf(Collections.emptyMap()).withCatalogEntity(entity);
  }
}
