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
package org.apache.gravitino.authorization.ranger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.authorization.AccessControlDispatcher;
import org.apache.gravitino.authorization.ranger.reference.RangerDefines.PolicyResource;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.hook.CatalogHookDispatcher;
import org.apache.gravitino.utils.ThrowableFunction;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.util.SearchFilter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Drops a Hive catalog through the hook dispatcher and checks the Ranger HDFS policies left. */
public class TestRangerAuthorizationHDFSPluginCatalogDrop {

  private static final String METALAKE = "metalake";
  private static final NameIdentifier CATALOG = NameIdentifier.of(METALAKE, "hive_catalog");
  private static final NameIdentifier OTHER_CATALOG = NameIdentifier.of(METALAKE, "other_catalog");
  private static final String WAREHOUSE = "hdfs://ns/user/hive/warehouse";

  private final List<RangerPolicy> policies = new ArrayList<>();
  private RangerClientExtension rangerClient;
  private CatalogDispatcher innerDispatcher;
  private CatalogDispatcher internalCatalogDispatcher;
  private SchemaDispatcher schemaDispatcher;
  private MockedStatic<GravitinoEnv> envStatic;

  @BeforeEach
  public void setUp() throws Exception {
    policies.clear();
    // The constructor connects to Ranger, so only the tested methods run for real.
    RangerAuthorizationHDFSPlugin plugin =
        Mockito.mock(RangerAuthorizationHDFSPlugin.class, Mockito.CALLS_REAL_METHODS);
    rangerClient = Mockito.mock(RangerClientExtension.class);
    // An in-memory Ranger HDFS service: a search returns the policies on the searched path.
    Mockito.when(rangerClient.findPolicies(any()))
        .thenAnswer(invocation -> findPolicies(invocation.getArgument(0)));
    Mockito.when(rangerClient.updatePolicy(anyLong(), any()))
        .thenAnswer(invocation -> invocation.getArgument(1));
    plugin.setRangerClient(rangerClient);
    plugin.setRangerHelper(
        new RangerHelper(
            rangerClient,
            "admin",
            "hdfsService",
            plugin.ownerMappingRule(),
            plugin.policyResourceDefinesRule()));
    FieldUtils.writeField(plugin, "metalake", METALAKE, true);

    BaseCatalog<?> catalog = Mockito.mock(BaseCatalog.class);
    Mockito.when(catalog.name()).thenReturn(CATALOG.name());
    Mockito.when(catalog.getAuthorizationPlugin()).thenReturn(plugin);
    CatalogManager catalogManager = Mockito.mock(CatalogManager.class);
    Mockito.doAnswer(
            invocation -> {
              ThrowableFunction<BaseCatalog, Object> operation = invocation.getArgument(1);
              return operation.apply(catalog);
            })
        .when(catalogManager)
        .doWithCatalog(Mockito.eq(CATALOG), Mockito.any());

    internalCatalogDispatcher = Mockito.mock(CatalogDispatcher.class);
    Catalog hiveCatalog = Mockito.mock(Catalog.class);
    Mockito.when(hiveCatalog.provider()).thenReturn("hive");
    Mockito.when(internalCatalogDispatcher.loadCatalog(any())).thenReturn(hiveCatalog);
    givenCatalogs(catalogInfo(CATALOG, "hive"));
    schemaDispatcher = Mockito.mock(SchemaDispatcher.class);
    givenDefaultSchemaLocation(CATALOG, WAREHOUSE);

    GravitinoEnv env = Mockito.mock(GravitinoEnv.class);
    Mockito.when(env.internalAccessControlDispatcher())
        .thenReturn(Mockito.mock(AccessControlDispatcher.class));
    Mockito.when(env.internalCatalogDispatcher()).thenReturn(internalCatalogDispatcher);
    Mockito.when(env.internalSchemaDispatcher()).thenReturn(schemaDispatcher);
    Mockito.when(env.catalogManager()).thenReturn(catalogManager);
    envStatic = Mockito.mockStatic(GravitinoEnv.class);
    envStatic.when(GravitinoEnv::getInstance).thenReturn(env);

    innerDispatcher = Mockito.mock(CatalogDispatcher.class);
    Mockito.when(innerDispatcher.catalogExists(CATALOG)).thenReturn(true);
    Mockito.when(innerDispatcher.dropCatalog(CATALOG, true)).thenReturn(true);
  }

  @AfterEach
  public void tearDown() {
    envStatic.close();
  }

  @Test
  public void testDropCatalogRemovesGravitinoItemsOfWarehousePolicy() throws Exception {
    RangerPolicy policy = addPolicy(1, "/user/hive/warehouse", "GRAVITINO_role", "external_role");

    Assertions.assertTrue(new CatalogHookDispatcher(innerDispatcher).dropCatalog(CATALOG, true));

    // Only the Gravitino-managed item goes; the item Ranger admins added stays.
    Mockito.verify(rangerClient).updatePolicy(Mockito.eq(1L), Mockito.any());
    Assertions.assertEquals(List.of("external_role"), rolesOf(policy));
    Mockito.verify(innerDispatcher).dropCatalog(CATALOG, true);
  }

  @Test
  public void testDropCatalogDeletesPolicyWithOnlyGravitinoItems() throws Exception {
    addPolicy(1, "/user/hive/warehouse", "GRAVITINO_role");

    Assertions.assertTrue(new CatalogHookDispatcher(innerDispatcher).dropCatalog(CATALOG, true));

    Mockito.verify(rangerClient).deletePolicy(1L);
    Mockito.verify(innerDispatcher).dropCatalog(CATALOG, true);
  }

  @Test
  public void testDropCatalogRefusedWhenLocationLookupFails() throws Exception {
    addPolicy(1, "/user/hive/warehouse", "GRAVITINO_role");
    Mockito.when(
            schemaDispatcher.loadSchema(NameIdentifier.of(METALAKE, CATALOG.name(), "default")))
        .thenThrow(new NoSuchSchemaException("metastore unreachable"));

    AuthorizationPluginException e =
        Assertions.assertThrows(
            AuthorizationPluginException.class,
            () -> new CatalogHookDispatcher(innerDispatcher).dropCatalog(CATALOG, true));

    Assertions.assertTrue(e.getMessage().contains(CATALOG.toString()), e.getMessage());
    Mockito.verify(innerDispatcher, Mockito.never())
        .dropCatalog(Mockito.any(), Mockito.anyBoolean());
    Mockito.verify(rangerClient, Mockito.never()).deletePolicy(anyLong());
    Mockito.verify(rangerClient, Mockito.never()).updatePolicy(anyLong(), any());
  }

  @Test
  public void testDropCatalogKeepsPolicyOfPathSharedWithOtherCatalog() throws Exception {
    RangerPolicy policy = addPolicy(1, "/user/hive/warehouse", "GRAVITINO_role");
    givenOtherCatalogWithLocation(WAREHOUSE + "/");

    Assertions.assertTrue(new CatalogHookDispatcher(innerDispatcher).dropCatalog(CATALOG, true));

    // The other catalog's grants live on the same policy, so it stays untouched.
    Mockito.verify(rangerClient, Mockito.never()).deletePolicy(anyLong());
    Mockito.verify(rangerClient, Mockito.never()).updatePolicy(anyLong(), any());
    Assertions.assertEquals(List.of("GRAVITINO_role"), rolesOf(policy));
    Mockito.verify(innerDispatcher).dropCatalog(CATALOG, true);
  }

  @Test
  public void testDropCatalogRemovesPolicyWhenOtherCatalogUsesAnotherPath() throws Exception {
    addPolicy(1, "/user/hive/warehouse", "GRAVITINO_role");
    givenOtherCatalogWithLocation("hdfs://ns/other/warehouse");

    Assertions.assertTrue(new CatalogHookDispatcher(innerDispatcher).dropCatalog(CATALOG, true));

    Mockito.verify(rangerClient).deletePolicy(1L);
  }

  @Test
  public void testDropCatalogRefusedWhenOtherCatalogLocationLookupFails() throws Exception {
    addPolicy(1, "/user/hive/warehouse", "GRAVITINO_role");
    givenOtherCatalogWithLocation(WAREHOUSE);
    Mockito.when(
            schemaDispatcher.loadSchema(
                NameIdentifier.of(METALAKE, OTHER_CATALOG.name(), "default")))
        .thenThrow(new NoSuchSchemaException("metastore unreachable"));

    // Without the other catalog's locations, removing the policy could revoke its grants.
    AuthorizationPluginException e =
        Assertions.assertThrows(
            AuthorizationPluginException.class,
            () -> new CatalogHookDispatcher(innerDispatcher).dropCatalog(CATALOG, true));

    Assertions.assertTrue(e.getMessage().contains(CATALOG.toString()), e.getMessage());

    Mockito.verify(innerDispatcher, Mockito.never())
        .dropCatalog(Mockito.any(), Mockito.anyBoolean());
    Mockito.verify(rangerClient, Mockito.never()).deletePolicy(anyLong());
  }

  @Test
  public void testDropCatalogIgnoresNonHiveCatalogs() throws Exception {
    addPolicy(1, "/user/hive/warehouse", "GRAVITINO_role");
    givenCatalogs(catalogInfo(CATALOG, "hive"), catalogInfo(OTHER_CATALOG, "lakehouse-iceberg"));

    Assertions.assertTrue(new CatalogHookDispatcher(innerDispatcher).dropCatalog(CATALOG, true));

    // A non-Hive catalog has no catalog location, so it is not loaded to check for sharing.
    Mockito.verify(internalCatalogDispatcher, Mockito.never()).loadCatalog(OTHER_CATALOG);
    Mockito.verify(rangerClient).deletePolicy(1L);
  }

  @Test
  public void testDropCatalogWithoutDefaultSchemaLocationSucceeds() throws Exception {
    // A default schema without a location is "no paths", not a failed lookup.
    Schema schema = Mockito.mock(Schema.class);
    Mockito.when(schema.properties()).thenReturn(ImmutableMap.of());
    Mockito.when(
            schemaDispatcher.loadSchema(NameIdentifier.of(METALAKE, CATALOG.name(), "default")))
        .thenReturn(schema);

    Assertions.assertTrue(new CatalogHookDispatcher(innerDispatcher).dropCatalog(CATALOG, true));

    Mockito.verify(rangerClient, Mockito.never()).deletePolicy(anyLong());
    Mockito.verify(innerDispatcher).dropCatalog(CATALOG, true);
  }

  private void givenOtherCatalogWithLocation(String location) {
    givenCatalogs(catalogInfo(CATALOG, "hive"), catalogInfo(OTHER_CATALOG, "hive"));
    givenDefaultSchemaLocation(OTHER_CATALOG, location);
  }

  private void givenCatalogs(Catalog... catalogs) {
    Mockito.when(internalCatalogDispatcher.listCatalogsInfo(Namespace.of(METALAKE)))
        .thenReturn(catalogs);
  }

  private static Catalog catalogInfo(NameIdentifier ident, String provider) {
    Catalog catalog = Mockito.mock(Catalog.class);
    Mockito.when(catalog.name()).thenReturn(ident.name());
    Mockito.when(catalog.provider()).thenReturn(provider);
    return catalog;
  }

  private void givenDefaultSchemaLocation(NameIdentifier catalog, String location) {
    Schema schema = Mockito.mock(Schema.class);
    Mockito.when(schema.properties()).thenReturn(ImmutableMap.of("location", location));
    Mockito.when(
            schemaDispatcher.loadSchema(NameIdentifier.of(METALAKE, catalog.name(), "default")))
        .thenReturn(schema);
  }

  private RangerPolicy addPolicy(long id, String path, String... roles) {
    RangerPolicy policy = new RangerPolicy();
    policy.setId(id);
    policy.setName(path);
    policy.setResources(
        ImmutableMap.of(
            PolicyResource.PATH.getName(), new RangerPolicy.RangerPolicyResource(path)));
    for (String role : roles) {
      RangerPolicy.RangerPolicyItem item = new RangerPolicy.RangerPolicyItem();
      item.setRoles(List.of(role));
      item.setAccesses(List.of(new RangerPolicy.RangerPolicyItemAccess("read")));
      policy.getPolicyItems().add(item);
    }
    policies.add(policy);
    return policy;
  }

  private List<RangerPolicy> findPolicies(Map<String, String> filters) {
    String path = filters.get(SearchFilter.RESOURCE_PREFIX + PolicyResource.PATH.getName());
    return policies.stream()
        .filter(
            policy ->
                Objects.equals(
                    policy.getResources().get(PolicyResource.PATH.getName()).getValues().get(0),
                    path))
        .collect(Collectors.toList());
  }

  private static List<String> rolesOf(RangerPolicy policy) {
    return policy.getPolicyItems().stream()
        .flatMap(item -> item.getRoles().stream())
        .collect(Collectors.toList());
  }
}
