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
package org.apache.gravitino.iceberg.service.dispatcher;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.iceberg.service.CatalogWrapperForREST;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** Tests for {@link RegisterTableSourceAuthorization}. */
class TestRegisterTableSourceAuthorization {

  private static final String METALAKE = "test_metalake";
  private static final String CATALOG = "test_catalog";
  private static final String META_LOC = "file:/warehouse/other/secret/metadata/v1.metadata.json";
  private static final String TABLE_LOC = "file:/warehouse/other/secret";

  private CatalogWrapperForREST catalogWrapper;
  private GravitinoAuthorizer authorizer;
  private MockedStatic<PrincipalUtils> principalUtils;
  private MockedStatic<AuthorizationUtils> authorizationUtils;
  private MockedStatic<GravitinoAuthorizerProvider> authorizerProvider;

  @BeforeEach
  void setUp() {
    IcebergConfigProvider configProvider = mock(IcebergConfigProvider.class);
    when(configProvider.getMetalakeName()).thenReturn(METALAKE);
    when(configProvider.getDefaultCatalogName()).thenReturn(CATALOG);
    IcebergRESTServerContext.create(configProvider, true, false, false, null);

    catalogWrapper = mock(CatalogWrapperForREST.class);
    when(catalogWrapper.isRESTCatalog()).thenReturn(false);

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
  }

  @AfterEach
  void tearDown() {
    authorizerProvider.close();
    authorizationUtils.close();
    principalUtils.close();
  }

  @Test
  void testCreateTableAloneCannotAdoptTrackedSource() {
    TableIdentifier source = TableIdentifier.of("other", "secret");
    TableMetadata metadata = tableMetadata();
    stubCatalogWithSource(source, metadata);
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.CREATE_TABLE);

    ForbiddenException thrown =
        Assertions.assertThrows(
            ForbiddenException.class,
            () ->
                RegisterTableSourceAuthorization.authorize(
                    CATALOG, Namespace.of("mine"), "copy", META_LOC, metadata, catalogWrapper));
    Assertions.assertTrue(thrown.getMessage().contains("other.secret"));
  }

  @Test
  void testSelectTableAllowsAdoptTrackedSource() {
    TableIdentifier source = TableIdentifier.of("other", "secret");
    TableMetadata metadata = tableMetadata();
    stubCatalogWithSource(source, metadata);
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.SELECT_TABLE);

    Assertions.assertDoesNotThrow(
        () ->
            RegisterTableSourceAuthorization.authorize(
                CATALOG, Namespace.of("mine"), "copy", META_LOC, metadata, catalogWrapper));
  }

  @Test
  void testOrphanedMetadataDoesNotRequireSourcePrivilege() {
    TableMetadata metadata = tableMetadata();
    stubEmptyCatalog();
    allow(Privilege.Name.CREATE_TABLE);

    Assertions.assertDoesNotThrow(
        () ->
            RegisterTableSourceAuthorization.authorize(
                CATALOG, Namespace.of("mine"), "copy", META_LOC, metadata, catalogWrapper));
    verify(authorizer, never()).authorize(any(), any(), any(), any(), any());
  }

  @Test
  void testTargetItselfIsNotTreatedAsForeignSource() {
    TableIdentifier target = TableIdentifier.of("mine", "t");
    TableMetadata metadata = tableMetadata();
    stubCatalogListing(Namespace.of("mine"), List.of(target));
    when(catalogWrapper.getTableMetadataLocation(target)).thenReturn(Optional.of(META_LOC));
    allow(Privilege.Name.CREATE_TABLE);

    Assertions.assertDoesNotThrow(
        () ->
            RegisterTableSourceAuthorization.authorize(
                CATALOG, Namespace.of("mine"), "t", META_LOC, metadata, catalogWrapper));
    verify(authorizer, never()).authorize(any(), any(), any(), any(), any());
  }

  @Test
  void testNormalizePathUnifiesFileUriForms() {
    Assertions.assertEquals(
        "file:/warehouse/t", RegisterTableSourceAuthorization.normalizePath("file:///warehouse/t"));
    Assertions.assertEquals(
        "file:/warehouse/t", RegisterTableSourceAuthorization.normalizePath("file:/warehouse/t"));
  }

  private void stubCatalogWithSource(TableIdentifier source, TableMetadata metadata) {
    stubCatalogListing(source.namespace(), List.of(source));
    when(catalogWrapper.getTableMetadataLocation(source)).thenReturn(Optional.of(META_LOC));
    when(catalogWrapper.loadTableMetadata(source)).thenReturn(metadata);
  }

  private void stubEmptyCatalog() {
    ListNamespacesResponse emptyNs = mock(ListNamespacesResponse.class);
    when(emptyNs.namespaces()).thenReturn(Collections.emptyList());
    ListTablesResponse emptyTables = mock(ListTablesResponse.class);
    when(emptyTables.identifiers()).thenReturn(Collections.emptyList());
    when(catalogWrapper.listNamespace(Namespace.empty())).thenReturn(emptyNs);
    when(catalogWrapper.listTable(Namespace.empty())).thenReturn(emptyTables);
  }

  private void stubCatalogListing(Namespace namespace, List<TableIdentifier> tables) {
    ListNamespacesResponse rootNs = mock(ListNamespacesResponse.class);
    when(rootNs.namespaces()).thenReturn(List.of(namespace));
    ListNamespacesResponse childNs = mock(ListNamespacesResponse.class);
    when(childNs.namespaces()).thenReturn(Collections.emptyList());
    ListTablesResponse rootTables = mock(ListTablesResponse.class);
    when(rootTables.identifiers()).thenReturn(Collections.emptyList());
    ListTablesResponse childTables = mock(ListTablesResponse.class);
    when(childTables.identifiers()).thenReturn(tables);

    when(catalogWrapper.listNamespace(Namespace.empty())).thenReturn(rootNs);
    when(catalogWrapper.listNamespace(namespace)).thenReturn(childNs);
    when(catalogWrapper.listTable(Namespace.empty())).thenReturn(rootTables);
    when(catalogWrapper.listTable(namespace)).thenReturn(childTables);
  }

  private static TableMetadata tableMetadata() {
    Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()));
    return TableMetadata.newTableMetadata(
        schema, PartitionSpec.unpartitioned(), TABLE_LOC, Collections.emptyMap());
  }

  private void allow(Privilege.Name... privileges) {
    Set<Privilege.Name> allowed = Set.of(privileges);
    doAnswer(invocation -> allowed.contains(invocation.getArgument(3)))
        .when(authorizer)
        .authorize(any(), any(), any(), any(), any());
  }
}
