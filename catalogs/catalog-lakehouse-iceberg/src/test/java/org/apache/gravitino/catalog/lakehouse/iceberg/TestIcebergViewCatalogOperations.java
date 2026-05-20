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
package org.apache.gravitino.catalog.lakehouse.iceberg;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewChange;
import org.apache.gravitino.rel.types.Types;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.UpdateRequirements;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.rest.requests.ImmutableCreateViewRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.view.ViewMetadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestIcebergViewCatalogOperations {

  @Test
  public void testLoadViewTranslatesOnlyNoSuchViewException() {
    IcebergCatalogWrapper wrapper = Mockito.mock(IcebergCatalogWrapper.class);
    IcebergViewCatalogOperations operations = new IcebergViewCatalogOperations(wrapper);

    NameIdentifier ident = NameIdentifier.of("schema1", "view1");
    when(wrapper.loadView(any(TableIdentifier.class)))
        .thenThrow(new org.apache.iceberg.exceptions.NoSuchViewException("view missing"));

    Assertions.assertThrows(NoSuchViewException.class, () -> operations.loadView(ident));
  }

  @Test
  public void testLoadViewPropagatesNonNoSuchViewException() {
    IcebergCatalogWrapper wrapper = Mockito.mock(IcebergCatalogWrapper.class);
    IcebergViewCatalogOperations operations = new IcebergViewCatalogOperations(wrapper);

    NameIdentifier ident = NameIdentifier.of("schema1", "view1");
    when(wrapper.loadView(any(TableIdentifier.class)))
        .thenThrow(new ServiceFailureException("backend unavailable"));

    Assertions.assertThrows(ServiceFailureException.class, () -> operations.loadView(ident));
  }

  @Test
  public void testViewExistsDelegatesToWrapper() {
    IcebergCatalogWrapper wrapper = Mockito.mock(IcebergCatalogWrapper.class);
    IcebergViewCatalogOperations operations = new IcebergViewCatalogOperations(wrapper);

    NameIdentifier ident = NameIdentifier.of("schema1", "view1");
    when(wrapper.viewExists(any(TableIdentifier.class))).thenReturn(true);

    Assertions.assertTrue(operations.viewExists(ident));
    verify(wrapper).viewExists(any(TableIdentifier.class));
  }

  @Test
  public void testAlterViewRejectsRenameWithOtherChanges() {
    IcebergCatalogWrapper wrapper = Mockito.mock(IcebergCatalogWrapper.class);
    IcebergViewCatalogOperations operations = new IcebergViewCatalogOperations(wrapper);

    NameIdentifier ident = NameIdentifier.of("schema1", "view1");

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                operations.alterView(
                    ident, ViewChange.rename("view2"), ViewChange.setProperty("k", "v")));

    Assertions.assertEquals(
        "Rename cannot be combined with other view changes.", exception.getMessage());

    verify(wrapper, never()).renameView(any());
  }

  @Test
  public void testCreateViewRejectsSqlLikeRepresentationWithoutSqlRepresentationClass()
      throws Exception {
    IcebergCatalogWrapper wrapper = Mockito.mock(IcebergCatalogWrapper.class);
    IcebergViewCatalogOperations operations = new IcebergViewCatalogOperations(wrapper);

    NameIdentifier ident = NameIdentifier.of("schema1", "view1");
    Column[] columns = {Column.of("id", Types.LongType.get(), null)};
    Representation sqlLikeRepresentation =
        new Representation() {
          @Override
          public String type() {
            return Representation.TYPE_SQL;
          }

          public String dialect() {
            return "spark";
          }

          public String sql() {
            return "SELECT id FROM t";
          }
        };

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                operations.createView(
                    ident,
                    null,
                    columns,
                    new Representation[] {sqlLikeRepresentation},
                    null,
                    null,
                    Collections.emptyMap()));
    Assertions.assertTrue(exception.getMessage().contains(SQLRepresentation.class.getSimpleName()));
  }

  @Test
  public void testCreateViewStoresDefaultCatalogInViewVersion() throws Exception {
    IcebergCatalogWrapper wrapper = Mockito.mock(IcebergCatalogWrapper.class);
    IcebergViewCatalogOperations operations = new IcebergViewCatalogOperations(wrapper);

    NameIdentifier ident = NameIdentifier.of("schema1", "view1");
    Column[] columns = {Column.of("id", Types.LongType.get(), null)};
    SQLRepresentation representation =
        SQLRepresentation.builder().withDialect("spark").withSql("SELECT id FROM t").build();

    doAnswer(
            invocation -> {
              ImmutableCreateViewRequest request = invocation.getArgument(1);
              Assertions.assertEquals("catalog1", request.viewVersion().defaultCatalog());
              Assertions.assertFalse(request.properties().containsKey("default-catalog"));
              return mockLoadViewResponse();
            })
        .when(wrapper)
        .createView(
            any(org.apache.iceberg.catalog.Namespace.class), any(ImmutableCreateViewRequest.class));

    View view =
        operations.createView(
            ident,
            "test comment",
            columns,
            new Representation[] {representation},
            "catalog1",
            "schema1",
            Collections.singletonMap("k", "v"));

    Assertions.assertEquals("catalog1", view.defaultCatalog());
    Assertions.assertEquals("schema1", view.defaultSchema());
    Assertions.assertFalse(view.properties().containsKey("default-catalog"));
  }

  @Test
  public void testCreateViewAllowsNullDefaultCatalogInViewVersion() throws Exception {
    IcebergCatalogWrapper wrapper = Mockito.mock(IcebergCatalogWrapper.class);
    IcebergViewCatalogOperations operations = new IcebergViewCatalogOperations(wrapper);

    NameIdentifier ident = NameIdentifier.of("schema1", "view1");
    Column[] columns = {Column.of("id", Types.LongType.get(), null)};
    SQLRepresentation representation =
        SQLRepresentation.builder().withDialect("spark").withSql("SELECT id FROM t").build();

    doAnswer(
            invocation -> {
              ImmutableCreateViewRequest request = invocation.getArgument(1);
              Assertions.assertNull(request.viewVersion().defaultCatalog());
              return mockLoadViewResponse();
            })
        .when(wrapper)
        .createView(
            any(org.apache.iceberg.catalog.Namespace.class), any(ImmutableCreateViewRequest.class));

    View view =
        operations.createView(
            ident,
            "test comment",
            columns,
            new Representation[] {representation},
            null,
            "schema1",
            Collections.singletonMap("k", "v"));

    Assertions.assertNull(view.defaultCatalog());
  }

  @Test
  public void testAlterViewSetAfterRemoveKeepsProperty() throws Exception {
    IcebergCatalogWrapper wrapper = Mockito.mock(IcebergCatalogWrapper.class);
    IcebergViewCatalogOperations operations = new IcebergViewCatalogOperations(wrapper);

    NameIdentifier ident = NameIdentifier.of("schema1", "view1");
    LoadViewResponse loaded = mockLoadViewResponseWithUuid("view-uuid");
    when(wrapper.loadView(any(TableIdentifier.class))).thenReturn(loaded);

    doAnswer(
            invocation -> {
              UpdateTableRequest request = invocation.getArgument(1);
              Map<String, String> setProps = extractSetProperties(request.updates());
              Set<String> removedProps = extractRemovedProperties(request.updates());
              Assertions.assertEquals("v1", setProps.get("k1"));
              Assertions.assertFalse(removedProps.contains("k1"));
              return mockLoadViewResponse();
            })
        .when(wrapper)
        .updateView(any(TableIdentifier.class), any(UpdateTableRequest.class));

    operations.alterView(
        ident, ViewChange.removeProperty("k1"), ViewChange.setProperty("k1", "v1"));

    verify(wrapper).updateView(any(TableIdentifier.class), any(UpdateTableRequest.class));
  }

  @Test
  public void testAlterViewRemoveAfterSetRemovesProperty() throws Exception {
    IcebergCatalogWrapper wrapper = Mockito.mock(IcebergCatalogWrapper.class);
    IcebergViewCatalogOperations operations = new IcebergViewCatalogOperations(wrapper);

    NameIdentifier ident = NameIdentifier.of("schema1", "view1");
    LoadViewResponse loaded = mockLoadViewResponseWithUuid("view-uuid");
    when(wrapper.loadView(any(TableIdentifier.class))).thenReturn(loaded);

    doAnswer(
            invocation -> {
              UpdateTableRequest request = invocation.getArgument(1);
              Map<String, String> setProps = extractSetProperties(request.updates());
              Set<String> removedProps = extractRemovedProperties(request.updates());
              Assertions.assertFalse(setProps.containsKey("k1"));
              Assertions.assertTrue(removedProps.contains("k1"));
              return mockLoadViewResponse();
            })
        .when(wrapper)
        .updateView(any(TableIdentifier.class), any(UpdateTableRequest.class));

    operations.alterView(
        ident, ViewChange.setProperty("k1", "v1"), ViewChange.removeProperty("k1"));

    verify(wrapper).updateView(any(TableIdentifier.class), any(UpdateTableRequest.class));
  }

  @Test
  public void testAlterViewReplaceDoesNotApplyLegacyDefaultCatalogPropertyCleanup()
      throws Exception {
    IcebergCatalogWrapper wrapper = Mockito.mock(IcebergCatalogWrapper.class);
    IcebergViewCatalogOperations operations = new IcebergViewCatalogOperations(wrapper);

    NameIdentifier ident = NameIdentifier.of("schema1", "view1");

    ViewMetadata metadata = Mockito.mock(ViewMetadata.class);
    when(metadata.uuid()).thenReturn("view-uuid");
    when(metadata.schemas()).thenReturn(Collections.emptyList());

    LoadViewResponse loaded = Mockito.mock(LoadViewResponse.class);
    when(loaded.metadata()).thenReturn(metadata);
    when(wrapper.loadView(any(TableIdentifier.class))).thenReturn(loaded);

    Column[] columns = {Column.of("id", Types.LongType.get(), null)};
    SQLRepresentation representation =
        SQLRepresentation.builder().withDialect("spark").withSql("SELECT id FROM t").build();

    doAnswer(
            invocation -> {
              UpdateTableRequest request = invocation.getArgument(1);
              Map<String, String> setProps = extractSetProperties(request.updates());
              Set<String> removedProps = extractRemovedProperties(request.updates());
              Assertions.assertFalse(setProps.containsKey("default-catalog"));
              Assertions.assertFalse(removedProps.contains("default-catalog"));
              return mockLoadViewResponse();
            })
        .when(wrapper)
        .updateView(any(TableIdentifier.class), any(UpdateTableRequest.class));

    operations.alterView(
        ident,
        ViewChange.replaceView(
            columns, new Representation[] {representation}, null, "schema1", null));

    verify(wrapper).updateView(any(TableIdentifier.class), any(UpdateTableRequest.class));
  }

  @Test
  public void testAlterViewBuildsOptimisticConcurrencyRequirements() throws Exception {
    IcebergCatalogWrapper wrapper = Mockito.mock(IcebergCatalogWrapper.class);
    IcebergViewCatalogOperations operations = new IcebergViewCatalogOperations(wrapper);

    NameIdentifier ident = NameIdentifier.of("schema1", "view1");

    ViewMetadata metadata = Mockito.mock(ViewMetadata.class);
    when(metadata.uuid()).thenReturn("view-uuid");

    LoadViewResponse loaded = Mockito.mock(LoadViewResponse.class);
    when(loaded.metadata()).thenReturn(metadata);
    when(wrapper.loadView(any(TableIdentifier.class))).thenReturn(loaded);

    doAnswer(
            invocation -> {
              UpdateTableRequest request = invocation.getArgument(1);
              List<UpdateRequirement> expectedRequirements =
                  UpdateRequirements.forReplaceView(metadata, request.updates());
              Assertions.assertEquals(expectedRequirements.size(), request.requirements().size());
              for (int i = 0; i < expectedRequirements.size(); i++) {
                Assertions.assertEquals(
                    expectedRequirements.get(i).getClass(),
                    request.requirements().get(i).getClass());
              }
              Assertions.assertFalse(request.requirements().isEmpty());
              return mockLoadViewResponse();
            })
        .when(wrapper)
        .updateView(any(TableIdentifier.class), any(UpdateTableRequest.class));

    operations.alterView(ident, ViewChange.setProperty("k", "v"));

    verify(wrapper).updateView(any(TableIdentifier.class), any(UpdateTableRequest.class));
  }

  private static LoadViewResponse mockLoadViewResponse() {
    return Mockito.mock(LoadViewResponse.class);
  }

  private static LoadViewResponse mockLoadViewResponseWithUuid(String uuid) {
    ViewMetadata metadata = Mockito.mock(ViewMetadata.class);
    when(metadata.uuid()).thenReturn(uuid);

    LoadViewResponse response = Mockito.mock(LoadViewResponse.class);
    when(response.metadata()).thenReturn(metadata);
    return response;
  }

  private static Map<String, String> extractSetProperties(List<MetadataUpdate> updates) {
    return updates.stream()
        .filter(update -> update instanceof MetadataUpdate.SetProperties)
        .map(update -> ((MetadataUpdate.SetProperties) update).updated())
        .findFirst()
        .orElse(java.util.Collections.emptyMap());
  }

  private static Set<String> extractRemovedProperties(List<MetadataUpdate> updates) {
    return updates.stream()
        .filter(update -> update instanceof MetadataUpdate.RemoveProperties)
        .map(update -> ((MetadataUpdate.RemoveProperties) update).removed())
        .findFirst()
        .orElse(java.util.Collections.emptySet());
  }
}
