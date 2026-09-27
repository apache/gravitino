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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.catalog.ViewDispatcher;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.IllegalSemanticModelException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.Relationship;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.server.authorization.PassThroughAuthorizer;
import org.junit.jupiter.api.Test;

/** Tests catalog-backed validation independently of model persistence. */
public class TestSemanticModelSourceValidator {
  private final TableDispatcher tables = mock(TableDispatcher.class);
  private final ViewDispatcher views = mock(ViewDispatcher.class);
  private GravitinoAuthorizer authorizer = new PassThroughAuthorizer();
  private final SemanticModelSourceValidator validator =
      new SemanticModelSourceValidator(tables, views, () -> authorizer);
  private static final NameIdentifier SOURCE =
      NameIdentifier.of("other_catalog", "schema", "orders");
  private static final NameIdentifier FULL_SOURCE =
      NameIdentifier.of("metalake", "other_catalog", "schema", "orders");

  @Test
  void testCrossCatalogSourceAndKeysWithDisabledAuthorization() {
    table("id", "unique_id");
    validator.validate(
        "metalake",
        definition(dataset("orders", new String[] {"id"}, new String[][] {{"id", "unique_id"}})));
    verify(tables).loadTable(FULL_SOURCE);
    verifyNoInteractions(views);
  }

  @Test
  void testViewFallbackAndColumns() {
    when(tables.loadTable(FULL_SOURCE)).thenThrow(new NoSuchTableException("missing"));
    View view = mock(View.class);
    doReturn(columns("id")).when(view).columns();
    when(views.loadView(FULL_SOURCE)).thenReturn(view);
    validator.validate("metalake", definition(dataset("orders", new String[] {"id"}, null)));
    assertThrows(
        IllegalSemanticModelException.class,
        () ->
            validator.validate(
                "metalake", definition(dataset("orders", new String[] {"missing"}, null))));
  }

  @Test
  void testMissingUniqueKeyAndUnavailableColumns() {
    Table table = table("id");
    assertThrows(
        IllegalSemanticModelException.class,
        () ->
            validator.validate(
                "metalake", definition(dataset("orders", null, new String[][] {{"missing"}}))));
    when(table.columns()).thenReturn(null);
    assertThrows(
        IllegalSemanticModelException.class,
        () -> validator.validate("metalake", definition(dataset("orders", null, null))));
  }

  @Test
  void testRelationshipColumnsAndRepeatedSource() {
    table("id");
    Dataset first = dataset("orders", null, null);
    Dataset second = dataset("customers", null, null);
    SemanticModelDefinition valid = related(first, second, "id", "id");
    validator.validate("metalake", valid);
    verify(tables, times(1)).loadTable(FULL_SOURCE);
    assertThrows(
        IllegalSemanticModelException.class,
        () -> validator.validate("metalake", related(first, second, "missing", "id")));
    assertThrows(
        IllegalSemanticModelException.class,
        () -> validator.validate("metalake", related(first, second, "id", "missing")));
  }

  @Test
  void testDeniedTableCanResolveAuthorizedViewWithoutTableLookup() {
    authorizeOnly(MetadataObject.Type.VIEW, Privilege.Name.SELECT_VIEW);
    View view = mock(View.class);
    doReturn(columns("id")).when(view).columns();
    when(views.loadView(FULL_SOURCE)).thenReturn(view);
    validator.validate("metalake", definition(dataset("orders", null, null)));
    verifyNoInteractions(tables);
    verify(views).loadView(FULL_SOURCE);
  }

  @Test
  void testAuthorizedTableDoesNotRequireViewAccess() {
    authorizeOnly(MetadataObject.Type.TABLE, Privilege.Name.SELECT_TABLE);
    table("id");
    validator.validate("metalake", definition(dataset("orders", new String[] {"id"}, null)));
    assertThrows(
        IllegalSemanticModelException.class,
        () ->
            validator.validate(
                "metalake", definition(dataset("orders", new String[] {"missing"}, null))));
    verifyNoInteractions(views);
  }

  @Test
  void testMissingAuthorizedTypeDoesNotProbeDeniedType() {
    authorizeOnly(MetadataObject.Type.TABLE, Privilege.Name.SELECT_TABLE);
    when(tables.loadTable(FULL_SOURCE)).thenThrow(new NoSuchTableException("missing"));
    assertThrows(
        ForbiddenException.class,
        () -> validator.validate("metalake", definition(dataset("orders", null, null))));
    verifyNoInteractions(views);
  }

  @Test
  void testDenyOverridesSelect() {
    authorizeOnly(MetadataObject.Type.TABLE, Privilege.Name.SELECT_TABLE);
    when(authorizer.deny(any(), anyString(), any(), eq(Privilege.Name.SELECT_TABLE), any()))
        .thenReturn(true);
    assertThrows(
        ForbiddenException.class,
        () -> validator.validate("metalake", definition(dataset("orders", null, null))));
    verifyNoInteractions(tables, views);
  }

  @Test
  void testMissingAndConnectionFailureForView() {
    when(tables.loadTable(FULL_SOURCE)).thenThrow(new NoSuchTableException("missing"));
    when(views.loadView(FULL_SOURCE)).thenThrow(new NoSuchViewException("missing"));
    assertThrows(
        IllegalSemanticModelException.class,
        () -> validator.validate("metalake", definition(dataset("orders", null, null))));
    doThrow(new ConnectionFailedException("offline")).when(views).loadView(FULL_SOURCE);
    assertThrows(
        ConnectionFailedException.class,
        () -> validator.validate("metalake", definition(dataset("orders", null, null))));
  }

  @Test
  void testInvalidSourceShapeDoesNotLookupMetadata() {
    Dataset dataset =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of("foreign_metalake", "catalog", "schema", "orders"))
            .build();
    assertThrows(
        IllegalSemanticModelException.class,
        () -> validator.validate("metalake", definition(dataset)));
    verifyNoInteractions(tables, views);
  }

  private void authorizeOnly(MetadataObject.Type type, Privilege.Name privilege) {
    authorizer = mock(GravitinoAuthorizer.class);
    when(authorizer.authorize(any(), anyString(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              Privilege.Name requested = invocation.getArgument(3);
              MetadataObject object = invocation.getArgument(2);
              return requested == Privilege.Name.USE_CATALOG
                  || requested == Privilege.Name.USE_SCHEMA
                  || (requested == privilege && object.type() == type);
            });
  }

  private Table table(String... names) {
    Table table = mock(Table.class);
    doReturn(columns(names)).when(table).columns();
    when(tables.loadTable(FULL_SOURCE)).thenReturn(table);
    return table;
  }

  private Column[] columns(String... names) {
    Column[] columns = new Column[names.length];
    for (int i = 0; i < names.length; i++) {
      columns[i] = mock(Column.class);
      when(columns[i].name()).thenReturn(names[i]);
    }
    return columns;
  }

  private Dataset dataset(String name, String[] primaryKey, String[][] uniqueKeys) {
    return Dataset.builder()
        .withName(name)
        .withSource(SOURCE)
        .withPrimaryKey(primaryKey)
        .withUniqueKeys(uniqueKeys)
        .build();
  }

  private SemanticModelDefinition definition(Dataset... datasets) {
    return SemanticModelDefinition.builder().withDatasets(datasets).build();
  }

  private SemanticModelDefinition related(Dataset first, Dataset second, String from, String to) {
    return SemanticModelDefinition.builder()
        .withDatasets(new Dataset[] {first, second})
        .withRelationships(
            new Relationship[] {
              Relationship.builder()
                  .withName("customer_orders")
                  .withFrom(first.name())
                  .withTo(second.name())
                  .withFromColumns(new String[] {from})
                  .withToColumns(new String[] {to})
                  .build()
            })
        .build();
  }
}
