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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.types.Types;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewVersion;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergView {

  @Test
  public void testFromLoadViewResponse() {
    // Test with properties
    Map<String, String> properties = ImmutableMap.of("key1", "value1", "key2", "value2");
    ViewVersion mockVersion = mock(ViewVersion.class);
    when(mockVersion.representations()).thenReturn(Collections.emptyList());
    ViewMetadata mockMetadata = mock(ViewMetadata.class);
    when(mockMetadata.properties()).thenReturn(properties);
    when(mockMetadata.currentVersion()).thenReturn(mockVersion);

    LoadViewResponse mockResponse = mock(LoadViewResponse.class);
    when(mockResponse.metadata()).thenReturn(mockMetadata);

    IcebergView view = IcebergView.fromLoadViewResponse(mockResponse, "test_view");

    Assertions.assertEquals("test_view", view.name());
    Assertions.assertEquals(properties, view.properties());
    Assertions.assertEquals(AuditInfo.EMPTY, view.auditInfo());
  }

  @Test
  public void testFromLoadViewResponseWithNullProperties() {
    // Test with null metadata
    LoadViewResponse mockResponse = mock(LoadViewResponse.class);
    when(mockResponse.metadata()).thenReturn(null);

    IcebergView view = IcebergView.fromLoadViewResponse(mockResponse, "test_view_null");

    Assertions.assertEquals("test_view_null", view.name());
    Assertions.assertNotNull(view.properties());
    Assertions.assertTrue(view.properties().isEmpty());
    Assertions.assertNotNull(view.auditInfo());
  }

  @Test
  public void testFromLoadViewResponseWithNullMetadataProperties() {
    // Test with null properties in metadata
    ViewVersion mockVersion = mock(ViewVersion.class);
    when(mockVersion.representations()).thenReturn(Collections.emptyList());
    ViewMetadata mockMetadata = mock(ViewMetadata.class);
    when(mockMetadata.properties()).thenReturn(null);
    when(mockMetadata.currentVersion()).thenReturn(mockVersion);

    LoadViewResponse mockResponse = mock(LoadViewResponse.class);
    when(mockResponse.metadata()).thenReturn(mockMetadata);

    IcebergView view = IcebergView.fromLoadViewResponse(mockResponse, "test_view_empty");

    Assertions.assertEquals("test_view_empty", view.name());
    Assertions.assertNotNull(view.properties());
    Assertions.assertTrue(view.properties().isEmpty());
  }

  @Test
  public void testBuilder() {
    Map<String, String> properties = ImmutableMap.of("prop1", "val1");
    AuditInfo auditInfo = AuditInfo.builder().withCreator("test_user").withCreateTime(null).build();

    IcebergView view =
        IcebergView.builder()
            .withName("builder_view")
            .withProperties(properties)
            .withAuditInfo(auditInfo)
            .build();

    Assertions.assertEquals("builder_view", view.name());
    Assertions.assertEquals(properties, view.properties());
    Assertions.assertEquals(auditInfo, view.auditInfo());
    Assertions.assertEquals("test_user", view.auditInfo().creator());
  }

  @Test
  public void testBuilderWithNullProperties() {
    AuditInfo auditInfo = AuditInfo.builder().withCreator("test_user").withCreateTime(null).build();

    IcebergView view =
        IcebergView.builder().withName("view_null_props").withAuditInfo(auditInfo).build();

    Assertions.assertEquals("view_null_props", view.name());
    Assertions.assertNotNull(view.properties());
    Assertions.assertTrue(view.properties().isEmpty());
  }

  @Test
  public void testFromLoadViewResponseExtractsComment() {
    Map<String, String> properties = ImmutableMap.of("comment", "my view comment", "key", "val");
    ViewVersion mockVersion = mock(ViewVersion.class);
    when(mockVersion.representations()).thenReturn(Collections.emptyList());
    ViewMetadata mockMetadata = mock(ViewMetadata.class);
    when(mockMetadata.properties()).thenReturn(properties);
    when(mockMetadata.currentVersion()).thenReturn(mockVersion);

    LoadViewResponse mockResponse = mock(LoadViewResponse.class);
    when(mockResponse.metadata()).thenReturn(mockMetadata);

    IcebergView view = IcebergView.fromLoadViewResponse(mockResponse, "comment_view");

    Assertions.assertEquals("my view comment", view.comment());
    Assertions.assertEquals(properties, view.properties());
  }

  @Test
  public void testFromLoadViewResponseExtractsRepresentations() {
    Map<String, String> properties = ImmutableMap.of("key", "val");

    SQLViewRepresentation sqlRep = mock(SQLViewRepresentation.class);
    when(sqlRep.dialect()).thenReturn("spark");
    when(sqlRep.sql()).thenReturn("SELECT id FROM t1");

    ViewVersion mockVersion = mock(ViewVersion.class);
    when(mockVersion.representations()).thenReturn(Collections.singletonList(sqlRep));

    ViewMetadata mockMetadata = mock(ViewMetadata.class);
    when(mockMetadata.properties()).thenReturn(properties);
    when(mockMetadata.currentVersion()).thenReturn(mockVersion);

    LoadViewResponse mockResponse = mock(LoadViewResponse.class);
    when(mockResponse.metadata()).thenReturn(mockMetadata);

    IcebergView view = IcebergView.fromLoadViewResponse(mockResponse, "rep_view");

    Assertions.assertEquals(1, view.representations().length);
    Representation rep = view.representations()[0];
    Assertions.assertInstanceOf(SQLRepresentation.class, rep);
    SQLRepresentation sqlRepResult = (SQLRepresentation) rep;
    Assertions.assertEquals("spark", sqlRepResult.dialect());
    Assertions.assertEquals("SELECT id FROM t1", sqlRepResult.sql());
  }

  @Test
  public void testFromLoadViewResponseExtractsDefaultCatalogAndSchema() {
    Map<String, String> properties = ImmutableMap.of("k", "v");

    ViewVersion mockVersion = mock(ViewVersion.class);
    when(mockVersion.representations()).thenReturn(Collections.emptyList());
    when(mockVersion.defaultCatalog()).thenReturn("current_cat");
    when(mockVersion.defaultNamespace()).thenReturn(Namespace.of("schema1"));

    ViewMetadata mockMetadata = mock(ViewMetadata.class);
    when(mockMetadata.properties()).thenReturn(properties);
    when(mockMetadata.currentVersion()).thenReturn(mockVersion);

    LoadViewResponse mockResponse = mock(LoadViewResponse.class);
    when(mockResponse.metadata()).thenReturn(mockMetadata);

    IcebergView view = IcebergView.fromLoadViewResponse(mockResponse, "default_view");

    Assertions.assertEquals("current_cat", view.defaultCatalog());
    Assertions.assertEquals("schema1", view.defaultSchema());
    Assertions.assertEquals("v", view.properties().get("k"));
  }

  @Test
  public void testFromLoadViewResponseExtractsColumns() {
    Map<String, String> properties = ImmutableMap.of("key", "val");

    ViewVersion mockVersion = mock(ViewVersion.class);
    when(mockVersion.representations()).thenReturn(Collections.emptyList());

    org.apache.iceberg.types.Types.NestedField field1 =
        org.apache.iceberg.types.Types.NestedField.optional(
            1, "id", org.apache.iceberg.types.Types.LongType.get(), "id column");
    org.apache.iceberg.types.Types.NestedField field2 =
        org.apache.iceberg.types.Types.NestedField.required(
            2, "name", org.apache.iceberg.types.Types.StringType.get());

    Schema schema = new Schema(Arrays.asList(field1, field2));

    ViewMetadata mockMetadata = mock(ViewMetadata.class);
    when(mockMetadata.properties()).thenReturn(properties);
    when(mockMetadata.currentVersion()).thenReturn(mockVersion);
    when(mockMetadata.schema()).thenReturn(schema);

    LoadViewResponse mockResponse = mock(LoadViewResponse.class);
    when(mockResponse.metadata()).thenReturn(mockMetadata);

    IcebergView view = IcebergView.fromLoadViewResponse(mockResponse, "col_view");

    Assertions.assertEquals(2, view.columns().length);
    Assertions.assertEquals("id", view.columns()[0].name());
    Assertions.assertTrue(view.columns()[0].nullable());
    Assertions.assertEquals("id column", view.columns()[0].comment());
    Assertions.assertEquals("name", view.columns()[1].name());
    Assertions.assertFalse(view.columns()[1].nullable());
    Assertions.assertEquals(Types.LongType.get(), view.columns()[0].dataType());
  }

  @Test
  public void testBuilderWithAllFields() {
    Column mockCol = mock(Column.class);
    when(mockCol.name()).thenReturn("c1");

    SQLRepresentation rep =
        SQLRepresentation.builder().withDialect("trino").withSql("SELECT 1").build();
    AuditInfo auditInfo = AuditInfo.builder().withCreator("user").withCreateTime(null).build();

    IcebergView view =
        IcebergView.builder()
            .withName("full_view")
            .withComment("full view")
            .withDefaultCatalog("cat1")
            .withDefaultSchema("schema1")
            .withColumns(new Column[] {mockCol})
            .withRepresentations(new Representation[] {rep})
            .withProperties(ImmutableMap.of("k", "v"))
            .withAuditInfo(auditInfo)
            .build();

    Assertions.assertEquals("full_view", view.name());
    Assertions.assertEquals("full view", view.comment());
    Assertions.assertEquals("cat1", view.defaultCatalog());
    Assertions.assertEquals("schema1", view.defaultSchema());
    Assertions.assertEquals(1, view.columns().length);
    Assertions.assertEquals("c1", view.columns()[0].name());
    Assertions.assertEquals(1, view.representations().length);
    Assertions.assertEquals("trino", ((SQLRepresentation) view.representations()[0]).dialect());
  }
}
