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
import java.util.Map;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.view.ViewMetadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergView {

  @Test
  public void testFromLoadViewResponse() {
    // Test with properties
    Map<String, String> properties = ImmutableMap.of("key1", "value1", "key2", "value2");
    ViewMetadata mockMetadata = mock(ViewMetadata.class);
    when(mockMetadata.properties()).thenReturn(properties);

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
    ViewMetadata mockMetadata = mock(ViewMetadata.class);
    when(mockMetadata.properties()).thenReturn(null);

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
}
