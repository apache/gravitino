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
package org.apache.gravitino.trino.connector.catalog;

import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.trino.spi.TrinoException;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.ViewCatalog;
import org.junit.jupiter.api.Test;

public class TestCatalogConnectorMetadataView {

  @Test
  public void testViewExists() {
    ViewCatalog viewCatalog = mock(ViewCatalog.class);
    when(viewCatalog.viewExists(any())).thenReturn(true);

    assertTrue(createMetadata(viewCatalog).viewExists("s", "v"));
    verify(viewCatalog).viewExists(NameIdentifier.of("s", "v"));
  }

  @Test
  public void testCatalogWithoutViewCatalogHasNoViews() {
    assertFalse(createMetadata(null).viewExists("s", "v"));
  }

  @Test
  public void testServerWithoutViewSupportHasNoViews() {
    ViewCatalog viewCatalog = mock(ViewCatalog.class);
    when(viewCatalog.viewExists(any()))
        .thenThrow(new UnsupportedOperationException("Views are not supported"));

    assertFalse(createMetadata(viewCatalog).viewExists("s", "v"));
  }

  @Test
  public void testMissingSchemaHasNoViews() {
    ViewCatalog viewCatalog = mock(ViewCatalog.class);
    when(viewCatalog.viewExists(any())).thenThrow(new NoSuchSchemaException("missing"));

    assertFalse(createMetadata(viewCatalog).viewExists("s", "v"));
  }

  @Test
  public void testViewExistsDeniedIsPermissionDenied() {
    ViewCatalog viewCatalog = mock(ViewCatalog.class);
    when(viewCatalog.viewExists(any())).thenThrow(new ForbiddenException("denied"));
    CatalogConnectorMetadata metadata = createMetadata(viewCatalog);

    TrinoException e = assertThrows(TrinoException.class, () -> metadata.viewExists("s", "v"));
    assertEquals(PERMISSION_DENIED.toErrorCode(), e.getErrorCode());
  }

  private static CatalogConnectorMetadata createMetadata(ViewCatalog viewCatalog) {
    GravitinoMetalake metalake = mock(GravitinoMetalake.class);
    Catalog catalog = mock(Catalog.class);
    when(metalake.loadCatalog(anyString())).thenReturn(catalog);
    when(catalog.asSchemas()).thenReturn(mock(SupportsSchemas.class));
    when(catalog.asTableCatalog()).thenReturn(mock(TableCatalog.class));
    when(catalog.asFunctionCatalog()).thenThrow(new UnsupportedOperationException("unsupported"));
    if (viewCatalog == null) {
      when(catalog.asViewCatalog()).thenThrow(new UnsupportedOperationException("unsupported"));
    } else {
      when(catalog.asViewCatalog()).thenReturn(viewCatalog);
    }
    return new CatalogConnectorMetadata(metalake, NameIdentifier.of("m", "c"));
  }
}
