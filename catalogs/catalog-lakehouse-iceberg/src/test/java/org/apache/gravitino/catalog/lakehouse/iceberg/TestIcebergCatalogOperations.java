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

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.utils.IcebergCatalogUtil;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.exceptions.ServiceUnavailableException;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergCatalogOperations {

  private static final String METALAKE = "metalake";
  private static final String CATALOG = "catalog";

  @Test
  public void testTestConnection() {
    IcebergCatalogOperations catalogOperations = new IcebergCatalogOperations();
    Exception exception =
        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () ->
                catalogOperations.testConnection(
                    NameIdentifier.of(METALAKE, CATALOG),
                    Catalog.Type.RELATIONAL,
                    "iceberg",
                    "comment",
                    ImmutableMap.of()));
    Assertions.assertTrue(
        exception.getMessage().contains("Failed to run listNamespace on Iceberg catalog"));
  }

  @Test
  public void testHandleRestExceptionTranslatesUnavailableToConnectionFailed() {
    // Transport failure (server unreachable) is the base RESTException.
    RESTException unreachable = new RESTException("Error occurred while processing GET request");
    Assertions.assertInstanceOf(
        ConnectionFailedException.class, IcebergCatalogOperations.handleRestException(unreachable));

    // The server answered 503.
    RESTException unavailable = new ServiceUnavailableException("Service Unavailable: %s", "down");
    Assertions.assertInstanceOf(
        ConnectionFailedException.class, IcebergCatalogOperations.handleRestException(unavailable));

    // A subtype we don't translate yet is returned unchanged (surfaces as a runtime error).
    RESTException badRequest = new BadRequestException("Malformed request: %s", "bad");
    Assertions.assertSame(badRequest, IcebergCatalogOperations.handleRestException(badRequest));
  }

  @Test
  public void testListSchemasConvertsMultiLevelNamespacesToLogicalNames() {
    IcebergBackend backend = memoryBackend();
    backend
        .wrapper()
        .createNamespace(
            CreateNamespaceRequest.builder()
                .withNamespace(org.apache.iceberg.catalog.Namespace.of("A", "B", "C"))
                .build());
    IcebergCatalogOperations catalogOperations = new IcebergCatalogOperations();
    catalogOperations.backend = backend;

    NameIdentifier[] result = catalogOperations.listSchemas(Namespace.of(METALAKE, CATALOG, "A:B"));

    Assertions.assertEquals(1, result.length);
    Assertions.assertEquals("A:B:C", result[0].name());
    backend.close();
  }

  @Test
  public void testListSchemasFlatOnlyReturnsUnchangedNames() {
    IcebergBackend backend = memoryBackend();
    backend
        .wrapper()
        .createNamespace(
            CreateNamespaceRequest.builder()
                .withNamespace(org.apache.iceberg.catalog.Namespace.of("db1"))
                .build());
    backend
        .wrapper()
        .createNamespace(
            CreateNamespaceRequest.builder()
                .withNamespace(org.apache.iceberg.catalog.Namespace.of("db2"))
                .build());
    IcebergCatalogOperations catalogOperations = new IcebergCatalogOperations();
    catalogOperations.backend = backend;

    NameIdentifier[] result = catalogOperations.listSchemas(Namespace.of(METALAKE, CATALOG));

    Assertions.assertEquals(2, result.length);
    Assertions.assertTrue(Arrays.stream(result).anyMatch(id -> "db1".equals(id.name())));
    Assertions.assertTrue(Arrays.stream(result).anyMatch(id -> "db2".equals(id.name())));
    backend.close();
  }

  @Test
  public void testCatalogDropRemovesInternedMemoryCatalog() {
    String catalogUuid = "catalog-uuid";
    IcebergConfig config =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND,
                "memory",
                IcebergConstants.CATALOG_UUID,
                catalogUuid));
    Object original = IcebergCatalogUtil.loadCatalogBackend(IcebergCatalogBackend.MEMORY, config);

    IcebergCatalogOperations catalogOperations = new IcebergCatalogOperations();
    catalogOperations.catalogUuid = catalogUuid;
    catalogOperations.onCatalogDropped();

    Object replacement =
        IcebergCatalogUtil.loadCatalogBackend(IcebergCatalogBackend.MEMORY, config);
    Assertions.assertNotSame(original, replacement);
    IcebergCatalogUtil.removeMemoryCatalog(catalogUuid);
  }

  private static IcebergBackend memoryBackend() {
    return IcebergBackend.create(
        ImmutableMap.of(
            IcebergConstants.CATALOG_BACKEND, "memory", IcebergConstants.WAREHOUSE, "/tmp/wh"));
  }
}
