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
package org.apache.gravitino.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

import java.time.Instant;
import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.dto.rel.RepresentationDTO;
import org.apache.gravitino.dto.rel.SQLRepresentationDTO;
import org.apache.gravitino.dto.rel.TableDTO;
import org.apache.gravitino.dto.rel.ViewDTO;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.semantic.SupportsSemanticModels;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestMetadataObjectSemanticModelOperations extends TestBase {
  private static final Namespace SOURCE_NAMESPACE =
      Namespace.of("metalake /%? 测试", "catalog", "schema");
  private static final String SOURCE_NAME = "orders /%? 明细";

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testSourceCapabilitiesPreserveFullIdentifiers(boolean view) throws Exception {
    NameIdentifier[] expected = {
      NameIdentifier.of(SOURCE_NAMESPACE.level(0), "another_catalog", "schema", "a", "Orders.raw"),
      NameIdentifier.of(SOURCE_NAMESPACE.level(0), "another_catalog", "schema", "a", "orders.raw"),
      NameIdentifier.of(SOURCE_NAMESPACE.level(0), "another_catalog", "schema", "a-b", "收入")
    };
    buildMockResource(Method.GET, path(view), null, new EntityListResponse(expected), 200);
    SupportsSemanticModels source = source(view);
    NameIdentifier[] actual = source.listSemanticModelDatasets();
    assertArrayEquals(expected, actual);
    assertEquals(4, actual[0].namespace().length());
    assertEquals("Orders.raw", actual[0].name());
    assertEquals(
        NameIdentifier.of(SOURCE_NAMESPACE.level(0), "another_catalog", "schema", "a"),
        NameIdentifier.of(actual[0].namespace().levels()));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testEmptyResults(boolean view) throws Exception {
    buildMockResource(
        Method.GET, path(view), null, new EntityListResponse(new NameIdentifier[0]), 200);
    assertArrayEquals(new NameIdentifier[0], source(view).listSemanticModelDatasets());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testSourceErrorsAreNotEmptyResults(boolean view) throws Exception {
    List<ErrorResponse> errors =
        List.of(
            ErrorResponse.notFound(
                NoSuchMetadataObjectException.class.getSimpleName(), "source missing"),
            ErrorResponse.notFound(NotFoundException.class.getSimpleName(), "endpoint unavailable"),
            ErrorResponse.illegalArguments("invalid source"),
            ErrorResponse.forbidden("source hidden", null),
            ErrorResponse.internalError("storage unavailable", null));
    List<Class<? extends RuntimeException>> types =
        List.of(
            NoSuchMetadataObjectException.class,
            NotFoundException.class,
            IllegalArgumentException.class,
            ForbiddenException.class,
            RuntimeException.class);
    int[] statuses = {404, 404, 400, 403, 500};
    SupportsSemanticModels source = source(view);
    for (int i = 0; i < errors.size(); i++) {
      ErrorResponse error = errors.get(i);
      buildMockResource(Method.GET, path(view), null, error, statuses[i]);
      RuntimeException failure = assertThrows(types.get(i), source::listSemanticModelDatasets);
      assertEquals(types.get(i), failure.getClass());
      assertTrue(failure.getMessage().contains(error.getMessage()));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testInvalidResponseIsRejected(boolean view) throws Exception {
    buildMockResource(Method.GET, path(view), null, new EntityListResponse(), 200);
    IllegalArgumentException failure =
        assertThrows(IllegalArgumentException.class, source(view)::listSemanticModelDatasets);
    assertEquals("identifiers must not be null", failure.getMessage());
  }

  @Test
  void testDefaultCapabilitiesRemainOptional() {
    assertThrows(
        UnsupportedOperationException.class,
        mock(Table.class, CALLS_REAL_METHODS)::supportsSemanticModels);
    assertThrows(
        UnsupportedOperationException.class,
        mock(View.class, CALLS_REAL_METHODS)::supportsSemanticModels);
  }

  private static String path(boolean view) {
    return "/api/metalakes/"
        + RESTUtils.encodeString(SOURCE_NAMESPACE.level(0))
        + "/objects/"
        + (view ? "view/" : "table/")
        + RESTUtils.encodeString("catalog.schema." + SOURCE_NAME)
        + "/semantic-model-datasets";
  }

  private static SupportsSemanticModels source(boolean view) {
    AuditDTO audit = AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build();
    if (view) {
      ViewDTO dto =
          ViewDTO.builder()
              .withName(SOURCE_NAME)
              .withAudit(audit)
              .withRepresentations(
                  new RepresentationDTO[] {
                    SQLRepresentationDTO.builder().withDialect("trino").withSql("SELECT 1").build()
                  })
              .build();
      return new GenericView(dto, client.restClient(), SOURCE_NAMESPACE).supportsSemanticModels();
    }
    TableDTO dto =
        TableDTO.builder()
            .withName(SOURCE_NAME)
            .withColumns(new ColumnDTO[0])
            .withAudit(audit)
            .build();
    return RelationalTable.from(SOURCE_NAMESPACE, dto, client.restClient())
        .supportsSemanticModels();
  }
}
