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

import static org.apache.hc.core5.http.HttpStatus.SC_BAD_REQUEST;
import static org.apache.hc.core5.http.HttpStatus.SC_CONFLICT;
import static org.apache.hc.core5.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.hc.core5.http.HttpStatus.SC_OK;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.CatalogDTO;
import org.apache.gravitino.dto.requests.SemanticModelCreateRequest;
import org.apache.gravitino.dto.requests.SemanticModelUpdateRequest;
import org.apache.gravitino.dto.requests.SemanticModelUpdatesRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.SemanticModelResponse;
import org.apache.gravitino.dto.semantic.SemanticModelDTO;
import org.apache.gravitino.dto.semantic.SemanticModelDefinitionDTO;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.IllegalSemanticModelException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchSemanticModelException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.exceptions.SemanticModelAlreadyExistsException;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.semantic.AIContext;
import org.apache.gravitino.semantic.AIContextObject;
import org.apache.gravitino.semantic.CustomExtension;
import org.apache.gravitino.semantic.DataType;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.DialectExpression;
import org.apache.gravitino.semantic.Dialects;
import org.apache.gravitino.semantic.Dimension;
import org.apache.gravitino.semantic.Expression;
import org.apache.gravitino.semantic.Field;
import org.apache.gravitino.semantic.Metric;
import org.apache.gravitino.semantic.Relationship;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelCatalog;
import org.apache.gravitino.semantic.SemanticModelChange;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestSemanticModelCatalog extends TestBase {

  private static final String METALAKE_NAME = "semantic_model_metalake";
  private static final String CATALOG_NAME = "semantic_model_catalog";
  private static final String SCHEMA_NAME = "semantic_schema";
  private static final String MODEL_NAME = "sales_model";

  private static Catalog catalog;

  @BeforeAll
  public static void setUp() throws Exception {
    TestBase.setUp();
    catalog = newCatalog(Catalog.Type.RELATIONAL);
  }

  @Test
  public void testOnlyRelationalCatalogExposesSemanticModelCatalog() {
    assertSame(catalog, catalog.asSemanticModelCatalog());
    for (Catalog.Type type :
        new Catalog.Type[] {Catalog.Type.FILESET, Catalog.Type.MESSAGING, Catalog.Type.MODEL}) {
      Catalog nonRelationalCatalog = newCatalog(type);
      assertThrows(
          UnsupportedOperationException.class, nonRelationalCatalog::asSemanticModelCatalog);
    }
  }

  @Test
  public void testListSemanticModelsUsesShortClientIdentifiers() throws JsonProcessingException {
    NameIdentifier first =
        NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, "sales_model");
    NameIdentifier second =
        NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, "inventory_model");
    buildMockResource(
        Method.GET,
        withSlash(semanticModelPath()),
        null,
        new EntityListResponse(new NameIdentifier[] {first, second}),
        SC_OK);

    NameIdentifier[] actual =
        catalog.asSemanticModelCatalog().listSemanticModels(Namespace.of(SCHEMA_NAME));

    assertArrayEquals(
        new NameIdentifier[] {
          NameIdentifier.of(SCHEMA_NAME, "sales_model"),
          NameIdentifier.of(SCHEMA_NAME, "inventory_model")
        },
        actual);
  }

  @Test
  public void testLoadSemanticModelPreservesNestedDefinition() throws JsonProcessingException {
    SemanticModelDTO semanticModelDTO = semanticModelDTO(MODEL_NAME);
    String path = withSlash(semanticModelPath() + "/" + RESTUtils.encodeString(MODEL_NAME));
    buildMockResource(Method.GET, path, null, new SemanticModelResponse(semanticModelDTO), SC_OK);

    SemanticModel loaded =
        catalog
            .asSemanticModelCatalog()
            .loadSemanticModel(NameIdentifier.of(SCHEMA_NAME, MODEL_NAME));

    assertInstanceOf(GenericSemanticModel.class, loaded);
    assertSemanticModel(semanticModelDTO, loaded);
    assertEquals("Use certified metrics", loaded.definition().aiContext().object().instructions());
    assertEquals(
        new BigDecimal("0.95"),
        loaded.definition().aiContext().object().additionalProperties().get("confidence"));
    assertEquals(DataType.DATE_TIME_TZ, loaded.definition().datasets()[0].fields()[0].datatype());
    assertEquals(
        "TRINO",
        loaded.definition().datasets()[0].fields()[1].expression().dialects()[0].dialect());
    assertArrayEquals(new Field[0], loaded.definition().datasets()[1].fields());
    assertNull(loaded.definition().datasets()[2].fields());
  }

  @Test
  public void testCreateSemanticModelUsesDefinitionWrapper() throws JsonProcessingException {
    SemanticModelDefinition definition = definition();
    SemanticModelCreateRequest request =
        new SemanticModelCreateRequest(
            MODEL_NAME,
            "Governed sales metrics",
            SemanticModelDefinitionDTO.fromDefinition(definition),
            Map.of("certified", "true"));
    SemanticModelDTO semanticModelDTO = semanticModelDTO(MODEL_NAME);
    buildMockResource(
        Method.POST,
        withSlash(semanticModelPath()),
        request,
        new SemanticModelResponse(semanticModelDTO),
        SC_OK);

    SemanticModel created =
        catalog
            .asSemanticModelCatalog()
            .createSemanticModel(
                NameIdentifier.of(SCHEMA_NAME, MODEL_NAME),
                "Governed sales metrics",
                definition,
                Map.of("certified", "true"));

    JsonNode body = MAPPER.valueToTree(request);
    assertTrue(body.has("definition"));
    assertFalse(body.has("datasets"));
    assertTrue(body.path("definition").has("datasets"));
    assertTrue(body.path("definition").has("ai_context"));
    assertArrayEquals(
        new String[] {"source_catalog", "source_schema"},
        MAPPER.convertValue(
            body.path("definition").path("datasets").get(0).path("source").path("namespace"),
            String[].class));
    assertEquals(
        "orders",
        body.path("definition").path("datasets").get(0).path("source").path("name").asText());
    assertEquals(
        "DateTimeTz",
        body.path("definition")
            .path("datasets")
            .get(0)
            .path("fields")
            .get(0)
            .path("datatype")
            .asText());
    assertEquals(
        "TRINO",
        body.path("definition")
            .path("datasets")
            .get(0)
            .path("fields")
            .get(1)
            .path("expression")
            .path("dialects")
            .get(0)
            .path("dialect")
            .asText());
    assertTrue(body.path("definition").path("datasets").get(1).path("fields").isEmpty());
    assertFalse(body.path("definition").path("datasets").get(2).has("fields"));
    assertSemanticModel(semanticModelDTO, created);
  }

  @Test
  public void testAlterSemanticModelConvertsAllChangeTypes() throws JsonProcessingException {
    SemanticModelDefinition replacement = definition();
    SemanticModelUpdatesRequest request =
        new SemanticModelUpdatesRequest(
            List.of(
                new SemanticModelUpdateRequest.RenameSemanticModelRequest("renamed_model"),
                new SemanticModelUpdateRequest.UpdateSemanticModelCommentRequest(""),
                new SemanticModelUpdateRequest.SetSemanticModelPropertyRequest(
                    "owner", "analytics"),
                new SemanticModelUpdateRequest.RemoveSemanticModelPropertyRequest("deprecated"),
                new SemanticModelUpdateRequest.ReplaceSemanticModelDefinitionRequest(
                    SemanticModelDefinitionDTO.fromDefinition(replacement))));
    SemanticModelDTO responseDTO = semanticModelDTO("renamed_model");
    String path = withSlash(semanticModelPath() + "/" + RESTUtils.encodeString(MODEL_NAME));
    buildMockResource(Method.PUT, path, request, new SemanticModelResponse(responseDTO), SC_OK);

    SemanticModel altered =
        catalog
            .asSemanticModelCatalog()
            .alterSemanticModel(
                NameIdentifier.of(SCHEMA_NAME, MODEL_NAME),
                SemanticModelChange.rename("renamed_model"),
                SemanticModelChange.updateComment(""),
                SemanticModelChange.setProperty("owner", "analytics"),
                SemanticModelChange.removeProperty("deprecated"),
                SemanticModelChange.replaceDefinition(replacement));

    JsonNode serializedUpdates = MAPPER.valueToTree(request).path("updates");
    assertEquals(
        List.of("rename", "updateComment", "setProperty", "removeProperty", "replaceDefinition"),
        Arrays.asList(
            serializedUpdates.get(0).path("@type").asText(),
            serializedUpdates.get(1).path("@type").asText(),
            serializedUpdates.get(2).path("@type").asText(),
            serializedUpdates.get(3).path("@type").asText(),
            serializedUpdates.get(4).path("@type").asText()));
    assertEquals("", serializedUpdates.get(1).path("newComment").asText());
    assertTrue(serializedUpdates.get(4).path("definition").has("datasets"));
    assertSemanticModel(responseDTO, altered);
  }

  @Test
  public void testDropSemanticModel() throws JsonProcessingException {
    String path = withSlash(semanticModelPath() + "/" + RESTUtils.encodeString(MODEL_NAME));
    buildMockResource(Method.DELETE, path, null, new DropResponse(true), SC_OK);
    buildMockResource(Method.DELETE, path, null, new DropResponse(false), SC_OK);

    SemanticModelCatalog semanticModels = catalog.asSemanticModelCatalog();
    NameIdentifier ident = NameIdentifier.of(SCHEMA_NAME, MODEL_NAME);
    assertTrue(semanticModels.dropSemanticModel(ident));
    assertFalse(semanticModels.dropSemanticModel(ident));
  }

  @Test
  public void testSemanticModelErrorMappings() throws JsonProcessingException {
    NameIdentifier ident = NameIdentifier.of(SCHEMA_NAME, MODEL_NAME);
    String entityPath = withSlash(semanticModelPath() + "/" + RESTUtils.encodeString(MODEL_NAME));
    ErrorResponse missing =
        ErrorResponse.notFound(
            NoSuchSemanticModelException.class.getSimpleName(), "Semantic Model not found");
    buildMockResource(Method.GET, entityPath, null, missing, SC_NOT_FOUND);
    assertThrows(
        NoSuchSemanticModelException.class,
        () -> catalog.asSemanticModelCatalog().loadSemanticModel(ident));

    SemanticModelDefinition definition = definition();
    SemanticModelCreateRequest createRequest =
        new SemanticModelCreateRequest(
            MODEL_NAME,
            null,
            SemanticModelDefinitionDTO.fromDefinition(definition),
            Collections.emptyMap());
    ErrorResponse exists =
        ErrorResponse.alreadyExists(
            SemanticModelAlreadyExistsException.class.getSimpleName(),
            "Semantic Model already exists");
    buildMockResource(
        Method.POST, withSlash(semanticModelPath()), createRequest, exists, SC_CONFLICT);
    assertThrows(
        SemanticModelAlreadyExistsException.class,
        () ->
            catalog
                .asSemanticModelCatalog()
                .createSemanticModel(ident, null, definition, Collections.emptyMap()));

    ErrorResponse invalid =
        ErrorResponse.illegalArguments(
            IllegalSemanticModelException.class.getSimpleName(), "Illegal Semantic Model", null);
    buildMockResource(
        Method.POST, withSlash(semanticModelPath()), createRequest, invalid, SC_BAD_REQUEST);
    assertThrows(
        IllegalSemanticModelException.class,
        () ->
            catalog
                .asSemanticModelCatalog()
                .createSemanticModel(ident, null, definition, Collections.emptyMap()));

    ErrorResponse unavailable = ErrorResponse.connectionFailed("Source catalog unavailable");
    assertThrows(
        ConnectionFailedException.class,
        () -> ErrorHandlers.semanticModelErrorHandler().accept(unavailable));

    ErrorResponse forbidden = ErrorResponse.forbidden("Source metadata is not visible", null);
    assertThrows(
        ForbiddenException.class,
        () -> ErrorHandlers.semanticModelErrorHandler().accept(forbidden));

    ErrorResponse unsupported =
        ErrorResponse.unsupportedOperation("Catalog is not relational", null);
    assertThrows(
        UnsupportedOperationException.class,
        () -> ErrorHandlers.semanticModelErrorHandler().accept(unsupported));

    ErrorResponse conflict =
        ErrorResponse.optimisticLockConflict(
            OptimisticLockException.class.getSimpleName(),
            "Concurrent Semantic Model update",
            null);
    assertThrows(
        OptimisticLockException.class,
        () -> ErrorHandlers.semanticModelErrorHandler().accept(conflict));
  }

  @Test
  public void testNamespacePathAndBatchValidation() throws JsonProcessingException {
    assertEquals(
        "api/metalakes/semantic_model_metalake/catalogs/semantic_model_catalog/schemas/"
            + "semantic_schema/semantic-models",
        semanticModelPath());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            catalog
                .asSemanticModelCatalog()
                .listSemanticModels(Namespace.of(CATALOG_NAME, SCHEMA_NAME)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            catalog
                .asSemanticModelCatalog()
                .loadSemanticModel(NameIdentifier.of(CATALOG_NAME, SCHEMA_NAME, MODEL_NAME)));

    NameIdentifier ident = NameIdentifier.of(SCHEMA_NAME, MODEL_NAME);
    assertThrows(
        IllegalArgumentException.class,
        () -> catalog.asSemanticModelCatalog().alterSemanticModel(ident));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            catalog
                .asSemanticModelCatalog()
                .alterSemanticModel(ident, (SemanticModelChange[]) null));

    ErrorResponse missingSchema =
        ErrorResponse.notFound(NoSuchSchemaException.class.getSimpleName(), "Schema not found");
    buildMockResource(
        Method.GET, withSlash(semanticModelPath()), null, missingSchema, SC_NOT_FOUND);
    assertThrows(
        NoSuchSchemaException.class,
        () -> catalog.asSemanticModelCatalog().listSemanticModels(Namespace.of(SCHEMA_NAME)));
  }

  @Test
  public void testUnknownSemanticModelChangeIsRejected() {
    SemanticModelChange unknown = new SemanticModelChange() {};
    assertThrows(
        IllegalArgumentException.class, () -> DTOConverters.toSemanticModelUpdateRequest(unknown));
    assertThrows(
        IllegalArgumentException.class, () -> DTOConverters.toSemanticModelUpdateRequest(null));
  }

  private static Catalog newCatalog(Catalog.Type type) {
    CatalogDTO catalogDTO =
        CatalogDTO.builder()
            .withName(CATALOG_NAME)
            .withType(type)
            .withProvider("test")
            .withComment("comment")
            .withProperties(Collections.emptyMap())
            .withAudit(audit())
            .build();
    return DTOConverters.toCatalog(METALAKE_NAME, catalogDTO, client.restClient());
  }

  private static String semanticModelPath() {
    return SemanticModelCatalogOperations.formatSemanticModelRequestPath(
        Namespace.of(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME));
  }

  private static SemanticModelDTO semanticModelDTO(String name) {
    return SemanticModelDTO.builder()
        .withName(name)
        .withComment("Governed sales metrics")
        .withDefinition(SemanticModelDefinitionDTO.fromDefinition(definition()))
        .withProperties(Map.of("certified", "true"))
        .withAudit(audit())
        .build();
  }

  private static SemanticModelDefinition definition() {
    Map<String, Object> additionalProperties = new LinkedHashMap<>();
    additionalProperties.put("confidence", new BigDecimal("0.95"));
    additionalProperties.put("hints", List.of("month", "region"));
    AIContextObject modelContext =
        AIContextObject.builder()
            .withInstructions("Use certified metrics")
            .withSynonyms(new String[] {"sales", "revenue"})
            .withExamples(new String[] {"Revenue by month"})
            .withAdditionalProperties(additionalProperties)
            .build();
    CustomExtension extension =
        CustomExtension.builder().withVendorName("example").withData("{\"tier\":\"gold\"}").build();
    Field orderTime =
        Field.builder()
            .withName("order_time")
            .withExpression(expression(Dialects.ANSI_SQL, "order_time"))
            .withDimension(Dimension.builder().withIsTime(true).build())
            .withLabel("Order time")
            .withDescription("Time the order was placed")
            .withDatatype(DataType.DATE_TIME_TZ)
            .withAIContext(AIContext.of("Use the business timezone"))
            .withCustomExtensions(new CustomExtension[0])
            .build();
    Field orderAmount =
        Field.builder()
            .withName("order_amount")
            .withExpression(expression("TRINO", "order_amount"))
            .withDatatype(DataType.DECIMAL)
            .withCustomExtensions(new CustomExtension[] {extension})
            .build();
    Dataset orders =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of("source_catalog", "source_schema", "orders"))
            .withPrimaryKey(new String[] {"order_id"})
            .withUniqueKeys(new String[0][])
            .withDescription("Governed order facts")
            .withAIContext(AIContext.of("Use completed orders"))
            .withFields(new Field[] {orderTime, orderAmount})
            .withCustomExtensions(new CustomExtension[0])
            .build();
    Dataset customers =
        Dataset.builder()
            .withName("customers")
            .withSource(NameIdentifier.of("source_catalog", "source_schema", "customers"))
            .withUniqueKeys(new String[][] {{"email"}})
            .withFields(new Field[0])
            .build();
    Dataset regions =
        Dataset.builder()
            .withName("regions")
            .withSource(NameIdentifier.of("source_catalog", "source_schema", "regions"))
            .build();
    Relationship relationship =
        Relationship.builder()
            .withName("orders_to_customers")
            .withFrom("orders")
            .withTo("customers")
            .withFromColumns(new String[] {"customer_id"})
            .withToColumns(new String[] {"id"})
            .withAIContext(AIContext.of("Join orders to customer attributes"))
            .withCustomExtensions(new CustomExtension[] {extension})
            .build();
    Metric revenue =
        Metric.builder()
            .withName("revenue")
            .withExpression(expression(Dialects.ANSI_SQL, "SUM(orders.order_amount)"))
            .withDescription("Certified revenue")
            .withDatatype(DataType.DECIMAL)
            .withAIContext(AIContext.of(modelContext))
            .withCustomExtensions(new CustomExtension[] {extension})
            .build();
    return SemanticModelDefinition.builder()
        .withAIContext(AIContext.of(modelContext))
        .withDatasets(new Dataset[] {orders, customers, regions})
        .withRelationships(new Relationship[] {relationship})
        .withMetrics(new Metric[] {revenue})
        .withCustomExtensions(new CustomExtension[] {extension})
        .build();
  }

  private static Expression expression(String dialect, String value) {
    return Expression.builder()
        .withDialects(
            new DialectExpression[] {
              DialectExpression.builder().withDialect(dialect).withExpression(value).build()
            })
        .build();
  }

  private static AuditDTO audit() {
    return AuditDTO.builder()
        .withCreator("creator")
        .withCreateTime(Instant.parse("2026-08-11T00:00:00Z"))
        .withLastModifier("modifier")
        .withLastModifiedTime(Instant.parse("2026-08-12T00:00:00Z"))
        .build();
  }

  private static void assertSemanticModel(SemanticModel expected, SemanticModel actual) {
    assertEquals(expected.name(), actual.name());
    assertEquals(expected.comment(), actual.comment());
    assertEquals(expected.definition(), actual.definition());
    assertEquals(expected.properties(), actual.properties());
    assertEquals(expected.auditInfo(), actual.auditInfo());
  }
}
