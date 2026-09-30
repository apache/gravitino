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
package org.apache.gravitino.semantic;

import static org.apache.gravitino.semantic.SemanticModel.DEFAULT_OSSIE_VERSION;
import static org.apache.gravitino.semantic.SemanticModel.PROPERTY_OSSIE_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.dto.requests.SemanticModelCreateRequest;
import org.apache.gravitino.exceptions.IllegalSemanticModelException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.SemanticModelEntity;
import org.junit.jupiter.api.Test;

/** Tests standalone Apache Ossie document import, export, and conversion diagnostics. */
public class TestOssieSemanticModelDocumentConverter {

  private static final JsonMapper JSON_MAPPER = JsonMapper.builder().build();

  @Test
  public void testImportYamlAndJsonDocuments() {
    String yaml =
        """
        version: 0.2.0.dev0
        name: sales
        description: Governed sales definitions
        ai_context:
          instructions: Use certified definitions
          confidence: 0.95
        datasets:
          - name: orders
            source: sales.mart.orders
            primary_key: [order_id]
            fields:
              - name: order_id
                expression:
                  dialects:
                    - dialect: ANSI_SQL
                      expression: order_id
                datatype: String
                dimension: {}
        relationships: []
        metrics:
          - name: revenue
            expression:
              dialects:
                - dialect: DAX
                  expression: SUM(orders.amount)
            datatype: Decimal
        custom_extensions:
          - vendor_name: EXAMPLE
            data: '{"certified":true}'
          - vendor_name: GRAVITINO
            data: '{"_apache_gravitino_interchange":{"version":1,"properties":{"domain":"sales"}}}'
        """;

    SemanticModelCreateRequest yamlRequest =
        OssieSemanticModelDocumentConverter.importDocument(OssieDocument.yaml(yaml));
    assertEquals("sales", yamlRequest.getName());
    assertEquals("Governed sales definitions", yamlRequest.getComment());
    assertEquals(
        Map.of("domain", "sales", PROPERTY_OSSIE_VERSION, DEFAULT_OSSIE_VERSION),
        yamlRequest.getProperties());
    SemanticModelDefinition yamlDefinition = yamlRequest.toDefinition();
    assertEquals(
        NameIdentifier.of("sales", "mart", "orders"), yamlDefinition.datasets()[0].source());
    assertEquals("DAX", yamlDefinition.metrics()[0].expression().dialects()[0].dialect());
    assertEquals(1, yamlDefinition.customExtensions().length);
    assertEquals("EXAMPLE", yamlDefinition.customExtensions()[0].vendorName());
    assertEquals("Use certified definitions", yamlDefinition.aiContext().object().instructions());
    assertEquals(
        "0.95",
        yamlDefinition.aiContext().object().additionalProperties().get("confidence").toString());

    String json =
        """
        {
          "version": "future-version",
          "name": "inventory",
          "datasets": [
            {"name": "items", "source": "sales.mart.items", "fields": []}
          ]
        }
        """;
    SemanticModelCreateRequest jsonRequest =
        OssieSemanticModelDocumentConverter.importDocument(OssieDocument.json(json));
    assertEquals("inventory", jsonRequest.getName());
    assertEquals("future-version", jsonRequest.getProperties().get(PROPERTY_OSSIE_VERSION));
    assertEquals(
        NameIdentifier.of("sales", "mart", "items"),
        jsonRequest.toDefinition().datasets()[0].source());
  }

  @Test
  public void testExportJsonAndYamlRoundTrip() throws Exception {
    SemanticModel semanticModel =
        semanticModel(
            definition(), Map.of("domain", "sales", PROPERTY_OSSIE_VERSION, "future-version"));

    OssieDocument jsonDocument =
        OssieSemanticModelDocumentConverter.exportDocument(semanticModel, OssieFormat.JSON);
    assertEquals(OssieFormat.JSON, jsonDocument.format());
    JsonNode json = JSON_MAPPER.readTree(jsonDocument.content());
    assertEquals("future-version", json.path("version").textValue());
    assertEquals("sales", json.path("name").textValue());
    assertEquals("Governed sales definitions", json.path("description").textValue());
    assertEquals("sales.mart.orders", json.at("/datasets/0/source").textValue());
    assertEquals("order_id", json.at("/datasets/0/unique_keys/0/0").textValue());
    assertTrue(json.at("/datasets/0/fields/0/dimension").isObject());
    assertTrue(json.at("/datasets/0/fields/0/dimension").has("is_time"));
    assertFalse(json.at("/datasets/0/fields/0/dimension/is_time").booleanValue());
    assertEquals("Use order identifiers", json.at("/datasets/0/fields/0/ai_context").textValue());
    assertEquals(
        "FIELD_VENDOR",
        json.at("/datasets/0/fields/0/custom_extensions/0/vendor_name").textValue());
    assertFalse(json.has("semantic_model"));
    assertFalse(json.has("definition"));
    assertFalse(json.has("properties"));
    assertFalse(json.has("audit"));
    assertEquals("GRAVITINO", json.at("/custom_extensions/1/vendor_name").textValue());
    JsonNode marker = JSON_MAPPER.readTree(json.at("/custom_extensions/1/data").textValue());
    assertEquals(
        "sales", marker.at("/_apache_gravitino_interchange/properties/domain").textValue());
    assertFalse(marker.at("/_apache_gravitino_interchange/properties").has(PROPERTY_OSSIE_VERSION));

    SemanticModelCreateRequest jsonRoundTrip =
        OssieSemanticModelDocumentConverter.importDocument(jsonDocument);
    assertEquals(semanticModel.name(), jsonRoundTrip.getName());
    assertEquals(semanticModel.comment(), jsonRoundTrip.getComment());
    assertEquals(semanticModel.definition(), jsonRoundTrip.toDefinition());
    assertEquals(semanticModel.properties(), jsonRoundTrip.getProperties());

    OssieDocument yamlDocument =
        OssieSemanticModelDocumentConverter.exportDocument(semanticModel, OssieFormat.YAML);
    assertEquals(OssieFormat.YAML, yamlDocument.format());
    assertTrue(yamlDocument.content().contains("version:"));
    assertFalse(yamlDocument.content().contains("semantic_model:"));
    SemanticModelCreateRequest yamlRoundTrip =
        OssieSemanticModelDocumentConverter.importDocument(yamlDocument);
    assertEquals(semanticModel.definition(), yamlRoundTrip.toDefinition());
    assertEquals(semanticModel.properties(), yamlRoundTrip.getProperties());
  }

  @Test
  public void testQuotedSourceRoundTrip() throws Exception {
    NameIdentifier source = NameIdentifier.of("sales.eu", "ma`rt", "ord.ers");
    Dataset dataset = Dataset.builder().withName("orders").withSource(source).build();
    SemanticModelDefinition definition =
        SemanticModelDefinition.builder().withDatasets(new Dataset[] {dataset}).build();

    OssieDocument document =
        OssieSemanticModelDocumentConverter.exportDocument(
            semanticModel(definition, Map.of()), OssieFormat.JSON);
    JsonNode json = JSON_MAPPER.readTree(document.content());
    assertEquals("`sales.eu`.`ma``rt`.`ord.ers`", json.at("/datasets/0/source").textValue());

    SemanticModelCreateRequest roundTrip =
        OssieSemanticModelDocumentConverter.importDocument(document);
    assertEquals(source, roundTrip.toDefinition().datasets()[0].source());
  }

  @Test
  public void testExportDefaultsMissingOssieVersion() throws Exception {
    SemanticModel semanticModel = semanticModel(definition(), Map.of());

    OssieDocument document =
        OssieSemanticModelDocumentConverter.exportDocument(semanticModel, OssieFormat.JSON);
    JsonNode json = JSON_MAPPER.readTree(document.content());

    assertEquals(DEFAULT_OSSIE_VERSION, json.path("version").textValue());
    SemanticModelCreateRequest roundTrip =
        OssieSemanticModelDocumentConverter.importDocument(document);
    assertEquals(DEFAULT_OSSIE_VERSION, roundTrip.getProperties().get(PROPERTY_OSSIE_VERSION));
  }

  @Test
  public void testRejectsMalformedAndUnsupportedDocuments() {
    assertInvalid("", "must not be empty");
    assertInvalid("[]", "root must be an object");
    assertInvalid("name: sales\ndatasets: []\n", "$.version: must be a non-empty string");
    assertInvalid("version: ' '\nname: sales\ndatasets: []\n", "must be a non-empty string");
    assertInvalid("version: 0.2.0.dev0\nsemantic_model: []\n", "semantic_model");
    assertInvalid(
        "version: 0.2.0.dev0\nname: sales\ndescription: null\ndatasets: []\n",
        "description: must not be null");
    assertInvalid("version: 0.2.0.dev0\nname: sales\nunknown: true\ndatasets: []\n", "unknown");
    assertInvalid(
        "version: 0.2.0.dev0\n"
            + "name: sales\n"
            + "datasets:\n"
            + "  - name: orders\n"
            + "    source: SELECT * FROM orders\n",
        "query sources are not supported");
    assertInvalid(
        "version: 0.2.0.dev0\n"
            + "name: sales\n"
            + "datasets:\n"
            + "  - name: orders\n"
            + "    source: '`sales.eu.mart.orders'\n",
        "unterminated quoted segment");
    assertInvalid(
        "version: 0.2.0.dev0\n"
            + "name: sales\n"
            + "datasets:\n"
            + "  - name: orders\n"
            + "    source: 'sales.ma`rt.orders'\n",
        "backticks must quote an entire segment");
    assertInvalid(
        "version: 0.2.0.dev0\n"
            + "name: sales\n"
            + "datasets:\n"
            + "  - name: orders\n"
            + "    source: sales.mart.orders\n"
            + "    fields:\n"
            + "      - name: id\n"
            + "        expression:\n"
            + "          dialects:\n"
            + "            - dialect: 123\n"
            + "              expression: id\n",
        "dialect: must be a string");
    assertInvalid(
        "version: 0.2.0.dev0\n" + "name: first\n" + "name: second\n" + "datasets: []\n",
        "Duplicate field 'name'");
    assertInvalid(
        "version: 0.2.0.dev0\nname: first\ndatasets: []\n"
            + "---\nversion: 0.2.0.dev0\nname: second\ndatasets: []\n",
        "Trailing token");
    assertInvalid(
        "x".repeat(OssieSemanticModelDocumentConverter.MAX_DOCUMENT_LENGTH + 1),
        "exceeds the maximum length");
  }

  @Test
  public void testRejectsIncorrectOssieTypes() {
    assertInvalid("version: 0.2.0.dev0\nname: 123\ndatasets: []\n", "$.name: must be a string");
    assertInvalid(
        "version: 0.2.0.dev0\n"
            + "name: sales\n"
            + "datasets:\n"
            + "  - name: orders\n"
            + "    source: sales.mart.orders\n"
            + "    primary_key: [123]\n",
        "$.datasets[0].primary_key[0]: must be a string");
    assertInvalid(
        "version: 0.2.0.dev0\n"
            + "name: sales\n"
            + "datasets:\n"
            + "  - name: orders\n"
            + "    source: sales.mart.orders\n"
            + "    fields:\n"
            + "      - name: id\n"
            + "        expression:\n"
            + "          dialects:\n"
            + "            - dialect: ANSI_SQL\n"
            + "              expression: id\n"
            + "        dimension:\n"
            + "          is_time: 'true'\n",
        "$.datasets[0].fields[0].dimension.is_time: must be a boolean");
    assertInvalid(
        "version: 0.2.0.dev0\nname: sales\ndatasets: []\nmetrics: [123]\n",
        "$.metrics[0]: must be an object");
  }

  @Test
  public void testJsonFormatDoesNotFallBackToYaml() {
    String yaml =
        "version: 0.2.0.dev0\nname: sales\ndatasets:\n"
            + "  - name: orders\n    source: sales.mart.orders\n";
    assertEquals(
        "sales",
        OssieSemanticModelDocumentConverter.importDocument(OssieDocument.yaml(yaml)).getName());
    assertInvalid(OssieDocument.json(yaml), "Cannot parse Apache Ossie JSON");
    assertInvalid(
        OssieDocument.json("{version: '0.2.0.dev0', name: sales, datasets: []}"),
        "Cannot parse Apache Ossie JSON");
  }

  @Test
  public void testRejectsMalformedJsonDocuments() {
    assertInvalid(OssieDocument.json(" "), "must not be empty");
    assertInvalid(OssieDocument.json("[]"), "root must be an object");
    assertInvalid(
        OssieDocument.json("{\"name\":\"first\",\"name\":\"second\"}"), "Duplicate field 'name'");
    assertInvalid(OssieDocument.json("{} {}"), "Trailing token");
    assertInvalid(OssieDocument.json("{\"name\":}"), "Cannot parse Apache Ossie JSON");
    assertInvalid(
        OssieDocument.json("x".repeat(OssieSemanticModelDocumentConverter.MAX_DOCUMENT_LENGTH + 1)),
        "exceeds the maximum length");
  }

  @Test
  public void testLimitsDocumentNesting() {
    String arrays = "[".repeat(110) + "0" + "]".repeat(110);
    assertInvalid(OssieDocument.json("{\"nested\":" + arrays + "}"), "nesting depth");
    assertInvalid(OssieDocument.yaml("nested: " + arrays), "nesting depth");
    String objects = "{\"nested\":".repeat(110) + "0" + "}".repeat(110);
    assertInvalid(OssieDocument.json(objects), "nesting depth");
    assertInvalid(OssieDocument.yaml(objects), "nesting depth");
  }

  @Test
  public void testRejectsConflictingOssieVersionPropertyOnImport() {
    String document =
        """
        version: root-version
        name: sales
        datasets:
          - name: orders
            source: sales.mart.orders
        custom_extensions:
          - vendor_name: GRAVITINO
            data: '{"_apache_gravitino_interchange":{"version":1,"properties":{"ossie-version":"extension-version"}}}'
        """;

    assertInvalid(document, "property 'ossie-version' conflicts with $.version");
  }

  @Test
  public void testLimitsNestedCustomExtensionParsing() throws Exception {
    String nested = "{}";
    for (int depth = 0; depth < 110; depth++) {
      nested = "{\"nested\":" + nested + "}";
    }
    String payload =
        "{\"_apache_gravitino_interchange\":{\"version\":1,\"properties\":{\"domain\":\"sales\"}},\"nested\":"
            + nested
            + "}";

    SemanticModelCreateRequest request = importWithGravitinoExtension(payload);
    assertFalse(request.getProperties().containsKey("domain"));
    assertEquals(payload, request.toDefinition().customExtensions()[0].data());
  }

  @Test
  public void testPreservesMalformedCustomExtensionJsonWithoutExtractingProperties()
      throws Exception {
    String marker =
        "{\"_apache_gravitino_interchange\":{\"version\":1,\"properties\":{\"domain\":\"sales\"}}}";
    String[] payloads = {
      marker + " {}",
      marker.substring(0, marker.length() - 1) + ",\"_apache_gravitino_interchange\":{}}"
    };
    for (String payload : payloads) {
      SemanticModelCreateRequest request = importWithGravitinoExtension(payload);
      assertFalse(request.getProperties().containsKey("domain"));
      assertEquals(payload, request.toDefinition().customExtensions()[0].data());
    }
  }

  @Test
  public void testRejectsBlankOssieVersionPropertyOnExport() {
    IllegalSemanticModelException exception =
        assertThrows(
            IllegalSemanticModelException.class,
            () ->
                OssieSemanticModelDocumentConverter.exportDocument(
                    semanticModel(definition(), Map.of(PROPERTY_OSSIE_VERSION, " ")),
                    OssieFormat.JSON));

    assertTrue(exception.getMessage().contains("property 'ossie-version' must not be blank"));
  }

  @Test
  public void testRejectsReservedExtensionCollisionOnExport() {
    CustomExtension reserved =
        CustomExtension.builder()
            .withVendorName("GRAVITINO")
            .withData("{\"_apache_gravitino_interchange\":{\"version\":1,\"properties\":{}}}")
            .build();
    SemanticModelDefinition definition =
        SemanticModelDefinition.builder()
            .withDatasets(new Dataset[] {dataset()})
            .withCustomExtensions(new CustomExtension[] {reserved})
            .build();

    IllegalSemanticModelException exception =
        assertThrows(
            IllegalSemanticModelException.class,
            () ->
                OssieSemanticModelDocumentConverter.exportDocument(
                    semanticModel(definition, Map.of()), OssieFormat.JSON));
    assertTrue(exception.getMessage().contains("reserved Gravitino interchange marker"));
  }

  @Test
  public void testPreservesCustomDialectOnImportAndExport() throws Exception {
    Metric metric =
        Metric.builder()
            .withName("revenue")
            .withExpression(expression("TRINO", "SUM(orders.amount)"))
            .build();
    SemanticModelDefinition definition =
        SemanticModelDefinition.builder()
            .withDatasets(new Dataset[] {dataset()})
            .withMetrics(new Metric[] {metric})
            .build();

    OssieDocument document =
        OssieSemanticModelDocumentConverter.exportDocument(
            semanticModel(definition, Map.of()), OssieFormat.JSON);
    JsonNode json = JSON_MAPPER.readTree(document.content());
    assertEquals(
        "TRINO",
        json.path("metrics")
            .get(0)
            .path("expression")
            .path("dialects")
            .get(0)
            .path("dialect")
            .textValue());

    SemanticModelCreateRequest roundTrip =
        OssieSemanticModelDocumentConverter.importDocument(document);
    assertEquals(
        "TRINO", roundTrip.toDefinition().metrics()[0].expression().dialects()[0].dialect());
  }

  private static SemanticModelCreateRequest importWithGravitinoExtension(String payload)
      throws Exception {
    ObjectNode root = JSON_MAPPER.createObjectNode();
    root.put("version", DEFAULT_OSSIE_VERSION);
    root.put("name", "sales");
    ObjectNode dataset = root.putArray("datasets").addObject();
    dataset.put("name", "orders");
    dataset.put("source", "sales.mart.orders");
    ObjectNode extension = root.putArray("custom_extensions").addObject();
    extension.put("vendor_name", "GRAVITINO");
    extension.put("data", payload);
    return OssieSemanticModelDocumentConverter.importDocument(
        OssieDocument.json(JSON_MAPPER.writeValueAsString(root)));
  }

  private static SemanticModel semanticModel(
      SemanticModelDefinition definition, Map<String, String> properties) {
    return SemanticModelEntity.builder()
        .withId(1L)
        .withName("sales")
        .withNamespace(Namespace.of("lake", "sales", "mart"))
        .withComment("Governed sales definitions")
        .withDefinition(definition)
        .withProperties(properties)
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator("tester")
                .withCreateTime(Instant.parse("2026-09-28T00:00:00Z"))
                .build())
        .build();
  }

  private static SemanticModelDefinition definition() {
    Dataset orders = dataset();
    Dataset customers =
        Dataset.builder()
            .withName("customers")
            .withSource(NameIdentifier.of("sales", "mart", "customers"))
            .build();
    Relationship relationship =
        Relationship.builder()
            .withName("orders_to_customers")
            .withFrom("orders")
            .withTo("customers")
            .withFromColumns(new String[] {"customer_id"})
            .withToColumns(new String[] {"customer_id"})
            .withAIContext(AIContext.of("Join orders to customers"))
            .withCustomExtensions(new CustomExtension[] {extension("RELATIONSHIP_VENDOR")})
            .build();
    Metric metric =
        Metric.builder()
            .withName("revenue")
            .withExpression(expression("SUM(orders.amount)"))
            .withDescription("Recognized revenue")
            .withDatatype(DataType.DECIMAL)
            .withAIContext(AIContext.of("Use the certified revenue metric"))
            .withCustomExtensions(new CustomExtension[] {extension("METRIC_VENDOR")})
            .build();
    return SemanticModelDefinition.builder()
        .withAIContext(AIContext.of("Use certified definitions"))
        .withDatasets(new Dataset[] {orders, customers})
        .withRelationships(new Relationship[] {relationship})
        .withMetrics(new Metric[] {metric})
        .withCustomExtensions(new CustomExtension[] {extension("EXAMPLE")})
        .build();
  }

  private static Dataset dataset() {
    Field field =
        Field.builder()
            .withName("order_id")
            .withExpression(expression("order_id"))
            .withDimension(Dimension.builder().withIsTime(false).build())
            .withLabel("Order ID")
            .withDescription("Unique order identifier")
            .withDatatype(DataType.STRING)
            .withAIContext(AIContext.of("Use order identifiers"))
            .withCustomExtensions(new CustomExtension[] {extension("FIELD_VENDOR")})
            .build();
    return Dataset.builder()
        .withName("orders")
        .withSource(NameIdentifier.of("sales", "mart", "orders"))
        .withPrimaryKey(new String[] {"order_id"})
        .withUniqueKeys(new String[][] {{"order_id"}})
        .withDescription("Orders dataset")
        .withAIContext(AIContext.of("Use certified orders"))
        .withFields(new Field[] {field})
        .withCustomExtensions(new CustomExtension[] {extension("DATASET_VENDOR")})
        .build();
  }

  private static CustomExtension extension(String vendorName) {
    return CustomExtension.builder()
        .withVendorName(vendorName)
        .withData("{\"certified\":true}")
        .build();
  }

  private static Expression expression(String value) {
    return expression(Dialects.ANSI_SQL, value);
  }

  private static Expression expression(String dialectName, String value) {
    DialectExpression dialect =
        DialectExpression.builder().withDialect(dialectName).withExpression(value).build();
    return Expression.builder().withDialects(new DialectExpression[] {dialect}).build();
  }

  private static void assertInvalid(String document, String expectedMessage) {
    assertInvalid(OssieDocument.yaml(document), expectedMessage);
  }

  private static void assertInvalid(OssieDocument document, String expectedMessage) {
    IllegalSemanticModelException exception =
        assertThrows(
            IllegalSemanticModelException.class,
            () -> OssieSemanticModelDocumentConverter.importDocument(document));
    assertTrue(
        exception.getMessage().contains(expectedMessage),
        () -> "Expected '" + expectedMessage + "' in: " + exception.getMessage());
  }
}
