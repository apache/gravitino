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
package org.apache.gravitino.client.integration.test;

import static org.apache.gravitino.semantic.CustomExtension.GRAVITINO_PROPERTIES_VENDOR;
import static org.apache.gravitino.semantic.SemanticModel.DEFAULT_OSSIE_VERSION;
import static org.apache.gravitino.semantic.SemanticModel.PROPERTY_OSSIE_VERSION;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.IllegalSemanticModelException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchSemanticModelException;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.exceptions.SemanticModelAlreadyExistsException;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.types.Types;
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
import org.apache.gravitino.semantic.OssieDocument;
import org.apache.gravitino.semantic.OssieFormat;
import org.apache.gravitino.semantic.Relationship;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelCatalog;
import org.apache.gravitino.semantic.SemanticModelChange;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.tag.SupportsTags;
import org.apache.gravitino.tag.Tag;
import org.apache.gravitino.tag.TagValue;
import org.apache.gravitino.tag.TagValueConstraint;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Exercises the Semantic Model Java client against a real embedded Gravitino server. */
public class SemanticModelIT extends BaseIT {

  private static final ObjectMapper JSON_MAPPER =
      new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
  private static final ObjectMapper YAML_MAPPER =
      new ObjectMapper(new YAMLFactory()).enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
  private static final String METALAKE_NAME =
      GravitinoITUtils.genRandomName("semantic_model_it_metalake");
  private static final String CATALOG_NAME = "semantic_model_it_catalog";
  private static final String SCHEMA_NAME = "semantic_model_it_schema";
  private static final String ORDERS_TABLE = "orders";
  private static final String CUSTOMERS_TABLE = "customers";
  private static final String MODEL_NAME = "sales_model";
  private static final String RENAMED_MODEL_NAME = "certified_sales_model";
  private static final String OSSIE_MODEL_NAME = "ossie_sales_model";
  private static final String INVALID_MODEL_NAME = "invalid_source_model";

  private final HttpClient httpClient = HttpClient.newHttpClient();

  private GravitinoMetalake metalake;
  private Catalog catalog;
  private SemanticModelCatalog semanticModelCatalog;

  @BeforeAll
  public void setUp() {
    metalake = client.createMetalake(METALAKE_NAME, "metalake comment", Collections.emptyMap());

    Map<String, String> catalogProperties = new LinkedHashMap<>();
    catalogProperties.put("catalog-backend", "jdbc");
    catalogProperties.put("warehouse", System.getProperty("java.io.tmpdir") + "/" + METALAKE_NAME);
    catalogProperties.put("uri", "jdbc:h2:mem:" + METALAKE_NAME + ";DB_CLOSE_DELAY=-1;MODE=MYSQL");
    catalogProperties.put("jdbc-driver", "org.h2.Driver");
    catalogProperties.put("jdbc-initialize", "true");
    catalog =
        metalake.createCatalog(
            CATALOG_NAME,
            Catalog.Type.RELATIONAL,
            "lakehouse-iceberg",
            "catalog comment",
            catalogProperties);
    catalog.asSchemas().createSchema(SCHEMA_NAME, "schema comment", Collections.emptyMap());

    TableCatalog tables = catalog.asTableCatalog();
    tables.createTable(
        NameIdentifier.of(SCHEMA_NAME, ORDERS_TABLE),
        new Column[] {
          Column.of("order_id", Types.LongType.get()),
          Column.of("customer_id", Types.LongType.get()),
          Column.of("order_time", Types.StringType.get()),
          Column.of("order_amount", Types.StringType.get())
        },
        "orders source",
        Collections.emptyMap());
    tables.createTable(
        NameIdentifier.of(SCHEMA_NAME, CUSTOMERS_TABLE),
        new Column[] {
          Column.of("id", Types.LongType.get()), Column.of("email", Types.StringType.get())
        },
        "customers source",
        Collections.emptyMap());

    semanticModelCatalog = catalog.asSemanticModelCatalog();
  }

  /** Removes models and tags left by a test so failures do not affect subsequent tests. */
  @AfterEach
  public void cleanUpSemanticModels() {
    for (NameIdentifier ident :
        semanticModelCatalog.listSemanticModels(Namespace.of(SCHEMA_NAME))) {
      semanticModelCatalog.dropSemanticModel(ident);
    }
    for (String tag : metalake.listTags()) {
      metalake.deleteTag(tag);
    }
  }

  @AfterAll
  public void tearDown() {
    if (metalake != null) {
      metalake.dropCatalog(CATALOG_NAME, true);
      client.dropMetalake(METALAKE_NAME, true);
    }
  }

  @Test
  public void testSemanticModelLifecycleRoundTrip() {
    NameIdentifier modelIdent = NameIdentifier.of(SCHEMA_NAME, MODEL_NAME);
    SemanticModelDefinition definition = initialDefinition();
    Map<String, String> properties =
        new LinkedHashMap<>(Map.of("certified", "true", "deprecated", "true"));

    assertArrayEquals(
        new NameIdentifier[0], semanticModelCatalog.listSemanticModels(Namespace.of(SCHEMA_NAME)));

    SemanticModel created =
        semanticModelCatalog.createSemanticModel(
            modelIdent, "Governed sales metrics", definition, properties);
    Map<String, String> expectedProperties = new LinkedHashMap<>(properties);
    expectedProperties.put(PROPERTY_OSSIE_VERSION, DEFAULT_OSSIE_VERSION);
    assertSemanticModel(
        MODEL_NAME, "Governed sales metrics", definition, expectedProperties, created);
    assertNotNull(created.auditInfo());
    assertNotNull(created.auditInfo().creator());
    assertNotNull(created.auditInfo().createTime());
    assertEquals("Use certified metrics", created.definition().aiContext().object().instructions());
    assertEquals(
        new BigDecimal("0.95"),
        created.definition().aiContext().object().additionalProperties().get("confidence"));
    assertEquals(DataType.DATE_TIME_TZ, created.definition().datasets()[0].fields()[0].datatype());
    assertEquals(
        "TRINO",
        created.definition().datasets()[0].fields()[1].expression().dialects()[0].dialect());
    assertArrayEquals(new Field[0], created.definition().datasets()[1].fields());

    assertThrows(
        SemanticModelAlreadyExistsException.class,
        () ->
            semanticModelCatalog.createSemanticModel(
                modelIdent, "duplicate", definition, Collections.emptyMap()));
    assertArrayEquals(
        new NameIdentifier[] {modelIdent},
        semanticModelCatalog.listSemanticModels(Namespace.of(SCHEMA_NAME)));

    SemanticModel loaded = semanticModelCatalog.loadSemanticModel(modelIdent);
    assertSemanticModel(
        MODEL_NAME, "Governed sales metrics", definition, expectedProperties, loaded);

    SemanticModelDefinition replacement = replacementDefinition();
    SemanticModel altered =
        semanticModelCatalog.alterSemanticModel(
            modelIdent,
            SemanticModelChange.rename(RENAMED_MODEL_NAME),
            SemanticModelChange.updateComment(""),
            SemanticModelChange.setProperty("owner", "analytics"),
            SemanticModelChange.removeProperty("deprecated"),
            SemanticModelChange.replaceDefinition(replacement));
    Map<String, String> alteredProperties =
        Map.of(
            "certified",
            "true",
            "owner",
            "analytics",
            PROPERTY_OSSIE_VERSION,
            DEFAULT_OSSIE_VERSION);
    assertSemanticModel(RENAMED_MODEL_NAME, "", replacement, alteredProperties, altered);
    assertNull(altered.definition().datasets()[0].fields());
    assertArrayEquals(new Field[0], altered.definition().datasets()[1].fields());
    assertArrayEquals(new Relationship[0], altered.definition().relationships());
    assertArrayEquals(new Metric[0], altered.definition().metrics());
    assertArrayEquals(new CustomExtension[0], altered.definition().customExtensions());

    assertThrows(
        NoSuchSemanticModelException.class,
        () -> semanticModelCatalog.loadSemanticModel(modelIdent));
    NameIdentifier renamedIdent = NameIdentifier.of(SCHEMA_NAME, RENAMED_MODEL_NAME);
    SemanticModel reloaded = semanticModelCatalog.loadSemanticModel(renamedIdent);
    assertSemanticModel(RENAMED_MODEL_NAME, "", replacement, alteredProperties, reloaded);

    assertTrue(semanticModelCatalog.dropSemanticModel(renamedIdent));
    assertFalse(semanticModelCatalog.dropSemanticModel(renamedIdent));
    assertArrayEquals(
        new NameIdentifier[0], semanticModelCatalog.listSemanticModels(Namespace.of(SCHEMA_NAME)));
    assertThrows(
        NoSuchSemanticModelException.class,
        () -> semanticModelCatalog.loadSemanticModel(renamedIdent));
  }

  @Test
  public void testCreateSemanticModelRejectsInvalidSourceIdentifier() {
    NameIdentifier ident = NameIdentifier.of(SCHEMA_NAME, INVALID_MODEL_NAME);
    Dataset invalidSource =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of(SCHEMA_NAME, ORDERS_TABLE))
            .build();
    SemanticModelDefinition definition =
        SemanticModelDefinition.builder().withDatasets(new Dataset[] {invalidSource}).build();

    IllegalSemanticModelException exception =
        assertThrows(
            IllegalSemanticModelException.class,
            () ->
                semanticModelCatalog.createSemanticModel(
                    ident, null, definition, Collections.emptyMap()));
    assertTrue(exception.getMessage().contains("Source must contain catalog.schema.name"));
    assertThrows(
        NoSuchSemanticModelException.class, () -> semanticModelCatalog.loadSemanticModel(ident));
  }

  /**
   * Verifies that a rejected create leaves no model behind.
   *
   * @param scenario the invalid-source scenario
   * @param definition the invalid definition
   * @param expectedMessage the server validation message
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("invalidSourceDefinitions")
  public void testCreateSemanticModelRejectsInvalidSource(
      String scenario, SemanticModelDefinition definition, String expectedMessage) {
    NameIdentifier ident = NameIdentifier.of(SCHEMA_NAME, INVALID_MODEL_NAME);

    IllegalSemanticModelException exception =
        assertThrows(
            IllegalSemanticModelException.class,
            () ->
                semanticModelCatalog.createSemanticModel(
                    ident, null, definition, Collections.emptyMap()));
    assertTrue(exception.getMessage().contains(expectedMessage), scenario);
    assertFalse(semanticModelCatalog.semanticModelExists(ident));
  }

  /**
   * Verifies that an invalid replacement prevents every change in the request from being persisted.
   *
   * @param scenario the invalid-source scenario
   * @param definition the invalid definition
   * @param expectedMessage the server validation message
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("invalidSourceDefinitions")
  public void testReplaceDefinitionRejectsInvalidSourceAtomically(
      String scenario, SemanticModelDefinition definition, String expectedMessage) {
    NameIdentifier ident = NameIdentifier.of(SCHEMA_NAME, MODEL_NAME);
    SemanticModel created =
        semanticModelCatalog.createSemanticModel(
            ident, "Original model", initialDefinition(), Map.of("owner", "analytics"));

    // Every replacement must be validated, even if a later replacement is valid.
    IllegalSemanticModelException exception =
        assertThrows(
            IllegalSemanticModelException.class,
            () ->
                semanticModelCatalog.alterSemanticModel(
                    ident,
                    SemanticModelChange.updateComment("Must not be persisted"),
                    SemanticModelChange.setProperty("owner", "another-team"),
                    SemanticModelChange.replaceDefinition(definition),
                    SemanticModelChange.replaceDefinition(replacementDefinition())));
    assertTrue(exception.getMessage().contains(expectedMessage), scenario);
    assertSemanticModel(
        created.name(),
        created.comment(),
        created.definition(),
        created.properties(),
        semanticModelCatalog.loadSemanticModel(ident));
  }

  /**
   * Verifies that YAML and JSON imports propagate source validation failures without persisting.
   *
   * @param scenario the invalid-source scenario and document format
   * @param document the document with an invalid source
   * @param expectedMessage the server validation message
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("invalidOssieSourceDocuments")
  public void testOssieImportRejectsInvalidSource(
      String scenario, OssieDocument document, String expectedMessage) {
    IllegalSemanticModelException exception =
        assertThrows(
            IllegalSemanticModelException.class,
            () ->
                semanticModelCatalog.importOssieSemanticModel(Namespace.of(SCHEMA_NAME), document));
    assertTrue(exception.getMessage().contains(expectedMessage), scenario);
    assertFalse(
        semanticModelCatalog.semanticModelExists(
            NameIdentifier.of(SCHEMA_NAME, INVALID_MODEL_NAME)));
  }

  /**
   * Verifies full native definitions and properties survive export, import, and cross-format
   * export.
   *
   * @param format the initial export format
   * @throws Exception if a document cannot be parsed
   */
  @ParameterizedTest
  @EnumSource(OssieFormat.class)
  public void testOssieImportExportRoundTrip(OssieFormat format) throws Exception {
    NameIdentifier ident = NameIdentifier.of(SCHEMA_NAME, OSSIE_MODEL_NAME);
    Map<String, String> properties =
        Map.of(
            "owner",
            "analytics",
            "label",
            "Revenue — 收入",
            "empty",
            "",
            PROPERTY_OSSIE_VERSION,
            "future-version");
    SemanticModel original =
        semanticModelCatalog.createSemanticModel(
            ident, "Revenue — 收入\nCertified metrics", initialDefinition(), properties);
    OssieDocument exported = semanticModelCatalog.exportOssieSemanticModel(ident, format);
    assertEquals(format, exported.format());
    JsonNode root = parseOssieDocument(exported);
    assertEquals(OSSIE_MODEL_NAME, root.path("name").textValue());
    assertEquals(original.comment(), root.path("description").textValue());
    assertFalse(root.has("semanticModel"));
    assertFalse(root.has("definition"));
    assertFalse(root.has("properties"));
    assertFalse(root.has("audit"));
    assertEquals("order_id", root.at("/datasets/0/primary_key/0").textValue());
    assertTrue(root.at("/datasets/0/unique_keys").isEmpty());
    assertEquals("email", root.at("/datasets/1/unique_keys/0/0").textValue());
    assertEquals("DateTimeTz", root.at("/datasets/0/fields/0/datatype").textValue());
    assertTrue(root.at("/datasets/0/fields/0/dimension/is_time").booleanValue());
    assertEquals(
        "TRINO", root.at("/datasets/0/fields/1/expression/dialects/0/dialect").textValue());
    assertEquals("customer_id", root.at("/relationships/0/from_columns/0").textValue());
    assertEquals("id", root.at("/relationships/0/to_columns/0").textValue());
    assertEquals("Decimal", root.at("/metrics/0/datatype").textValue());
    assertEquals(new BigDecimal("0.95"), root.at("/ai_context/confidence").decimalValue());
    assertOssieProperties(root, properties);
    assertTrue(semanticModelCatalog.dropSemanticModel(ident));
    SemanticModel reimported =
        semanticModelCatalog.importOssieSemanticModel(Namespace.of(SCHEMA_NAME), exported);
    assertSemanticModel(
        original.name(), original.comment(), original.definition(), properties, reimported);
    assertSemanticModel(
        original.name(),
        original.comment(),
        original.definition(),
        properties,
        semanticModelCatalog.loadSemanticModel(ident));
    OssieFormat otherFormat = format == OssieFormat.JSON ? OssieFormat.YAML : OssieFormat.JSON;
    OssieDocument other = semanticModelCatalog.exportOssieSemanticModel(ident, otherFormat);
    assertEquals(otherFormat, other.format());
    assertEquals(root, parseOssieDocument(other));
  }

  /**
   * Imports an independently constructed document without using the production converter.
   *
   * @param format the import format
   * @throws Exception if a document cannot be serialized or parsed
   */
  @ParameterizedTest
  @EnumSource(OssieFormat.class)
  public void testOssieStandaloneDocumentImport(OssieFormat format) throws Exception {
    ObjectNode document = ossieModelDocument();
    Map<String, String> properties =
        Map.of(
            "owner",
            "analytics",
            "label",
            "Revenue — 收入",
            "empty",
            "",
            PROPERTY_OSSIE_VERSION,
            "future-version");
    document.put("version", "future-version");
    document.put("description", "Revenue — 收入\nCertified metrics");
    document
        .putObject("ai_context")
        .put("instructions", "Use certified metrics")
        .put("confidence", new BigDecimal("0.1234567890123456789"))
        .putArray("hints")
        .addNull()
        .add("month");
    ObjectNode dataset = (ObjectNode) document.path("datasets").get(0);
    dataset.put("source", "`" + CATALOG_NAME + "`.`" + SCHEMA_NAME + "`.`" + ORDERS_TABLE + "`");
    dataset.putArray("primary_key").add("order_id");
    dataset
        .putArray("custom_extensions")
        .addObject()
        .put("vendor_name", GRAVITINO_PROPERTIES_VENDOR)
        .put("data", "opaque dataset payload");
    document
        .putArray("custom_extensions")
        .addObject()
        .put("vendor_name", "GRAVITINO")
        .put("data", "opaque payload, not JSON");
    addPropertiesExtension(document, JSON_MAPPER.writeValueAsString(properties));

    SemanticModel imported =
        semanticModelCatalog.importOssieSemanticModel(
            Namespace.of(SCHEMA_NAME), ossieDocument(document, format));
    assertEquals(properties, imported.properties());
    assertEquals(document.path("description").textValue(), imported.comment());
    assertEquals(
        NameIdentifier.of(CATALOG_NAME, SCHEMA_NAME, ORDERS_TABLE),
        imported.definition().datasets()[0].source());
    assertEquals(
        new BigDecimal("0.1234567890123456789"),
        imported.definition().aiContext().object().additionalProperties().get("confidence"));
    assertArrayEquals(
        new CustomExtension[] {
          CustomExtension.builder()
              .withVendorName("GRAVITINO")
              .withData("opaque payload, not JSON")
              .build()
        },
        imported.definition().customExtensions());
    assertEquals(
        "opaque dataset payload", imported.definition().datasets()[0].customExtensions()[0].data());

    NameIdentifier ident = NameIdentifier.of(SCHEMA_NAME, imported.name());
    OssieDocument exported = semanticModelCatalog.exportOssieSemanticModel(ident, format);
    JsonNode root = parseOssieDocument(exported);
    assertOssieProperties(root, properties);
    assertEquals("opaque payload, not JSON", root.at("/custom_extensions/0/data").textValue());
    assertEquals(
        GRAVITINO_PROPERTIES_VENDOR,
        root.at("/datasets/0/custom_extensions/0/vendor_name").textValue());
    assertTrue(semanticModelCatalog.dropSemanticModel(ident));
    assertSemanticModel(
        imported.name(),
        imported.comment(),
        imported.definition(),
        properties,
        semanticModelCatalog.importOssieSemanticModel(Namespace.of(SCHEMA_NAME), exported));
  }

  /**
   * Verifies that importing an existing name never overwrites the stored model.
   *
   * @param format the import format
   * @throws Exception if a document cannot be serialized
   */
  @ParameterizedTest
  @EnumSource(OssieFormat.class)
  public void testOssieImportIsCreateOnly(OssieFormat format) throws Exception {
    NameIdentifier ident = NameIdentifier.of(SCHEMA_NAME, OSSIE_MODEL_NAME);
    SemanticModel original =
        semanticModelCatalog.createSemanticModel(
            ident, "Original model", initialDefinition(), Map.of("owner", "analytics"));
    ObjectNode document = ossieModelDocument();
    document.put("description", "Must not replace the original model");
    addPropertiesExtension(document, "{\"owner\":\"another-team\"}");
    OssieDocument replacement = ossieDocument(document, format);
    assertThrows(
        SemanticModelAlreadyExistsException.class,
        () ->
            semanticModelCatalog.importOssieSemanticModel(Namespace.of(SCHEMA_NAME), replacement));
    assertSemanticModel(
        original.name(),
        original.comment(),
        original.definition(),
        original.properties(),
        semanticModelCatalog.loadSemanticModel(ident));
    assertArrayEquals(
        new NameIdentifier[] {ident},
        semanticModelCatalog.listSemanticModels(Namespace.of(SCHEMA_NAME)));
  }

  /**
   * Verifies typed missing-model and missing-schema errors through the public interchange API.
   *
   * @param format the document format
   * @throws Exception if a document cannot be serialized
   */
  @ParameterizedTest
  @EnumSource(OssieFormat.class)
  public void testOssieMissingModelAndSchemaErrors(OssieFormat format) throws Exception {
    assertThrows(
        NoSuchSemanticModelException.class,
        () ->
            semanticModelCatalog.exportOssieSemanticModel(
                NameIdentifier.of(SCHEMA_NAME, "missing_model"), format));
    OssieDocument document = ossieDocument(ossieModelDocument(), format);
    assertThrows(
        NoSuchSchemaException.class,
        () ->
            semanticModelCatalog.importOssieSemanticModel(
                Namespace.of("missing_schema"), document));
    assertArrayEquals(
        new NameIdentifier[0], semanticModelCatalog.listSemanticModels(Namespace.of(SCHEMA_NAME)));
  }

  /**
   * Verifies malformed and unsupported documents are rejected without persisting any model.
   *
   * @param scenario the invalid document scenario
   * @param document the document to reject
   * @param expectedMessage the expected validation diagnostic
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("invalidOssieDocuments")
  public void testOssieImportRejectsInvalidDocument(
      String scenario, OssieDocument document, String expectedMessage) {
    IllegalSemanticModelException error =
        assertThrows(
            IllegalSemanticModelException.class,
            () ->
                semanticModelCatalog.importOssieSemanticModel(Namespace.of(SCHEMA_NAME), document));
    assertTrue(error.getMessage().contains(expectedMessage), scenario + ": " + error.getMessage());
    assertArrayEquals(
        new NameIdentifier[0], semanticModelCatalog.listSemanticModels(Namespace.of(SCHEMA_NAME)));
  }

  /**
   * Verifies native create and replacement cannot inject the reserved root properties extension.
   */
  @Test
  public void testReservedOssiePropertiesExtensionIsRejectedAtomically() {
    NameIdentifier ident = NameIdentifier.of(SCHEMA_NAME, OSSIE_MODEL_NAME);
    SemanticModelDefinition reserved =
        SemanticModelDefinition.builder()
            .withDatasets(initialDefinition().datasets())
            .withCustomExtensions(
                new CustomExtension[] {
                  CustomExtension.builder()
                      .withVendorName(GRAVITINO_PROPERTIES_VENDOR)
                      .withData("{\"owner\":\"another-team\"}")
                      .build()
                })
            .build();
    assertThrows(
        IllegalSemanticModelException.class,
        () -> semanticModelCatalog.createSemanticModel(ident, null, reserved, Map.of()));
    assertFalse(semanticModelCatalog.semanticModelExists(ident));
    SemanticModel original =
        semanticModelCatalog.createSemanticModel(
            ident, "Original model", initialDefinition(), Map.of("owner", "analytics"));
    assertThrows(
        IllegalSemanticModelException.class,
        () ->
            semanticModelCatalog.alterSemanticModel(
                ident,
                SemanticModelChange.updateComment("Must not persist"),
                SemanticModelChange.setProperty("owner", "another-team"),
                SemanticModelChange.replaceDefinition(reserved)));
    assertSemanticModel(
        original.name(),
        original.comment(),
        original.definition(),
        original.properties(),
        semanticModelCatalog.loadSemanticModel(ident));
  }

  /**
   * Verifies version property validation, ordered changes, and the export fallback after removal.
   *
   * @throws Exception if an exported document cannot be parsed
   */
  @Test
  public void testOssieVersionPropertyLifecycle() throws Exception {
    NameIdentifier ident = NameIdentifier.of(SCHEMA_NAME, OSSIE_MODEL_NAME);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            semanticModelCatalog.createSemanticModel(
                ident, null, initialDefinition(), Map.of(PROPERTY_OSSIE_VERSION, " ")));
    assertFalse(semanticModelCatalog.semanticModelExists(ident));
    SemanticModel original =
        semanticModelCatalog.createSemanticModel(
            ident, "Original model", initialDefinition(), Map.of());
    assertEquals(DEFAULT_OSSIE_VERSION, original.properties().get(PROPERTY_OSSIE_VERSION));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            semanticModelCatalog.alterSemanticModel(
                ident,
                SemanticModelChange.updateComment("Must not persist"),
                SemanticModelChange.setProperty(PROPERTY_OSSIE_VERSION, " ")));
    assertSemanticModel(
        original.name(),
        original.comment(),
        original.definition(),
        original.properties(),
        semanticModelCatalog.loadSemanticModel(ident));

    SemanticModel removed =
        semanticModelCatalog.alterSemanticModel(
            ident,
            SemanticModelChange.setProperty(PROPERTY_OSSIE_VERSION, "future-version"),
            SemanticModelChange.removeProperty(PROPERTY_OSSIE_VERSION));
    assertFalse(removed.properties().containsKey(PROPERTY_OSSIE_VERSION));
    assertOssieProperties(
        parseOssieDocument(semanticModelCatalog.exportOssieSemanticModel(ident, OssieFormat.JSON)),
        Map.of());
    SemanticModel updated =
        semanticModelCatalog.alterSemanticModel(
            ident,
            SemanticModelChange.removeProperty(PROPERTY_OSSIE_VERSION),
            SemanticModelChange.setProperty(PROPERTY_OSSIE_VERSION, "future-version"));
    assertEquals("future-version", updated.properties().get(PROPERTY_OSSIE_VERSION));
    assertOssieProperties(
        parseOssieDocument(semanticModelCatalog.exportOssieSemanticModel(ident, OssieFormat.YAML)),
        updated.properties());
  }

  /**
   * Verifies YAML aliases and the documented default format through actual HTTP routing.
   *
   * @param contentType the import media type, or null to omit the header
   * @throws Exception if the HTTP request fails
   */
  @ParameterizedTest
  @NullSource
  @ValueSource(
      strings = {
        "application/yaml",
        "application/x-yaml",
        "text/yaml",
        "application/yaml; charset=UTF-8"
      })
  public void testOssieImportMediaTypes(@Nullable String contentType) throws Exception {
    String yaml = ossieDocument(ossieModelDocument(), OssieFormat.YAML).content();
    HttpResponse<String> response = requestOssie("/ossie", contentType, yaml);
    assertEquals(200, response.statusCode(), response.body());
    assertEquals(
        OSSIE_MODEL_NAME,
        JSON_MAPPER.readTree(response.body()).at("/semanticModel/name").textValue());
    assertTrue(
        semanticModelCatalog.semanticModelExists(NameIdentifier.of(SCHEMA_NAME, OSSIE_MODEL_NAME)));
  }

  /**
   * Verifies unsupported media types are rejected before importing a valid document.
   *
   * @param contentType the unsupported import media type
   * @throws Exception if the HTTP request fails
   */
  @ParameterizedTest
  @ValueSource(strings = {"text/plain", "application/x-www-form-urlencoded"})
  public void testOssieImportRejectsUnsupportedMediaTypes(String contentType) throws Exception {
    HttpResponse<String> response =
        requestOssie(
            "/ossie", contentType, ossieDocument(ossieModelDocument(), OssieFormat.YAML).content());
    assertEquals(415, response.statusCode(), response.body());
    assertArrayEquals(
        new NameIdentifier[0], semanticModelCatalog.listSemanticModels(Namespace.of(SCHEMA_NAME)));
  }

  /**
   * Verifies raw export bodies, default format, and media type and download headers.
   *
   * @param format the format query parameter, or null to omit it
   * @throws Exception if the HTTP request or document parsing fails
   */
  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"yaml", "json"})
  public void testOssieExportHttpRepresentation(@Nullable String format) throws Exception {
    NameIdentifier ident = NameIdentifier.of(SCHEMA_NAME, OSSIE_MODEL_NAME);
    semanticModelCatalog.createSemanticModel(
        ident, "Exported model", initialDefinition(), Map.of());
    String query = format == null ? "" : "?format=" + format;
    HttpResponse<String> response =
        requestOssie("/" + OSSIE_MODEL_NAME + "/ossie" + query, null, null);
    assertEquals(200, response.statusCode(), response.body());
    String expectedFormat = format == null ? "yaml" : format;
    assertEquals(
        "application/" + expectedFormat,
        response.headers().firstValue("Content-Type").orElseThrow().split(";")[0]);
    assertEquals(
        "attachment; filename=\"" + OSSIE_MODEL_NAME + ".ossie." + expectedFormat + "\"",
        response.headers().firstValue("Content-Disposition").orElseThrow());
    JsonNode root =
        "json".equals(format)
            ? JSON_MAPPER.readTree(response.body())
            : YAML_MAPPER.readTree(response.body());
    assertEquals(OSSIE_MODEL_NAME, root.path("name").textValue());
    assertFalse(root.has("semanticModel"));
    assertOssieProperties(root, Map.of());
    HttpResponse<String> invalid =
        requestOssie("/" + OSSIE_MODEL_NAME + "/ossie?format=xml", null, null);
    assertEquals(400, invalid.statusCode(), invalid.body());
    assertTrue(
        JSON_MAPPER
            .readTree(invalid.body())
            .path("message")
            .textValue()
            .contains("Unsupported Ossie format"));
  }

  /** Verifies direct and inherited tags, detail retrieval, reverse lookup, and removal. */
  @Test
  public void testSemanticModelTagLifecycle() {
    NameIdentifier ident = NameIdentifier.of(SCHEMA_NAME, MODEL_NAME);
    SemanticModel model =
        semanticModelCatalog.createSemanticModel(
            ident, null, initialDefinition(), Collections.emptyMap());
    Tag catalogTag = createTag("catalog_tag");
    Tag schemaTag = createTag("schema_tag");
    Tag modelTag = createTag("model_tag");
    SupportsTags schemaTags = catalog.asSchemas().loadSchema(SCHEMA_NAME).supportsTags();

    assertArrayEquals(new String[0], model.supportsTags().listTags());
    assertArrayEquals(new Tag[0], model.supportsTags().listTagsInfo());
    catalog.supportsTags().associateTags(new String[] {catalogTag.name()}, null);
    schemaTags.associateTags(new String[] {schemaTag.name()}, null);
    assertArrayEquals(
        new String[] {modelTag.name()},
        model.supportsTags().associateTags(new String[] {modelTag.name()}, null));

    SupportsTags tags = semanticModelCatalog.loadSemanticModel(ident).supportsTags();
    assertEquals(
        Set.of(catalogTag.name(), schemaTag.name(), modelTag.name()), Set.of(tags.listTags()));
    assertEquals(
        Map.of(catalogTag.name(), true, schemaTag.name(), true, modelTag.name(), false),
        Arrays.stream(tags.listTagsInfo())
            .collect(Collectors.toMap(Tag::name, tag -> tag.inherited().orElseThrow())));
    assertEquals(catalogTag, tags.getTag(catalogTag.name()));
    assertEquals(schemaTag, tags.getTag(schemaTag.name()));
    assertEquals(modelTag, tags.getTag(modelTag.name()));
    assertTagAssignment(tags, catalogTag.name(), true);
    assertTagAssignment(tags, schemaTag.name(), true);
    assertTagAssignment(tags, modelTag.name(), false);
    assertMetadataObjects(modelTag.associatedObjects().objects(), semanticModelObject(MODEL_NAME));
    assertMetadataObjects(
        catalogTag.associatedObjects().objects(),
        MetadataObjects.of(null, CATALOG_NAME, MetadataObject.Type.CATALOG));
    assertMetadataObjects(
        schemaTag.associatedObjects().objects(),
        MetadataObjects.of(CATALOG_NAME, SCHEMA_NAME, MetadataObject.Type.SCHEMA));

    // Removing an inherited tag from the child must not detach it from its ancestor.
    tags.associateTags(null, new String[] {catalogTag.name()});
    assertTagAssignment(tags, catalogTag.name(), true);
    assertArrayEquals(new String[0], tags.associateTags(null, new String[] {modelTag.name()}));
    assertThrows(NoSuchTagException.class, () -> tags.getTag(modelTag.name()));
    assertArrayEquals(new MetadataObject[0], modelTag.associatedObjects().objects());
    assertEquals(Set.of(catalogTag.name(), schemaTag.name()), Set.of(tags.listTags()));

    schemaTags.associateTags(null, new String[] {schemaTag.name()});
    catalog.supportsTags().associateTags(null, new String[] {catalogTag.name()});
    assertArrayEquals(new String[0], tags.listTags());
    assertArrayEquals(new Tag[0], tags.listTagsInfo());
  }

  /** Verifies tag values, ancestor precedence, direct overrides, and value-filtered lookup. */
  @Test
  public void testSemanticModelTagValues() {
    SemanticModel model =
        semanticModelCatalog.createSemanticModel(
            NameIdentifier.of(SCHEMA_NAME, MODEL_NAME),
            null,
            initialDefinition(),
            Collections.emptyMap());
    Tag tag =
        metalake.createTag(
            "data_domain",
            null,
            Collections.emptyMap(),
            TagValueConstraint.ofAllowedValues("finance", "risk"));
    TagValue[] finance = {TagValue.of(tag.name(), "finance")};
    TagValue[] risk = {TagValue.of(tag.name(), "risk")};
    SupportsTags tags = model.supportsTags();
    SupportsTags schemaTags = catalog.asSchemas().loadSchema(SCHEMA_NAME).supportsTags();

    catalog.supportsTags().associateTags(finance, null);
    assertTagAssignment(tags, tag.name(), true, "finance");
    schemaTags.associateTags(risk, null);
    assertTagAssignment(tags, tag.name(), true, "risk");
    assertArrayEquals(new String[] {tag.name()}, tags.associateTags(finance, null));
    assertTagAssignment(tags, tag.name(), false, "finance");
    assertEquals(tag.valueConstraint(), tags.getTag(tag.name()).valueConstraint());
    tags.associateTags(risk, null);
    tags.associateTags(risk, null);
    assertTagAssignment(tags, tag.name(), false, "finance", "risk");
    assertMetadataObjects(
        tag.associatedObjects().objects("finance"),
        MetadataObjects.of(null, CATALOG_NAME, MetadataObject.Type.CATALOG),
        semanticModelObject(MODEL_NAME));
    assertArrayEquals(new MetadataObject[0], tag.associatedObjects().objects("unknown"));

    tags.associateTags(null, finance);
    assertTagAssignment(tags, tag.name(), false, "risk");
    assertArrayEquals(new String[0], tags.associateTags(null, risk));
    assertTagAssignment(tags, tag.name(), true, "risk");
    assertMetadataObjects(
        tag.associatedObjects().objects("risk"),
        MetadataObjects.of(CATALOG_NAME, SCHEMA_NAME, MetadataObject.Type.SCHEMA));
    schemaTags.associateTags(null, risk);
    assertTagAssignment(tags, tag.name(), true, "finance");
    catalog.supportsTags().associateTags(null, finance);
    assertArrayEquals(new String[0], tags.listTags());
    assertArrayEquals(new MetadataObject[0], tag.associatedObjects().objects());
  }

  /**
   * Verifies that tags follow a rename but cannot leak into a recreated model with the same name.
   */
  @Test
  public void testSemanticModelTagsAcrossRenameAndDrop() {
    NameIdentifier ident = NameIdentifier.of(SCHEMA_NAME, MODEL_NAME);
    SemanticModel model =
        semanticModelCatalog.createSemanticModel(
            ident, null, initialDefinition(), Collections.emptyMap());
    Tag tag = createTag("renamed_model_tag");
    model.supportsTags().associateTags(new String[] {tag.name()}, null);

    SemanticModel renamed =
        semanticModelCatalog.alterSemanticModel(
            ident, SemanticModelChange.rename(RENAMED_MODEL_NAME));
    assertArrayEquals(new String[] {tag.name()}, renamed.supportsTags().listTags());
    assertTagAssignment(renamed.supportsTags(), tag.name(), false);
    assertMetadataObjects(
        tag.associatedObjects().objects(), semanticModelObject(RENAMED_MODEL_NAME));
    NotFoundException missing =
        assertThrows(NotFoundException.class, () -> model.supportsTags().listTags());
    assertTrue(missing.getMessage().contains(semanticModelObject(MODEL_NAME).fullName()));

    NameIdentifier renamedIdent = NameIdentifier.of(SCHEMA_NAME, RENAMED_MODEL_NAME);
    assertTrue(semanticModelCatalog.dropSemanticModel(renamedIdent));
    SupportsTags droppedTags = renamed.supportsTags();
    assertThrows(NotFoundException.class, droppedTags::listTags);
    assertThrows(NotFoundException.class, droppedTags::listTagsInfo);
    assertThrows(NotFoundException.class, () -> droppedTags.getTag(tag.name()));
    assertThrows(
        NotFoundException.class, () -> droppedTags.associateTags(new String[] {tag.name()}, null));
    assertThrows(
        NotFoundException.class,
        () -> droppedTags.associateTags(new TagValue[] {TagValue.of(tag.name(), "value")}, null));
    assertArrayEquals(new MetadataObject[0], tag.associatedObjects().objects());

    SemanticModel recreated =
        semanticModelCatalog.createSemanticModel(
            renamedIdent, null, initialDefinition(), Collections.emptyMap());
    assertArrayEquals(new String[0], recreated.supportsTags().listTags());
    assertArrayEquals(new MetadataObject[0], tag.associatedObjects().objects());
  }

  /**
   * Verifies that imported models expose tag operations with the correct model identity.
   *
   * @param format the Ossie import format
   */
  @ParameterizedTest
  @EnumSource(OssieFormat.class)
  public void testImportedSemanticModelSupportsTags(OssieFormat format) {
    SemanticModel imported =
        semanticModelCatalog.importOssieSemanticModel(
            Namespace.of(SCHEMA_NAME), ossieSourceDocument(ORDERS_TABLE, null, format));
    Tag tag = createTag("imported_model_tag");
    assertArrayEquals(
        new String[] {tag.name()},
        imported.supportsTags().associateTags(new String[] {tag.name()}, null));
    assertTagAssignment(imported.supportsTags(), tag.name(), false);
    assertMetadataObjects(tag.associatedObjects().objects(), semanticModelObject(imported.name()));
    assertArrayEquals(
        new String[] {tag.name()},
        semanticModelCatalog
            .loadSemanticModel(NameIdentifier.of(SCHEMA_NAME, imported.name()))
            .supportsTags()
            .listTags());
  }

  private static ObjectNode ossieModelDocument() {
    ObjectNode root = JSON_MAPPER.createObjectNode();
    root.put("version", DEFAULT_OSSIE_VERSION);
    root.put("name", OSSIE_MODEL_NAME);
    root.putArray("datasets")
        .addObject()
        .put("name", "orders")
        .put("source", CATALOG_NAME + "." + SCHEMA_NAME + "." + ORDERS_TABLE);
    return root;
  }

  private static OssieDocument ossieDocument(JsonNode root, OssieFormat format)
      throws JsonProcessingException {
    return format == OssieFormat.JSON
        ? OssieDocument.json(JSON_MAPPER.writeValueAsString(root))
        : OssieDocument.yaml(YAML_MAPPER.writeValueAsString(root));
  }

  private static JsonNode parseOssieDocument(OssieDocument document)
      throws JsonProcessingException {
    return (document.format() == OssieFormat.JSON ? JSON_MAPPER : YAML_MAPPER)
        .readTree(document.content());
  }

  private static void addPropertiesExtension(ObjectNode root, String data) {
    root.withArray("custom_extensions")
        .addObject()
        .put("vendor_name", GRAVITINO_PROPERTIES_VENDOR)
        .put("data", data);
  }

  private static void assertOssieProperties(JsonNode root, Map<String, String> properties)
      throws JsonProcessingException {
    assertEquals(
        properties.getOrDefault(PROPERTY_OSSIE_VERSION, DEFAULT_OSSIE_VERSION),
        root.path("version").textValue());
    Map<String, String> expected = new LinkedHashMap<>(properties);
    expected.remove(PROPERTY_OSSIE_VERSION);
    int count = 0;
    for (JsonNode extension : root.path("custom_extensions")) {
      if (GRAVITINO_PROPERTIES_VENDOR.equals(extension.path("vendor_name").textValue())) {
        count++;
        assertTrue(extension.path("data").isTextual());
        JsonNode actual = JSON_MAPPER.readTree(extension.path("data").textValue());
        assertEquals(JSON_MAPPER.valueToTree(expected), actual);
        assertFalse(actual.has(PROPERTY_OSSIE_VERSION));
      }
    }
    assertEquals(expected.isEmpty() ? 0 : 1, count);
  }

  private HttpResponse<String> requestOssie(
      String suffix, @Nullable String contentType, @Nullable String body) throws Exception {
    HttpRequest.Builder request =
        HttpRequest.newBuilder()
            .uri(
                URI.create(
                    String.format(
                        "%s/api/metalakes/%s/catalogs/%s/schemas/%s/semantic-models%s",
                        serverUri, METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, suffix)))
            .timeout(Duration.ofSeconds(30));
    if (contentType != null) {
      request.header("Content-Type", contentType);
    }
    if (body == null) {
      request.GET();
    } else {
      request.POST(HttpRequest.BodyPublishers.ofString(body));
    }
    return httpClient.send(request.build(), HttpResponse.BodyHandlers.ofString());
  }

  private static Stream<Arguments> invalidOssieDocuments() throws JsonProcessingException {
    Stream.Builder<Arguments> cases = Stream.builder();
    for (OssieFormat format : OssieFormat.values()) {
      ObjectNode missingVersion = ossieModelDocument();
      missingVersion.remove("version");
      cases.add(
          Arguments.of(
              "missing version " + format, ossieDocument(missingVersion, format), "$.version"));
      ObjectNode wrongType = ossieModelDocument();
      wrongType.putObject("datasets");
      cases.add(
          Arguments.of(
              "wrong dataset type " + format, ossieDocument(wrongType, format), "$.datasets"));
      ObjectNode unknown = ossieModelDocument().put("unsupported", true);
      cases.add(
          Arguments.of("unknown field " + format, ossieDocument(unknown, format), "$.unsupported"));
      ObjectNode nullDescription = ossieModelDocument().putNull("description");
      cases.add(
          Arguments.of(
              "null description " + format,
              ossieDocument(nullDescription, format),
              "$.description"));
      ObjectNode conflict = ossieModelDocument();
      addPropertiesExtension(conflict, "{\"ossie-version\":\"different-version\"}");
      cases.add(
          Arguments.of(
              "conflicting version " + format,
              ossieDocument(conflict, format),
              "conflicts with $.version"));
      ObjectNode invalidProperties = ossieModelDocument();
      addPropertiesExtension(invalidProperties, "{\"owner\":1}");
      cases.add(
          Arguments.of(
              "non-string property " + format,
              ossieDocument(invalidProperties, format),
              "property 'owner' must be a string"));
      ObjectNode duplicateProperties = ossieModelDocument();
      addPropertiesExtension(duplicateProperties, "{}");
      addPropertiesExtension(duplicateProperties, "{}");
      cases.add(
          Arguments.of(
              "duplicate properties extension " + format,
              ossieDocument(duplicateProperties, format),
              "multiple GRAVITINO_PROPERTIES extensions"));
      ObjectNode duplicateProperty = ossieModelDocument();
      addPropertiesExtension(duplicateProperty, "{\"owner\":\"first\",\"owner\":\"second\"}");
      cases.add(
          Arguments.of(
              "duplicate property key " + format,
              ossieDocument(duplicateProperty, format),
              "Duplicate field 'owner'"));

      String content = ossieDocument(ossieModelDocument(), format).content();
      OssieDocument duplicate =
          format == OssieFormat.JSON
              ? OssieDocument.json("{\"name\":\"duplicate\"," + content.substring(1))
              : OssieDocument.yaml(content + "\nname: duplicate\n");
      cases.add(
          Arguments.of("duplicate root field " + format, duplicate, "Duplicate field 'name'"));
      OssieDocument trailing =
          format == OssieFormat.JSON
              ? OssieDocument.json(content + " {}")
              : OssieDocument.yaml(content + "\n---\n{}\n");
      cases.add(Arguments.of("trailing document " + format, trailing, "Trailing token"));
    }
    cases.add(
        Arguments.of(
            "YAML declared as JSON",
            OssieDocument.json(ossieDocument(ossieModelDocument(), OssieFormat.YAML).content()),
            "Cannot parse Apache Ossie JSON"));
    return cases.build();
  }

  private Tag createTag(String prefix) {
    return metalake.createTag(
        GravitinoITUtils.genRandomName(prefix), "Semantic Model tag", Collections.emptyMap());
  }

  private static MetadataObject semanticModelObject(String name) {
    return MetadataObjects.of(
        CATALOG_NAME + "." + SCHEMA_NAME, name, MetadataObject.Type.SEMANTIC_MODEL);
  }

  private static void assertTagAssignment(
      SupportsTags tags, String name, boolean inherited, String... values) {
    Tag tag = tags.getTag(name);
    assertEquals(inherited, tag.inherited().orElseThrow());
    assertEquals(Set.of(values), Set.of(tag.assignment().orElseThrow().values()));
  }

  private static void assertMetadataObjects(MetadataObject[] actual, MetadataObject... expected) {
    assertEquals(expected.length, actual.length);
    assertEquals(
        Set.of(expected),
        Arrays.stream(actual)
            .map(object -> MetadataObjects.parse(object.fullName(), object.type()))
            .collect(Collectors.toSet()));
  }

  private static Stream<Arguments> invalidSourceDefinitions() {
    Dataset missingSource =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of(CATALOG_NAME, SCHEMA_NAME, "missing_table"))
            .build();
    Dataset missingColumn =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of(CATALOG_NAME, SCHEMA_NAME, ORDERS_TABLE))
            .withPrimaryKey(new String[] {"missing_column"})
            .build();
    return Stream.of(
        Arguments.of(
            "missing source",
            SemanticModelDefinition.builder().withDatasets(new Dataset[] {missingSource}).build(),
            "missing_table does not exist"),
        Arguments.of(
            "missing column",
            SemanticModelDefinition.builder().withDatasets(new Dataset[] {missingColumn}).build(),
            "Dataset orders source has no column missing_column"));
  }

  private static Stream<Arguments> invalidOssieSourceDocuments() {
    return Stream.of(OssieFormat.values())
        .flatMap(
            format ->
                Stream.of(
                    Arguments.of(
                        "missing source " + format,
                        ossieSourceDocument("missing_table", null, format),
                        "missing_table does not exist"),
                    Arguments.of(
                        "missing column " + format,
                        ossieSourceDocument(ORDERS_TABLE, "missing_column", format),
                        "Dataset orders source has no column missing_column")));
  }

  private static OssieDocument ossieSourceDocument(
      String table, @Nullable String primaryKey, OssieFormat format) {
    String source = CATALOG_NAME + "." + SCHEMA_NAME + "." + table;
    if (format == OssieFormat.JSON) {
      return OssieDocument.json(
          String.format(
              "{\"version\":\"%s\",\"name\":\"%s\","
                  + "\"datasets\":[{\"name\":\"orders\",\"source\":\"%s\"%s}]}",
              DEFAULT_OSSIE_VERSION,
              INVALID_MODEL_NAME,
              source,
              primaryKey == null ? "" : ",\"primary_key\":[\"" + primaryKey + "\"]"));
    }
    return OssieDocument.yaml(
        String.format(
            "version: %s%nname: %s%ndatasets:%n  - name: orders%n    source: %s%n%s",
            DEFAULT_OSSIE_VERSION,
            INVALID_MODEL_NAME,
            source,
            primaryKey == null ? "" : "    primary_key: [" + primaryKey + "]\n"));
  }

  private static SemanticModelDefinition initialDefinition() {
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
            .withSource(NameIdentifier.of(CATALOG_NAME, SCHEMA_NAME, ORDERS_TABLE))
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
            .withSource(NameIdentifier.of(CATALOG_NAME, SCHEMA_NAME, CUSTOMERS_TABLE))
            .withUniqueKeys(new String[][] {{"email"}})
            .withFields(new Field[0])
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
        .withDatasets(new Dataset[] {orders, customers})
        .withRelationships(new Relationship[] {relationship})
        .withMetrics(new Metric[] {revenue})
        .withCustomExtensions(new CustomExtension[] {extension})
        .build();
  }

  private static SemanticModelDefinition replacementDefinition() {
    Dataset orders =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of(CATALOG_NAME, SCHEMA_NAME, ORDERS_TABLE))
            .withPrimaryKey(new String[] {"order_id"})
            .build();
    Dataset customers =
        Dataset.builder()
            .withName("customers")
            .withSource(NameIdentifier.of(CATALOG_NAME, SCHEMA_NAME, CUSTOMERS_TABLE))
            .withFields(new Field[0])
            .build();
    return SemanticModelDefinition.builder()
        .withAIContext(AIContext.of("Use the replacement definition"))
        .withDatasets(new Dataset[] {orders, customers})
        .withRelationships(new Relationship[0])
        .withMetrics(new Metric[0])
        .withCustomExtensions(new CustomExtension[0])
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

  private static void assertSemanticModel(
      String name,
      String comment,
      SemanticModelDefinition definition,
      Map<String, String> properties,
      SemanticModel actual) {
    assertEquals(name, actual.name());
    assertEquals(comment, actual.comment());
    assertEquals(definition, actual.definition());
    assertEquals(properties, actual.properties());
  }
}
