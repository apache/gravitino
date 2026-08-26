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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.IllegalSemanticModelException;
import org.apache.gravitino.exceptions.NoSuchSemanticModelException;
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
import org.apache.gravitino.semantic.Relationship;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelCatalog;
import org.apache.gravitino.semantic.SemanticModelChange;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Exercises the Semantic Model Java client against a real embedded Gravitino server. */
public class SemanticModelIT extends BaseIT {

  private static final String METALAKE_NAME =
      GravitinoITUtils.genRandomName("semantic_model_it_metalake");
  private static final String CATALOG_NAME = "semantic_model_it_catalog";
  private static final String SCHEMA_NAME = "semantic_model_it_schema";
  private static final String ORDERS_TABLE = "orders";
  private static final String CUSTOMERS_TABLE = "customers";
  private static final String MODEL_NAME = "sales_model";
  private static final String RENAMED_MODEL_NAME = "certified_sales_model";

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
    assertSemanticModel(MODEL_NAME, "Governed sales metrics", definition, properties, created);
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
    assertSemanticModel(MODEL_NAME, "Governed sales metrics", definition, properties, loaded);

    SemanticModelDefinition replacement = replacementDefinition();
    SemanticModel altered =
        semanticModelCatalog.alterSemanticModel(
            modelIdent,
            SemanticModelChange.rename(RENAMED_MODEL_NAME),
            SemanticModelChange.updateComment(""),
            SemanticModelChange.setProperty("owner", "analytics"),
            SemanticModelChange.removeProperty("deprecated"),
            SemanticModelChange.replaceDefinition(replacement));
    Map<String, String> alteredProperties = Map.of("certified", "true", "owner", "analytics");
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
  public void testCreateSemanticModelRejectsMissingSource() {
    NameIdentifier ident = NameIdentifier.of(SCHEMA_NAME, "missing_source_model");
    Dataset missingSource =
        Dataset.builder()
            .withName("missing_source")
            .withSource(NameIdentifier.of(CATALOG_NAME, SCHEMA_NAME, "missing_table"))
            .build();
    SemanticModelDefinition definition =
        SemanticModelDefinition.builder().withDatasets(new Dataset[] {missingSource}).build();

    assertThrows(
        IllegalSemanticModelException.class,
        () ->
            semanticModelCatalog.createSemanticModel(
                ident, null, definition, Collections.emptyMap()));
    assertThrows(
        NoSuchSemanticModelException.class, () -> semanticModelCatalog.loadSemanticModel(ident));
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
