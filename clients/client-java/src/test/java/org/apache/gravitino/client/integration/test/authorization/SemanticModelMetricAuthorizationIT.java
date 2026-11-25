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
package org.apache.gravitino.client.integration.test.authorization;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.DialectExpression;
import org.apache.gravitino.semantic.Expression;
import org.apache.gravitino.semantic.Metric;
import org.apache.gravitino.semantic.SemanticModelCatalog;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.semantic.SemanticModelMetric;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Exercises metric discovery with real schema and containing-model authorization. */
public class SemanticModelMetricAuthorizationIT extends BaseRestApiAuthorizationIT {
  private static final String CATALOG = "metric_discovery_catalog";
  private static final String SCHEMA = "metric_discovery_schema";
  private static final String ROLE = "metric_reader";
  private static final Namespace NAMESPACE = Namespace.of(SCHEMA);

  private GravitinoMetalake metalake;
  private SemanticModelCatalog adminModels;
  private SemanticModelCatalog readerModels;

  /** Creates models and a reader with catalog access but no schema or model privileges. */
  @BeforeAll
  public void setUpMetricDiscovery() {
    metalake = client.loadMetalake(METALAKE);
    String database = GravitinoITUtils.genRandomName("metric_authorization");
    Catalog catalog =
        metalake.createCatalog(
            CATALOG,
            Catalog.Type.RELATIONAL,
            "lakehouse-iceberg",
            null,
            Map.of(
                "catalog-backend", "jdbc",
                "warehouse", System.getProperty("java.io.tmpdir") + "/" + database,
                "uri", "jdbc:h2:mem:" + database + ";DB_CLOSE_DELAY=-1;MODE=MYSQL",
                "jdbc-driver", "org.h2.Driver",
                "jdbc-initialize", "true"));
    catalog.asSchemas().createSchema(SCHEMA, null, Map.of());
    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(SCHEMA, "orders"),
            new Column[] {Column.of("amount", Types.LongType.get())},
            null,
            Map.of());
    adminModels = catalog.asSemanticModelCatalog();
    SemanticModelDefinition definition =
        SemanticModelDefinition.builder()
            .withDatasets(
                new Dataset[] {
                  Dataset.builder()
                      .withName("orders")
                      .withSource(NameIdentifier.of(CATALOG, SCHEMA, "orders"))
                      .build()
                })
            .withMetrics(new Metric[] {metric("count"), metric("revenue")})
            .build();
    for (String model : List.of("hidden", "modify", "owned", "use")) {
      adminModels.createSemanticModel(NameIdentifier.of(SCHEMA, model), null, definition, Map.of());
    }
    metalake.createRole(
        ROLE,
        Map.of(),
        List.of(SecurableObjects.ofCatalog(CATALOG, List.of(Privileges.UseCatalog.allow()))));
    metalake.grantRolesToUser(List.of(ROLE), NORMAL_USER);
    readerModels =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG).asSemanticModelCatalog();
  }

  @Test
  void testDiscoveryUsesSchemaAccessAndWholeModelVisibility() {
    assertVisibleModels(adminModels, "hidden", "modify", "owned", "use");
    assertSchemaDenied();
    MetadataObject schema = MetadataObjects.of(CATALOG, SCHEMA, MetadataObject.Type.SCHEMA);
    metalake.grantPrivilegesToRole(ROLE, schema, List.of(Privileges.UseSchema.allow()));
    assertVisibleModels(readerModels);

    metalake.grantPrivilegesToRole(
        ROLE, modelObject("use"), List.of(Privileges.UseSemanticModel.allow()));
    metalake.grantPrivilegesToRole(
        ROLE, modelObject("modify"), List.of(Privileges.ModifySemanticModel.allow()));
    metalake.setOwner(modelObject("owned"), NORMAL_USER, Owner.Type.USER);
    assertVisibleModels(readerModels, "modify", "owned", "use");

    metalake.revokePrivilegesFromRole(
        ROLE, modelObject("use"), Set.of(Privileges.UseSemanticModel.allow()));
    assertVisibleModels(readerModels, "modify", "owned");
    metalake.revokePrivilegesFromRole(
        ROLE, modelObject("modify"), Set.of(Privileges.ModifySemanticModel.allow()));
    assertVisibleModels(readerModels, "owned");
    metalake.setOwner(modelObject("owned"), USER, Owner.Type.USER);
    assertVisibleModels(readerModels);
    metalake.revokePrivilegesFromRole(ROLE, schema, Set.of(Privileges.UseSchema.allow()));
    assertSchemaDenied();
  }

  private void assertSchemaDenied() {
    assertThrows(ForbiddenException.class, () -> readerModels.listMetrics(NAMESPACE));
    assertThrows(ForbiddenException.class, () -> readerModels.listMetricInfos(NAMESPACE));
  }

  private static void assertVisibleModels(SemanticModelCatalog catalog, String... models) {
    NameIdentifier[] expected =
        Arrays.stream(models)
            .flatMap(
                model ->
                    Arrays.stream(new String[] {"count", "revenue"})
                        .map(name -> NameIdentifier.of(SCHEMA, model, name)))
            .toArray(NameIdentifier[]::new);
    assertArrayEquals(expected, catalog.listMetrics(NAMESPACE));
    SemanticModelMetric[] infos = catalog.listMetricInfos(NAMESPACE);
    assertEquals(expected.length, infos.length);
    for (int i = 0; i < infos.length; i++) {
      assertEquals(expected[i], NameIdentifier.of(infos[i].namespace(), infos[i].metric().name()));
      assertEquals(metric(expected[i].name()), infos[i].metric());
    }
  }

  private static MetadataObject modelObject(String model) {
    return MetadataObjects.of(List.of(CATALOG, SCHEMA, model), MetadataObject.Type.SEMANTIC_MODEL);
  }

  private static Metric metric(String name) {
    return Metric.builder()
        .withName(name)
        .withExpression(
            Expression.builder()
                .withDialects(
                    new DialectExpression[] {
                      DialectExpression.builder()
                          .withDialect("ANSI_SQL")
                          .withExpression(
                              "count".equals(name) ? "COUNT(orders.amount)" : "SUM(orders.amount)")
                          .build()
                    })
                .build())
        .build();
  }
}
