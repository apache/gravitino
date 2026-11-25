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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.semantic.SupportsSemanticModels;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Exercises source lookup with real source and containing-model authorization. */
public class SemanticModelSourceAuthorizationIT extends BaseRestApiAuthorizationIT {
  private static final String CATALOG = "source_lookup_a";
  private static final String OTHER_CATALOG = "source_lookup_b";
  private static final String ROLE = "source_lookup_reader";
  private static final String SOURCE_SCHEMA = "sources";
  private static final String MODEL_SCHEMA = "models";
  private GravitinoMetalake metalake;
  private SupportsSemanticModels readerSource;

  /** Creates sources and models in separate schemas and catalogs with distinct access grants. */
  @BeforeAll
  public void setUpSourceLookup() {
    metalake = client.loadMetalake(METALAKE);
    Catalog sourceCatalog = createCatalog(CATALOG);
    sourceCatalog.asSchemas().createSchema(SOURCE_SCHEMA, null, Map.of());
    sourceCatalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(SOURCE_SCHEMA, "orders"),
            new Column[] {Column.of("id", Types.LongType.get())},
            null,
            Map.of());
    SemanticModelDefinition definition =
        SemanticModelDefinition.builder()
            .withDatasets(new Dataset[] {dataset("employee.raw"), dataset("manager")})
            .build();
    for (Catalog catalog : List.of(sourceCatalog, createCatalog(OTHER_CATALOG))) {
      catalog.asSchemas().createSchema(MODEL_SCHEMA, null, Map.of());
      for (String model : List.of("hidden", "owned", "use")) {
        catalog
            .asSemanticModelCatalog()
            .createSemanticModel(
                NameIdentifier.of(MODEL_SCHEMA, model), null, definition, Map.of());
      }
      metalake.setOwner(model(catalog.name(), "owned"), NORMAL_USER, Owner.Type.USER);
    }
    metalake.createRole(
        ROLE,
        Map.of(),
        List.of(SecurableObjects.ofCatalog(CATALOG, List.of(Privileges.UseCatalog.allow()))));
    metalake.grantPrivilegesToRole(
        ROLE,
        MetadataObjects.of(CATALOG, SOURCE_SCHEMA, MetadataObject.Type.SCHEMA),
        List.of(Privileges.UseSchema.allow()));
    metalake.grantPrivilegesToRole(
        ROLE,
        MetadataObjects.of(List.of(CATALOG, SOURCE_SCHEMA, "orders"), MetadataObject.Type.TABLE),
        List.of(Privileges.SelectTable.allow()));
    metalake.grantRolesToUser(List.of(ROLE), NORMAL_USER);
    metalake.grantPrivilegesToRole(
        ROLE, model(CATALOG, "use"), List.of(Privileges.UseSemanticModel.allow()));
    readerSource =
        normalUserClient
            .loadMetalake(METALAKE)
            .loadCatalog(CATALOG)
            .asTableCatalog()
            .loadTable(NameIdentifier.of(SOURCE_SCHEMA, "orders"))
            .supportsSemanticModels();
  }

  @Test
  void testChecksSourceAndEachModelsOwnParents() {
    // Owning a model does not bypass missing access to its schema or catalog.
    assertArrayEquals(new NameIdentifier[0], readerSource.listSemanticModelDatasets());
    metalake.grantPrivilegesToRole(ROLE, schema(CATALOG), List.of(Privileges.UseSchema.allow()));
    assertArrayEquals(expected(CATALOG, "owned", "use"), readerSource.listSemanticModelDatasets());
    metalake.grantPrivilegesToRole(
        ROLE, schema(OTHER_CATALOG), List.of(Privileges.UseSchema.allow()));
    assertArrayEquals(expected(CATALOG, "owned", "use"), readerSource.listSemanticModelDatasets());
    metalake.grantPrivilegesToRole(
        ROLE,
        MetadataObjects.of(null, OTHER_CATALOG, MetadataObject.Type.CATALOG),
        List.of(Privileges.UseCatalog.allow()));
    NameIdentifier[] both =
        Stream.concat(
                Arrays.stream(expected(CATALOG, "owned", "use")),
                Arrays.stream(expected(OTHER_CATALOG, "owned")))
            .toArray(NameIdentifier[]::new);
    assertArrayEquals(both, readerSource.listSemanticModelDatasets());
    metalake.revokePrivilegesFromRole(
        ROLE,
        MetadataObjects.of(List.of(CATALOG, SOURCE_SCHEMA, "orders"), MetadataObject.Type.TABLE),
        Set.of(Privileges.SelectTable.allow()));
    assertThrows(ForbiddenException.class, readerSource::listSemanticModelDatasets);
  }

  private Catalog createCatalog(String name) {
    String database = GravitinoITUtils.genRandomName("source_auth");
    return metalake.createCatalog(
        name,
        Catalog.Type.RELATIONAL,
        "lakehouse-iceberg",
        null,
        Map.of(
            "catalog-backend",
            "jdbc",
            "warehouse",
            System.getProperty("java.io.tmpdir") + "/" + database,
            "uri",
            "jdbc:h2:mem:" + database + ";DB_CLOSE_DELAY=-1;MODE=MYSQL",
            "jdbc-driver",
            "org.h2.Driver",
            "jdbc-initialize",
            "true"));
  }

  private static Dataset dataset(String name) {
    return Dataset.builder()
        .withName(name)
        .withSource(NameIdentifier.of(CATALOG, SOURCE_SCHEMA, "orders"))
        .build();
  }

  private static MetadataObject model(String catalog, String name) {
    return MetadataObjects.of(
        List.of(catalog, MODEL_SCHEMA, name), MetadataObject.Type.SEMANTIC_MODEL);
  }

  private static MetadataObject schema(String catalog) {
    return MetadataObjects.of(catalog, MODEL_SCHEMA, MetadataObject.Type.SCHEMA);
  }

  private static NameIdentifier[] expected(String catalog, String... models) {
    return Arrays.stream(models)
        .flatMap(
            model ->
                Arrays.stream(new String[] {"employee.raw", "manager"})
                    .map(name -> NameIdentifier.of(METALAKE, catalog, MODEL_SCHEMA, model, name)))
        .toArray(NameIdentifier[]::new);
  }
}
