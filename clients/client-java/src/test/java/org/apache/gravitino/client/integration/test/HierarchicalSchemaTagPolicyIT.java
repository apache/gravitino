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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.policy.SupportsPolicies;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.tag.SupportsTags;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Integration test that verifies tags and policies assigned to a parent schema are inherited along
 * a multi-level (hierarchical) schema hierarchy, including by the tables and columns under it.
 *
 * <p>A hierarchical schema {@code A:B:C} (using the configured {@code ":"} separator) has the
 * intermediate schemas {@code A:B} and {@code A} as ancestors. Listing the tags/policies of {@code
 * A:B:C}, or of a table/column under it, must therefore include the tags/policies assigned to
 * {@code A:B}, {@code A} and the catalog as inherited (see issue #11639). The catalog is an Iceberg
 * catalog backed by an in-memory H2 database, because {@code ":"} hierarchical schema names are
 * only supported by Iceberg catalogs accessed through the Gravitino REST server with a configured
 * schema separator.
 *
 * <p>This test runs entirely against the embedded server and an in-memory H2 database, so it needs
 * no Docker container and is intentionally not tagged {@code gravitino-docker-test}; tagging it as
 * such would exclude it from the standard (non-Docker) integration-test runs.
 */
public class HierarchicalSchemaTagPolicyIT extends BaseIT {

  private static final String METALAKE =
      GravitinoITUtils.genRandomName("hierarchical_tag_policy_metalake");
  private static final String CATALOG = "hierarchical_tag_policy_catalog";

  private static final String ROOT_A = "A";
  private static final String SCHEMA_AB = "A:B";
  private static final String SCHEMA_ABC = "A:B:C";

  // A table directly under the deepest schema A:B:C and one under the intermediate schema A:B, used
  // to verify that tables/columns inherit tags and policies according to their position in the
  // hierarchy.
  private static final String LEAF_TABLE = "leaf_table";
  private static final String MID_TABLE = "mid_table";
  private static final String COLUMN_NAME = "col1";

  private static GravitinoMetalake metalake;
  private static Catalog catalog;

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    // Configure the hierarchical schema separator before the server starts.
    customConfigs.put(Configs.SCHEMA_SEPARATOR.getKey(), ":");
    super.startIntegrationTest();

    metalake = client.createMetalake(METALAKE, "comment", new HashMap<>());

    // Create an Iceberg catalog backed by an in-memory H2 database; it is lightweight, needs no
    // extra container, and supports ':' hierarchical schema names. H2 is NOT a supported production
    // Iceberg backend. The warehouse path and the H2 database name are derived from the randomized
    // metalake name so concurrent runs on the same host do not share state and interfere.
    Map<String, String> catalogProperties = new HashMap<>();
    catalogProperties.put("catalog-backend", "jdbc");
    catalogProperties.put("warehouse", "/tmp/" + METALAKE);
    catalogProperties.put("uri", "jdbc:h2:mem:" + METALAKE + ";DB_CLOSE_DELAY=-1;MODE=MYSQL");
    catalogProperties.put("jdbc-driver", "org.h2.Driver");
    catalogProperties.put("jdbc-initialize", "true");
    catalog =
        metalake.createCatalog(
            CATALOG, Catalog.Type.RELATIONAL, "lakehouse-iceberg", "comment", catalogProperties);

    // Creating "A:B:C" auto-creates the parent chain "A" and "A:B".
    catalog.asSchemas().createSchema(SCHEMA_ABC, "hierarchical schema", new HashMap<>());
    Assertions.assertEquals(SCHEMA_AB, catalog.asSchemas().loadSchema(SCHEMA_AB).name());
    Assertions.assertEquals(ROOT_A, catalog.asSchemas().loadSchema(ROOT_A).name());

    // A table under the deepest schema A:B:C and one under the intermediate schema A:B, so that
    // table/column tag and policy inheritance through the hierarchy can be verified.
    Column[] columns = new Column[] {Column.of(COLUMN_NAME, Types.IntegerType.get())};
    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(SCHEMA_ABC, LEAF_TABLE),
            columns,
            "table under A:B:C",
            new HashMap<>());
    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(SCHEMA_AB, MID_TABLE), columns, "table under A:B", new HashMap<>());
  }

  @AfterAll
  public void tearDown() {
    if (metalake != null) {
      metalake.dropCatalog(CATALOG, true);
      client.dropMetalake(METALAKE, true);
    }
    // Note: this intentionally does not call super.stopIntegrationTest(). JUnit 5 invokes @AfterAll
    // methods of both this subclass and the parent BaseIT, so BaseIT.stopIntegrationTest() (which
    // stops the embedded server and clears customConfigs) still runs. Closing the client and
    // nulling it here prevents BaseIT from double-closing it afterwards.
    if (client != null) {
      client.close();
      client = null;
    }
  }

  @Test
  public void testTagInheritanceThroughHierarchicalSchema() {
    String catalogTag = GravitinoITUtils.genRandomName("h_catalog_tag");
    String rootTag = GravitinoITUtils.genRandomName("h_root_tag");
    String midTag = GravitinoITUtils.genRandomName("h_mid_tag");
    String leafTag = GravitinoITUtils.genRandomName("h_leaf_tag");
    for (String tag : new String[] {catalogTag, rootTag, midTag, leafTag}) {
      metalake.createTag(tag, "comment", Collections.emptyMap());
    }

    catalog.supportsTags().associateTags(new String[] {catalogTag}, null);
    catalog
        .asSchemas()
        .loadSchema(ROOT_A)
        .supportsTags()
        .associateTags(new String[] {rootTag}, null);
    catalog
        .asSchemas()
        .loadSchema(SCHEMA_AB)
        .supportsTags()
        .associateTags(new String[] {midTag}, null);
    Schema leafSchema = catalog.asSchemas().loadSchema(SCHEMA_ABC);
    leafSchema.supportsTags().associateTags(new String[] {leafTag}, null);

    Map<String, Boolean> inheritedByName = tagInheritanceByName(leafSchema.supportsTags());

    // The leaf schema's own tag plus the tags inherited from the intermediate schema A:B, the
    // ancestor schema A, and the catalog. Before the fix, A:B and A were skipped.
    Assertions.assertTrue(inheritedByName.containsKey(leafTag), "leaf tag should be present");
    Assertions.assertTrue(
        inheritedByName.containsKey(midTag), "tag on intermediate schema A:B should be inherited");
    Assertions.assertTrue(
        inheritedByName.containsKey(rootTag), "tag on ancestor schema A should be inherited");
    Assertions.assertTrue(
        inheritedByName.containsKey(catalogTag), "catalog tag should be inherited");

    Assertions.assertFalse(inheritedByName.get(leafTag));
    Assertions.assertTrue(inheritedByName.get(midTag));
    Assertions.assertTrue(inheritedByName.get(rootTag));
    Assertions.assertTrue(inheritedByName.get(catalogTag));

    // Clean up associations and tags so the test is repeatable.
    catalog.supportsTags().associateTags(null, new String[] {catalogTag});
    catalog
        .asSchemas()
        .loadSchema(ROOT_A)
        .supportsTags()
        .associateTags(null, new String[] {rootTag});
    catalog
        .asSchemas()
        .loadSchema(SCHEMA_AB)
        .supportsTags()
        .associateTags(null, new String[] {midTag});
    leafSchema.supportsTags().associateTags(null, new String[] {leafTag});
    for (String tag : new String[] {catalogTag, rootTag, midTag, leafTag}) {
      metalake.deleteTag(tag);
    }
  }

  @Test
  public void testPolicyInheritanceThroughHierarchicalSchema() {
    Set<MetadataObject.Type> types =
        ImmutableSet.of(MetadataObject.Type.CATALOG, MetadataObject.Type.SCHEMA);
    PolicyContent content = PolicyContents.custom(ImmutableMap.of("rule", "value"), types, null);

    String catalogPolicy = GravitinoITUtils.genRandomName("h_catalog_policy");
    String rootPolicy = GravitinoITUtils.genRandomName("h_root_policy");
    String midPolicy = GravitinoITUtils.genRandomName("h_mid_policy");
    String leafPolicy = GravitinoITUtils.genRandomName("h_leaf_policy");
    for (String policy : new String[] {catalogPolicy, rootPolicy, midPolicy, leafPolicy}) {
      metalake.createPolicy(policy, "custom", "comment", true, content);
    }

    catalog.supportsPolicies().associatePolicies(new String[] {catalogPolicy}, null);
    catalog
        .asSchemas()
        .loadSchema(ROOT_A)
        .supportsPolicies()
        .associatePolicies(new String[] {rootPolicy}, null);
    catalog
        .asSchemas()
        .loadSchema(SCHEMA_AB)
        .supportsPolicies()
        .associatePolicies(new String[] {midPolicy}, null);
    Schema leafSchema = catalog.asSchemas().loadSchema(SCHEMA_ABC);
    leafSchema.supportsPolicies().associatePolicies(new String[] {leafPolicy}, null);

    Map<String, Boolean> inheritedByName = policyInheritanceByName(leafSchema.supportsPolicies());

    Assertions.assertTrue(inheritedByName.containsKey(leafPolicy), "leaf policy should be present");
    Assertions.assertTrue(
        inheritedByName.containsKey(midPolicy),
        "policy on intermediate schema A:B should be inherited");
    Assertions.assertTrue(
        inheritedByName.containsKey(rootPolicy), "policy on ancestor schema A should be inherited");
    Assertions.assertTrue(
        inheritedByName.containsKey(catalogPolicy), "catalog policy should be inherited");

    Assertions.assertFalse(inheritedByName.get(leafPolicy));
    Assertions.assertTrue(inheritedByName.get(midPolicy));
    Assertions.assertTrue(inheritedByName.get(rootPolicy));
    Assertions.assertTrue(inheritedByName.get(catalogPolicy));

    // Clean up associations and policies so the test is repeatable.
    catalog.supportsPolicies().associatePolicies(null, new String[] {catalogPolicy});
    catalog
        .asSchemas()
        .loadSchema(ROOT_A)
        .supportsPolicies()
        .associatePolicies(null, new String[] {rootPolicy});
    catalog
        .asSchemas()
        .loadSchema(SCHEMA_AB)
        .supportsPolicies()
        .associatePolicies(null, new String[] {midPolicy});
    leafSchema.supportsPolicies().associatePolicies(null, new String[] {leafPolicy});
    for (String policy : new String[] {catalogPolicy, rootPolicy, midPolicy, leafPolicy}) {
      metalake.deletePolicy(policy);
    }
  }

  @Test
  public void testTagInheritanceForTableAndColumnUnderHierarchicalSchema() {
    String catalogTag = GravitinoITUtils.genRandomName("ht_catalog_tag");
    String rootTag = GravitinoITUtils.genRandomName("ht_root_tag");
    String midTag = GravitinoITUtils.genRandomName("ht_mid_tag");
    String leafTag = GravitinoITUtils.genRandomName("ht_leaf_tag");
    for (String tag : new String[] {catalogTag, rootTag, midTag, leafTag}) {
      metalake.createTag(tag, "comment", Collections.emptyMap());
    }

    catalog.supportsTags().associateTags(new String[] {catalogTag}, null);
    catalog
        .asSchemas()
        .loadSchema(ROOT_A)
        .supportsTags()
        .associateTags(new String[] {rootTag}, null);
    catalog
        .asSchemas()
        .loadSchema(SCHEMA_AB)
        .supportsTags()
        .associateTags(new String[] {midTag}, null);
    catalog
        .asSchemas()
        .loadSchema(SCHEMA_ABC)
        .supportsTags()
        .associateTags(new String[] {leafTag}, null);

    // A table under the deepest schema A:B:C inherits the whole chain: A:B:C, A:B, A and the
    // catalog.
    Table leafTable = catalog.asTableCatalog().loadTable(NameIdentifier.of(SCHEMA_ABC, LEAF_TABLE));
    Map<String, Boolean> tableTags = tagInheritanceByName(leafTable.supportsTags());
    for (String tag : new String[] {catalogTag, rootTag, midTag, leafTag}) {
      Assertions.assertTrue(tableTags.containsKey(tag), tag + " should be inherited by the table");
      Assertions.assertTrue(tableTags.get(tag), tag + " on an ancestor must be marked inherited");
    }

    // A column of that table inherits the same chain.
    Map<String, Boolean> columnTags =
        tagInheritanceByName(columnByName(leafTable, COLUMN_NAME).supportsTags());
    for (String tag : new String[] {catalogTag, rootTag, midTag, leafTag}) {
      Assertions.assertTrue(
          columnTags.containsKey(tag), tag + " should be inherited by the column");
      Assertions.assertTrue(columnTags.get(tag), tag + " on an ancestor must be marked inherited");
    }

    // A table under the intermediate schema A:B inherits A:B, A and the catalog, but not the tag
    // assigned only to the deeper sibling schema A:B:C.
    Map<String, Boolean> midTableTags =
        tagInheritanceByName(
            catalog
                .asTableCatalog()
                .loadTable(NameIdentifier.of(SCHEMA_AB, MID_TABLE))
                .supportsTags());
    Assertions.assertTrue(midTableTags.containsKey(catalogTag));
    Assertions.assertTrue(midTableTags.containsKey(rootTag));
    Assertions.assertTrue(midTableTags.containsKey(midTag));
    Assertions.assertFalse(
        midTableTags.containsKey(leafTag), "tag on A:B:C must not leak to a table under A:B");

    // Clean up associations and tags so the test is repeatable.
    catalog.supportsTags().associateTags(null, new String[] {catalogTag});
    catalog
        .asSchemas()
        .loadSchema(ROOT_A)
        .supportsTags()
        .associateTags(null, new String[] {rootTag});
    catalog
        .asSchemas()
        .loadSchema(SCHEMA_AB)
        .supportsTags()
        .associateTags(null, new String[] {midTag});
    catalog
        .asSchemas()
        .loadSchema(SCHEMA_ABC)
        .supportsTags()
        .associateTags(null, new String[] {leafTag});
    for (String tag : new String[] {catalogTag, rootTag, midTag, leafTag}) {
      metalake.deleteTag(tag);
    }
  }

  @Test
  public void testPolicyInheritanceForTableUnderHierarchicalSchema() {
    Set<MetadataObject.Type> types =
        ImmutableSet.of(
            MetadataObject.Type.CATALOG, MetadataObject.Type.SCHEMA, MetadataObject.Type.TABLE);
    PolicyContent content = PolicyContents.custom(ImmutableMap.of("rule", "value"), types, null);

    String catalogPolicy = GravitinoITUtils.genRandomName("ht_catalog_policy");
    String rootPolicy = GravitinoITUtils.genRandomName("ht_root_policy");
    String midPolicy = GravitinoITUtils.genRandomName("ht_mid_policy");
    String leafPolicy = GravitinoITUtils.genRandomName("ht_leaf_policy");
    for (String policy : new String[] {catalogPolicy, rootPolicy, midPolicy, leafPolicy}) {
      metalake.createPolicy(policy, "custom", "comment", true, content);
    }

    catalog.supportsPolicies().associatePolicies(new String[] {catalogPolicy}, null);
    catalog
        .asSchemas()
        .loadSchema(ROOT_A)
        .supportsPolicies()
        .associatePolicies(new String[] {rootPolicy}, null);
    catalog
        .asSchemas()
        .loadSchema(SCHEMA_AB)
        .supportsPolicies()
        .associatePolicies(new String[] {midPolicy}, null);
    catalog
        .asSchemas()
        .loadSchema(SCHEMA_ABC)
        .supportsPolicies()
        .associatePolicies(new String[] {leafPolicy}, null);

    // A table under the deepest schema A:B:C inherits policies through the whole chain.
    Map<String, Boolean> tablePolicies =
        policyInheritanceByName(
            catalog
                .asTableCatalog()
                .loadTable(NameIdentifier.of(SCHEMA_ABC, LEAF_TABLE))
                .supportsPolicies());
    for (String policy : new String[] {catalogPolicy, rootPolicy, midPolicy, leafPolicy}) {
      Assertions.assertTrue(
          tablePolicies.containsKey(policy), policy + " should be inherited by the table");
      Assertions.assertTrue(
          tablePolicies.get(policy), policy + " on an ancestor must be marked inherited");
    }

    // A table under the intermediate schema A:B does not inherit the policy assigned only to the
    // deeper sibling schema A:B:C.
    Map<String, Boolean> midTablePolicies =
        policyInheritanceByName(
            catalog
                .asTableCatalog()
                .loadTable(NameIdentifier.of(SCHEMA_AB, MID_TABLE))
                .supportsPolicies());
    Assertions.assertTrue(midTablePolicies.containsKey(catalogPolicy));
    Assertions.assertTrue(midTablePolicies.containsKey(rootPolicy));
    Assertions.assertTrue(midTablePolicies.containsKey(midPolicy));
    Assertions.assertFalse(
        midTablePolicies.containsKey(leafPolicy),
        "policy on A:B:C must not leak to a table under A:B");

    // Clean up associations and policies so the test is repeatable.
    catalog.supportsPolicies().associatePolicies(null, new String[] {catalogPolicy});
    catalog
        .asSchemas()
        .loadSchema(ROOT_A)
        .supportsPolicies()
        .associatePolicies(null, new String[] {rootPolicy});
    catalog
        .asSchemas()
        .loadSchema(SCHEMA_AB)
        .supportsPolicies()
        .associatePolicies(null, new String[] {midPolicy});
    catalog
        .asSchemas()
        .loadSchema(SCHEMA_ABC)
        .supportsPolicies()
        .associatePolicies(null, new String[] {leafPolicy});
    for (String policy : new String[] {catalogPolicy, rootPolicy, midPolicy, leafPolicy}) {
      metalake.deletePolicy(policy);
    }
  }

  private static Map<String, Boolean> tagInheritanceByName(SupportsTags supportsTags) {
    return Stream.of(supportsTags.listTagsInfo())
        .collect(Collectors.toMap(tag -> tag.name(), tag -> tag.inherited().get()));
  }

  private static Map<String, Boolean> policyInheritanceByName(SupportsPolicies supportsPolicies) {
    return Stream.of(supportsPolicies.listPolicyInfos())
        .collect(Collectors.toMap(policy -> policy.name(), policy -> policy.inherited().get()));
  }

  private static Column columnByName(Table table, String columnName) {
    return Stream.of(table.columns())
        .filter(column -> column.name().equals(columnName))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("Column not found: " + columnName));
  }
}
