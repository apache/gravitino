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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.Schema;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.tag.Tag;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Integration test that verifies tags and policies assigned to a parent schema are inherited along
 * a multi-level (hierarchical) schema hierarchy.
 *
 * <p>A hierarchical schema {@code A:B:C} (using the configured {@code ":"} separator) has the
 * intermediate schemas {@code A:B} and {@code A} as ancestors. Listing the tags/policies of {@code
 * A:B:C} must therefore include the tags/policies assigned to {@code A:B}, {@code A} and the
 * catalog as inherited (see issue #11639). The catalog is an Iceberg catalog backed by an in-memory
 * H2 database, because {@code ":"} hierarchical schema names are only supported by Iceberg catalogs
 * accessed through the Gravitino REST server with a configured schema separator.
 */
@org.junit.jupiter.api.Tag("gravitino-docker-test")
public class HierarchicalSchemaTagPolicyIT extends BaseIT {

  private static final String METALAKE =
      GravitinoITUtils.genRandomName("hierarchical_tag_policy_metalake");
  private static final String CATALOG = "hierarchical_tag_policy_catalog";

  private static final String ROOT_A = "A";
  private static final String SCHEMA_AB = "A:B";
  private static final String SCHEMA_ABC = "A:B:C";

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
    // extra container, and supports ':' hierarchical schema names. H2 is NOT a supported
    // production Iceberg backend.
    Map<String, String> catalogProperties = new HashMap<>();
    catalogProperties.put("catalog-backend", "jdbc");
    catalogProperties.put("warehouse", "/tmp/gravitino-it-hierarchical-tag-policy");
    catalogProperties.put(
        "uri", "jdbc:h2:mem:gravitino-it-hierarchical-tag-policy;DB_CLOSE_DELAY=-1;MODE=MYSQL");
    catalogProperties.put("jdbc-driver", "org.h2.Driver");
    catalogProperties.put("jdbc-initialize", "true");
    catalog =
        metalake.createCatalog(
            CATALOG, Catalog.Type.RELATIONAL, "lakehouse-iceberg", "comment", catalogProperties);

    // Creating "A:B:C" auto-creates the parent chain "A" and "A:B".
    catalog.asSchemas().createSchema(SCHEMA_ABC, "hierarchical schema", new HashMap<>());
    Assertions.assertEquals(SCHEMA_AB, catalog.asSchemas().loadSchema(SCHEMA_AB).name());
    Assertions.assertEquals(ROOT_A, catalog.asSchemas().loadSchema(ROOT_A).name());
  }

  @AfterAll
  public void tearDown() {
    if (metalake != null) {
      metalake.dropCatalog(CATALOG, true);
      client.dropMetalake(METALAKE, true);
    }
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

    Tag[] tags = leafSchema.supportsTags().listTagsInfo();
    Map<String, Tag> byName = Arrays.stream(tags).collect(Collectors.toMap(Tag::name, t -> t));

    // The leaf schema's own tag plus the tags inherited from the intermediate schema A:B, the
    // ancestor schema A, and the catalog. Before the fix, A:B and A were skipped.
    Assertions.assertTrue(byName.containsKey(leafTag), "leaf tag should be present");
    Assertions.assertTrue(
        byName.containsKey(midTag), "tag on intermediate schema A:B should be inherited");
    Assertions.assertTrue(
        byName.containsKey(rootTag), "tag on ancestor schema A should be inherited");
    Assertions.assertTrue(byName.containsKey(catalogTag), "catalog tag should be inherited");

    Assertions.assertFalse(byName.get(leafTag).inherited().get());
    Assertions.assertTrue(byName.get(midTag).inherited().get());
    Assertions.assertTrue(byName.get(rootTag).inherited().get());
    Assertions.assertTrue(byName.get(catalogTag).inherited().get());

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

    Policy[] policies = leafSchema.supportsPolicies().listPolicyInfos();
    Map<String, Policy> byName =
        Arrays.stream(policies).collect(Collectors.toMap(Policy::name, p -> p));

    Assertions.assertTrue(byName.containsKey(leafPolicy), "leaf policy should be present");
    Assertions.assertTrue(
        byName.containsKey(midPolicy), "policy on intermediate schema A:B should be inherited");
    Assertions.assertTrue(
        byName.containsKey(rootPolicy), "policy on ancestor schema A should be inherited");
    Assertions.assertTrue(byName.containsKey(catalogPolicy), "catalog policy should be inherited");

    Assertions.assertFalse(byName.get(leafPolicy).inherited().get());
    Assertions.assertTrue(byName.get(midPolicy).inherited().get());
    Assertions.assertTrue(byName.get(rootPolicy).inherited().get());
    Assertions.assertTrue(byName.get(catalogPolicy).inherited().get());

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
}
