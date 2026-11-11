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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.domain.DomainChange;
import org.apache.gravitino.domain.DomainMember;
import org.apache.gravitino.domain.DomainMemberFilter;
import org.apache.gravitino.domain.DomainRelation;
import org.apache.gravitino.exceptions.BackingTagAlreadyBoundException;
import org.apache.gravitino.exceptions.DomainAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchDomainException;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.exceptions.NonEmptyDomainException;
import org.apache.gravitino.exceptions.TagAlreadyExistsException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.tag.TagChange;
import org.apache.gravitino.tag.TagValue;
import org.apache.gravitino.tag.TagValueConstraint;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Real-server Domain SDK tests using the metadata backend selected by {@code jdbcBackend}.
 *
 * <p>Backend provisioning and server lifecycle follow {@link BaseIT}, including MySQL and
 * PostgreSQL.
 */
public class DomainIT extends BaseIT {
  private String metalakeName;
  /** The test metalake, owned by the explicitly authenticated service admin. */
  protected GravitinoMetalake metalake;

  @TempDir Path directory;

  /** {@inheritDoc} */
  @Override
  @BeforeAll
  public void startIntegrationTest() throws Exception {
    customConfigs.put(
        Configs.ENABLE_AUTHORIZATION.getKey(), String.valueOf(authorizationEnabled()));
    customConfigs.put(Configs.SERVICE_ADMINS.getKey(), "anonymous");
    customConfigs.put(Configs.AUTHENTICATORS.getKey(), "simple");
    // BaseIT also uses this bind host in client URIs; a wildcard address is not a client target.
    // Loopback keeps the test server local and makes readiness probes reachable on macOS.
    customConfigs.put("gravitino.server.webserver.host", "127.0.0.1");

    super.startIntegrationTest();

    // Use the configured service admin rather than the developer's operating-system username.
    client.close();
    client = GravitinoAdminClient.builder(serverUri).withSimpleAuth("anonymous").build();
  }

  /** {@inheritDoc} */
  @Override
  @AfterAll
  public void stopIntegrationTest() throws IOException, InterruptedException {
    String storage =
        serverConfig == null ? null : serverConfig.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PATH);
    boolean embedded = "embedded".equals(testMode);
    super.stopIntegrationTest();

    // BaseIT owns external database containers; only the embedded H2 store leaves local files.
    if (embedded && "h2".equalsIgnoreCase(getJDBCBackend()) && storage != null) {
      for (String suffix : List.of(".mv.db", ".trace.db", ".lock.db", "")) {
        Files.deleteIfExists(Path.of(storage + suffix));
      }
    }
  }

  /** Whether this suite enables the real Casbin authorizer. */
  protected boolean authorizationEnabled() {
    return false;
  }

  /**
   * Creates an isolated Iceberg JDBC catalog and schema, using local H2 and files.
   *
   * @return The catalog, containing an empty schema named schema.
   */
  protected Catalog createRelationalCatalog() {
    // This is the Iceberg catalog database, independent of Gravitino's jdbcBackend entity store.
    Catalog catalog =
        metalake.createCatalog(
            "ice",
            Catalog.Type.RELATIONAL,
            "lakehouse-iceberg",
            "",
            Map.of(
                "catalog-backend",
                "jdbc",
                "warehouse",
                directory.resolve("warehouse").toString(),
                "uri",
                "jdbc:h2:mem:" + metalake.name() + ";DB_CLOSE_DELAY=-1;MODE=MYSQL",
                "jdbc-driver",
                "org.h2.Driver",
                "jdbc-initialize",
                "true"));
    catalog.asSchemas().createSchema("schema", "", Collections.emptyMap());
    return catalog;
  }

  @BeforeEach
  void createMetalake() {
    metalakeName = "domain_it_" + UUID.randomUUID().toString().replace("-", "");
    metalake = client.createMetalake(metalakeName, "", Collections.emptyMap());
  }

  @AfterEach
  void dropMetalake() {
    client.dropMetalake(metalakeName, true);
  }

  @Test
  void testDomainLifecycleAndEncodedNames() {
    // Exercise names that must survive path and query encoding without becoming hierarchy.
    String name = "Finance/US. East,?&+%";
    Domain draft = create(name);
    assertEquals(name, metalake.getDomain(name).name());
    assertEquals(Set.of(name), Set.of(metalake.listDomains()));
    assertEquals(name, metalake.listDomainsInfo()[0].name());
    assertEquals(Domain.Status.DRAFT, draft.status());

    // Mutations return a fresh snapshot and leave previously loaded state unchanged.
    Domain published = draft.publish();
    assertEquals(Domain.Status.PUBLISHED, published.status());
    assertEquals(Domain.Status.DRAFT, draft.status());
    assertEquals(Domain.Status.DRAFT, published.unpublish().status());

    Map<String, String> properties = new HashMap<>();
    properties.put("key", "value");
    Domain changed =
        metalake.alterDomain(
            name, DomainChange.updateComment("new"), DomainChange.setProperty("key", "value"));
    assertEquals("new", changed.comment());
    assertEquals(properties, changed.properties());
    assertNull(draft.comment());
    assertTrue(draft.properties().isEmpty());

    // Renaming a domain preserves its backing tag but invalidates the old name until reused.
    changed =
        metalake.alterDomain(
            name, DomainChange.removeProperty("key"), DomainChange.rename("Renamed/US. East,?&+%"));
    assertTrue(changed.properties().isEmpty());
    assertEquals(
        TagValue.noValue(name.replace(".", "_") + " tag"), changed.backingTag().orElseThrow());
    assertNull(metalake.alterDomain(changed.name(), DomainChange.updateComment(null)).comment());
    assertEquals("new", changed.comment());
    assertThrows(NoSuchDomainException.class, draft::publish);

    // An old snapshot addresses the replacement domain by name, not the original entity ID.
    Domain replacement =
        metalake.createDomain(name, null, null, TagValue.noValue("replacement tag"), true);
    assertEquals(Domain.Status.PUBLISHED, draft.publish().status());
    assertEquals(Domain.Status.DRAFT, replacement.status());

    assertTrue(metalake.deleteDomain(name));
    assertFalse(metalake.deleteDomain(name));
    assertTrue(metalake.deleteDomain(changed.name()));
  }

  @Test
  void testHierarchyAndBackingTagChanges() {
    Domain parent = create("Parent/with. spaces");
    Domain other = create("Other/parent. name");
    Domain child =
        parent.createSubdomain("Tax,EU/child", null, null, TagValue.noValue("child tag"), true);
    assertEquals(parent.name(), child.parent().orElseThrow());
    assertArrayEquals(new String[] {child.name()}, parent.listSubdomains());
    assertEquals(child.name(), parent.listSubdomainsInfo()[0].name());

    // Published children constrain both parent deletion and visibility changes.
    assertThrows(NonEmptyDomainException.class, () -> metalake.deleteDomain(parent.name()));
    assertThrows(IllegalArgumentException.class, child::publish);
    parent.publish();
    Domain publishedChild = child.publish();
    assertThrows(IllegalArgumentException.class, parent::unpublish);
    assertThrows(IllegalArgumentException.class, () -> publishedChild.attachToParent(other.name()));

    // Move and detach operate on current server state even when invoked on an older snapshot.
    other.publish();
    Domain moved = publishedChild.attachToParent(other.name());
    assertEquals(other.name(), moved.parent().orElseThrow());
    assertEquals(parent.name(), child.parent().orElseThrow());
    assertEquals(0, parent.listSubdomains().length);
    assertEquals(child.name(), other.listSubdomains()[0]);
    assertTrue(child.detachFromParent().parent().isEmpty());
    assertTrue(metalake.getDomain(child.name()).parent().isEmpty());
    assertEquals(parent.name(), child.parent().orElseThrow());

    // Removing a binding does not delete the tag or mutate the original snapshot.
    metalake.createTag("new backing", null, Collections.emptyMap());
    assertEquals(
        TagValue.noValue("new backing"),
        child.updateBackingTag(TagValue.noValue("new backing")).backingTag().orElseThrow());
    assertTrue(child.updateBackingTag(null).backingTag().isEmpty());
    assertEquals(TagValue.noValue("child tag"), child.backingTag().orElseThrow());
    assertNotNull(metalake.getTag("child tag"));
    assertTrue(metalake.deleteDomain(parent.name()));
  }

  @Test
  void testErrorsAndEmptyMemberFilters() {
    Domain domain = create("errors");
    assertThrows(
        DomainAlreadyExistsException.class,
        () -> metalake.createDomain(domain.name(), null, null, TagValue.noValue("another"), true));
    assertThrows(
        BackingTagAlreadyBoundException.class,
        () ->
            metalake.createDomain(
                "conflict", null, null, domain.backingTag().orElseThrow(), false));
    assertEquals(
        0,
        domain.listMembers(
                DomainMemberFilter.builder()
                    .withTypes(MetadataObject.Type.TABLE, MetadataObject.Type.VIEW)
                    .withDomains("Tax,EU", "Trading/US. East&?+%")
                    .withNameContains("x /?&+%,")
                    .build())
            .length);
    assertThrows(NoSuchDomainException.class, () -> metalake.getDomain("missing"));
  }

  @Test
  void testMembershipAcrossLocalCatalogs() {
    Catalog catalog = createRelationalCatalog();
    Column[] columns = {Column.of("id", Types.LongType.get())};
    Table table =
        catalog
            .asTableCatalog()
            .createTable(
                NameIdentifier.of("schema", "orders"), columns, "", Collections.emptyMap());
    Table hidden =
        catalog
            .asTableCatalog()
            .createTable(
                NameIdentifier.of("schema", "hidden"), columns, "", Collections.emptyMap());
    View view =
        catalog
            .asViewCatalog()
            .createView(
                NameIdentifier.of("schema", "orders_view"),
                "",
                columns,
                new SQLRepresentation[] {
                  SQLRepresentation.builder()
                      .withDialect("spark")
                      .withSql("SELECT id FROM ice.schema.orders")
                      .build()
                },
                null,
                null,
                Collections.emptyMap());
    SemanticModel semantic =
        catalog
            .asSemanticModelCatalog()
            .createSemanticModel(
                NameIdentifier.of("schema", "business"),
                "",
                SemanticModelDefinition.builder()
                    .withDatasets(
                        new Dataset[] {
                          Dataset.builder()
                              .withName("orders")
                              .withSource(NameIdentifier.of("ice", "schema", "orders"))
                              .build()
                        })
                    .build(),
                Collections.emptyMap());

    // Add the remaining locally supported leaf types from independent catalogs.
    Catalog files =
        metalake.createCatalog(
            "files",
            Catalog.Type.FILESET,
            "hadoop",
            "",
            Map.of("location", directory.resolve("files").toString()));
    files.asSchemas().createSchema("schema", "", Collections.emptyMap());
    Fileset fileset =
        files
            .asFilesetCatalog()
            .createFileset(
                NameIdentifier.of("schema", "data"),
                "",
                Fileset.Type.EXTERNAL,
                directory.resolve("external").toString(),
                Collections.emptyMap());

    Catalog models =
        metalake.createCatalog("models", Catalog.Type.MODEL, "model", "", Collections.emptyMap());
    models.asSchemas().createSchema("schema", "", Collections.emptyMap());
    Model model =
        models
            .asModelCatalog()
            .registerModel(NameIdentifier.of("schema", "model"), "", Collections.emptyMap());

    // The schema supplies Finance membership; the nearest table assignment overrides it for Risk.
    metalake.createTag(
        "department",
        null,
        Collections.emptyMap(),
        TagValueConstraint.ofAllowedValues("finance", "risk"));
    Domain finance =
        metalake.createDomain(
            "Finance/US. East,?&+%", null, null, TagValue.of("department", "finance"), false);
    Domain risk =
        metalake.createDomain("Risk,EU", null, null, TagValue.of("department", "risk"), false);
    Domain child =
        finance.createSubdomain(
            "Trading,US/with. spaces?&+%", null, null, TagValue.noValue("trading"), true);
    catalog
        .asSchemas()
        .loadSchema("schema")
        .supportsTags()
        .associateTags(new TagValue[] {TagValue.of("department", "finance")}, new TagValue[0]);
    table.supportsTags().associateTags(new String[] {"trading"}, new String[0]);
    hidden
        .supportsTags()
        .associateTags(new TagValue[] {TagValue.of("department", "risk")}, new TagValue[0]);
    fileset
        .supportsTags()
        .associateTags(new TagValue[] {TagValue.of("department", "finance")}, new TagValue[0]);
    model
        .supportsTags()
        .associateTags(new TagValue[] {TagValue.of("department", "finance")}, new TagValue[0]);

    // An object can be inherited from Finance and reached through Trading at the same time.
    Map<String, DomainMember> members =
        Arrays.stream(finance.listMembers(DomainMemberFilter.builder().build()))
            .collect(Collectors.toMap(member -> member.object().fullName(), member -> member));
    assertEquals(
        Set.of(
            "ice.schema.orders",
            "ice.schema.orders_view",
            "ice.schema.business",
            "files.schema.data",
            "models.schema.model"),
        members.keySet());
    assertEquals(
        Set.of(DomainRelation.Type.INHERITED, DomainRelation.Type.VIA_SUBDOMAIN),
        Arrays.stream(members.get("ice.schema.orders").relations())
            .map(DomainRelation::type)
            .collect(Collectors.toSet()));
    assertEquals(
        DomainRelation.Type.DIRECT, members.get("files.schema.data").relations()[0].type());
    assertEquals(
        "ice.schema.hidden",
        risk.listMembers(DomainMemberFilter.builder().build())[0].object().fullName());

    // Verify the reverse lookup exposed by every supported leaf resource.
    assertTrue(Set.of(table.supportsDomains().listDomains()).contains(finance.name()));
    assertEquals(finance.name(), view.supportsDomains().listDomainsInfo()[0].name());
    assertEquals(finance.name(), semantic.supportsDomains().listDomainsInfo()[0].name());
    assertEquals(finance.name(), fileset.supportsDomains().listDomainsInfo()[0].name());
    assertEquals(finance.name(), model.supportsDomains().listDomainsInfo()[0].name());

    // Commas belong to scope names; repeated scopes select their union without duplicating members.
    assertEquals(
        1,
        finance.listMembers(
                DomainMemberFilter.builder().withDomains(child.name(), "unknown,scope").build())
            .length);
    assertEquals(
        5,
        finance.listMembers(
                DomainMemberFilter.builder().withDomains(finance.name(), child.name()).build())
            .length);
    assertEquals(
        2,
        finance.listMembers(
                DomainMemberFilter.builder()
                    .withTypes(MetadataObject.Type.VIEW, MetadataObject.Type.MODEL)
                    .build())
            .length);
    assertEquals(
        2,
        finance.listMembers(DomainMemberFilter.builder().withNameContains("ORDERS").build())
            .length);
    assertEquals(
        0,
        finance.listMembers(DomainMemberFilter.builder().withNameContains("/ ?&+%,").build())
            .length);

    // Membership roll-ups follow hierarchy changes without retagging the leaf object.
    Domain other = create("new parent");
    child.attachToParent(other.name());
    assertTrue(Set.of(table.supportsDomains().listDomains()).contains(other.name()));
    metalake.alterDomain(child.name(), DomainChange.rename("renamed/child"));
    assertTrue(Set.of(table.supportsDomains().listDomains()).contains("renamed/child"));
    metalake.deleteDomain("renamed/child");
    assertFalse(Set.of(table.supportsDomains().listDomains()).contains(other.name()));
    assertNotNull(metalake.getTag("trading"));
    finance.updateBackingTag(null);
    assertEquals(0, table.supportsDomains().listDomains().length);
  }

  @Test
  void testEncodedNamesThroughPreservingProxy() throws Exception {
    // Preserve the raw URI so this proxy does not decode %2F before forwarding to Jetty.
    HttpServer proxy = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    HttpClient upstream = HttpClient.newHttpClient();
    proxy.createContext("/", exchange -> forward(exchange, upstream));
    proxy.start();
    String uri = "http://127.0.0.1:" + proxy.getAddress().getPort();
    try (GravitinoAdminClient proxyClient =
        GravitinoAdminClient.builder(uri).withSimpleAuth("anonymous").build()) {
      GravitinoMetalake proxied = proxyClient.loadMetalake(metalake.name());
      String name = "Proxy/US. East,?&+%";
      Domain domain = proxied.createDomain(name, null, null, TagValue.noValue("proxy tag"), true);
      assertEquals(name, proxied.getDomain(name).name());
      assertEquals(Domain.Status.PUBLISHED, domain.publish().status());
      Catalog catalog = createRelationalCatalog();
      Table table = createTable(catalog);
      Table other =
          catalog
              .asTableCatalog()
              .createTable(
                  NameIdentifier.of("schema", "other"),
                  new Column[] {Column.of("id", Types.LongType.get())},
                  "",
                  Collections.emptyMap());
      Domain first =
          domain.createSubdomain("scope,one", null, null, TagValue.noValue("proxy-first"), true);
      Domain second =
          domain.createSubdomain(
              "scope/two?&+%", null, null, TagValue.noValue("proxy-second"), true);
      table.supportsTags().associateTags(new String[] {"proxy-first"}, new String[0]);
      other.supportsTags().associateTags(new String[] {"proxy-second"}, new String[0]);

      // A dropped or incorrectly split scope parameter changes these nonempty results.
      DomainMember[] selected =
          domain.listMembers(
              DomainMemberFilter.builder()
                  .withDomains(first.name(), "unknown,scope")
                  .withNameContains("ORDERS")
                  .build());
      assertEquals(1, selected.length);
      assertEquals("ice.schema.orders", selected[0].object().fullName());
      assertArrayEquals(
          new DomainRelation[] {DomainRelation.viaSubdomain(first.name())},
          selected[0].relations());
      assertEquals(
          2,
          domain.listMembers(
                  DomainMemberFilter.builder().withDomains(first.name(), second.name()).build())
              .length);
      assertEquals(
          "ice.schema.other",
          domain
              .listMembers(DomainMemberFilter.builder().withDomains(second.name()).build())[0]
              .object()
              .fullName());
      assertEquals(
          0,
          domain.listMembers(DomainMemberFilter.builder().withDomains("unknown,scope").build())
              .length);
      assertEquals(
          0,
          domain.listMembers(
                  DomainMemberFilter.builder()
                      .withDomains(first.name())
                      .withNameContains("other")
                      .build())
              .length);
      assertTrue(proxied.deleteDomain(first.name()));
      assertTrue(proxied.deleteDomain(second.name()));
      assertTrue(proxied.deleteDomain(name));
    } finally {
      proxy.stop(0);
    }
  }

  @Test
  void testNearestTagAssignmentAndValueMatching() {
    Catalog catalog = createRelationalCatalog();
    Table table = createTable(catalog);
    metalake.createTag(
        "department",
        null,
        Collections.emptyMap(),
        TagValueConstraint.ofAllowedValues("finance", "risk", "Finance"));
    Domain finance =
        metalake.createDomain("Finance", null, null, TagValue.of("department", "finance"), false);
    Domain risk =
        metalake.createDomain("Risk", null, null, TagValue.of("department", "risk"), false);
    Domain caseSensitive =
        metalake.createDomain(
            "Capitalized", null, null, TagValue.of("department", "Finance"), false);

    assertThrows(
        IllegalArgumentException.class,
        () -> finance.updateBackingTag(TagValue.of("department", "unknown")));
    assertEquals(
        TagValue.of("department", "finance"),
        metalake.getDomain(finance.name()).backingTag().orElseThrow());

    // A catalog assignment is inherited until a nearer schema assignment masks the same tag.
    catalog
        .supportsTags()
        .associateTags(new TagValue[] {TagValue.of("department", "finance")}, new TagValue[0]);
    assertTableRelations(
        finance,
        table,
        DomainRelation.inherited(MetadataObjects.parse("ice", MetadataObject.Type.CATALOG)));
    assertTableRelations(caseSensitive, table);
    catalog
        .asSchemas()
        .loadSchema("schema")
        .supportsTags()
        .associateTags(new TagValue[] {TagValue.of("department", "risk")}, new TagValue[0]);
    assertTableRelations(finance, table);
    assertTableRelations(
        risk,
        table,
        DomainRelation.inherited(MetadataObjects.parse("ice.schema", MetadataObject.Type.SCHEMA)));

    // Multiple direct values match independently and case-sensitively; lower assignments stay
    // masked.
    table
        .supportsTags()
        .associateTags(
            new TagValue[] {
              TagValue.of("department", "finance"), TagValue.of("department", "Finance")
            },
            new TagValue[0]);
    assertTableRelations(finance, table, DomainRelation.direct());
    assertTableRelations(caseSensitive, table, DomainRelation.direct());
    assertTableRelations(risk, table);
    table
        .supportsTags()
        .associateTags(new TagValue[0], new TagValue[] {TagValue.of("department", "finance")});
    assertTableRelations(finance, table);
    assertTableRelations(caseSensitive, table, DomainRelation.direct());
    assertTableRelations(risk, table);

    // Removing the nearest assignment reveals schema, then catalog, then no membership.
    table
        .supportsTags()
        .associateTags(new TagValue[0], new TagValue[] {TagValue.of("department", "Finance")});
    assertTableRelations(
        risk,
        table,
        DomainRelation.inherited(MetadataObjects.parse("ice.schema", MetadataObject.Type.SCHEMA)));
    catalog
        .asSchemas()
        .loadSchema("schema")
        .supportsTags()
        .associateTags(new TagValue[0], new TagValue[] {TagValue.of("department", "risk")});
    assertTableRelations(
        finance,
        table,
        DomainRelation.inherited(MetadataObjects.parse("ice", MetadataObject.Type.CATALOG)));
    catalog
        .supportsTags()
        .associateTags(new TagValue[0], new TagValue[] {TagValue.of("department", "finance")});
    assertTableRelations(finance, table);
    assertTableRelations(risk, table);
    assertTableRelations(caseSensitive, table);

    // A valueless backing binding matches both valued and valueless effective assignments.
    metalake.createTag("any-value", null, Collections.emptyMap());
    Domain any = metalake.createDomain("Any", null, null, TagValue.noValue("any-value"), false);
    table
        .supportsTags()
        .associateTags(new TagValue[] {TagValue.of("any-value", "arbitrary")}, new TagValue[0]);
    assertTableRelations(any, table, DomainRelation.direct());
    table
        .supportsTags()
        .associateTags(
            new TagValue[] {TagValue.noValue("any-value")},
            new TagValue[] {TagValue.of("any-value", "arbitrary")});
    assertTableRelations(any, table, DomainRelation.direct());
    assertThrows(
        BackingTagAlreadyBoundException.class,
        () ->
            metalake.createDomain(
                "Mixed", null, null, TagValue.of("any-value", "arbitrary"), false));
  }

  @Test
  void testMultipleSubdomainRelationsAndBackingTagIdentity() {
    Catalog catalog = createRelationalCatalog();
    Table table = createTable(catalog);
    Domain parent = create("Parent");
    Domain first =
        parent.createSubdomain("First/child.A", null, null, TagValue.noValue("first-tag"), true);
    Domain second =
        parent.createSubdomain("Second,child.B", null, null, TagValue.noValue("second-tag"), true);
    catalog.supportsTags().associateTags(new String[] {"first-tag"}, new String[0]);
    catalog
        .asSchemas()
        .loadSchema("schema")
        .supportsTags()
        .associateTags(new String[] {"second-tag"}, new String[0]);
    table
        .supportsTags()
        .associateTags(new String[] {parent.backingTag().orElseThrow().name()}, new String[0]);

    // The member appears once but retains direct membership and both independent roll-up paths.
    assertTableRelations(
        parent,
        table,
        DomainRelation.direct(),
        DomainRelation.viaSubdomain(first.name()),
        DomainRelation.viaSubdomain(second.name()));
    assertTableRelations(
        first,
        table,
        DomainRelation.inherited(MetadataObjects.parse("ice", MetadataObject.Type.CATALOG)));
    assertTableRelations(
        second,
        table,
        DomainRelation.inherited(MetadataObjects.parse("ice.schema", MetadataObject.Type.SCHEMA)));
    DomainMember[] selected =
        parent.listMembers(
            DomainMemberFilter.builder()
                .withDomains(first.name(), second.name())
                .withTypes(MetadataObject.Type.TABLE)
                .withNameContains("ORDERS")
                .build());
    assertEquals(1, selected.length);
    assertEquals(3, selected[0].relations().length);
    assertEquals(
        0,
        parent.listMembers(
                DomainMemberFilter.builder()
                    .withDomains(first.name())
                    .withTypes(MetadataObject.Type.VIEW)
                    .build())
            .length);

    // Child scopes do not include their siblings; detaching changes only the parent's roll-up.
    assertEquals(
        0,
        first.listMembers(DomainMemberFilter.builder().withDomains(second.name()).build()).length);
    table
        .supportsTags()
        .associateTags(new String[0], new String[] {parent.backingTag().orElseThrow().name()});
    assertTableRelations(
        parent,
        table,
        DomainRelation.viaSubdomain(first.name()),
        DomainRelation.viaSubdomain(second.name()));
    second.detachFromParent();
    assertTableRelations(parent, table, DomainRelation.viaSubdomain(first.name()));
    first = metalake.alterDomain(first.name(), DomainChange.rename("Renamed/child.A"));
    assertTableRelations(parent, table, DomainRelation.viaSubdomain(first.name()));
    parent = metalake.alterDomain(parent.name(), DomainChange.rename("Renamed/parent.P"));
    assertEquals(parent.name(), metalake.getDomain(first.name()).parent().orElseThrow());
    assertTableRelations(parent, table, DomainRelation.viaSubdomain(first.name()));

    // Renaming a tag preserves its identity; deleting and recreating its name must not rebind it.
    metalake.alterTag("first-tag", TagChange.rename("renamed-first-tag"));
    assertEquals(
        "renamed-first-tag", metalake.getDomain(first.name()).backingTag().orElseThrow().name());
    assertTableRelations(parent, table, DomainRelation.viaSubdomain(first.name()));
    metalake.deleteTag("renamed-first-tag");
    assertTrue(metalake.getDomain(first.name()).backingTag().isEmpty());
    assertTableRelations(parent, table);
    metalake.createTag("renamed-first-tag", null, Collections.emptyMap());
    catalog.supportsTags().associateTags(new String[] {"renamed-first-tag"}, new String[0]);
    assertTableRelations(first, table);
    assertTableRelations(parent, table);
    first.updateBackingTag(TagValue.noValue("renamed-first-tag"));
    assertTableRelations(
        first,
        table,
        DomainRelation.inherited(MetadataObjects.parse("ice", MetadataObject.Type.CATALOG)));
    assertTableRelations(parent, table, DomainRelation.viaSubdomain(first.name()));
    first.updateBackingTag(null);
    assertTableRelations(parent, table);
  }

  @Test
  void testInvalidHierarchyAtomicCreationAndIdempotence() {
    Domain parent = create("Parent");
    Domain other = create("Other");
    Domain child = parent.createSubdomain("Child", null, null, TagValue.noValue("child-tag"), true);

    metalake.createTag(
        "existing-valued-tag",
        null,
        Collections.emptyMap(),
        TagValueConstraint.ofAllowedValues("finance"));
    Domain bound =
        parent.createSubdomain(
            "Bound",
            "existing tag",
            Map.of("key", "value"),
            TagValue.of("existing-valued-tag", "finance"),
            false);
    assertEquals(parent.name(), bound.parent().orElseThrow());
    assertEquals(TagValue.of("existing-valued-tag", "finance"), bound.backingTag().orElseThrow());
    assertEquals(Map.of("key", "value"), bound.properties());
    assertThrows(
        DomainAlreadyExistsException.class,
        () -> metalake.alterDomain(other.name(), DomainChange.rename(parent.name())));
    assertEquals(other.name(), metalake.getDomain(other.name()).name());

    // Hierarchy validation must leave neither a new domain nor its requested backing tag behind.
    assertThrows(
        IllegalArgumentException.class,
        () ->
            child.createSubdomain(
                "Grandchild", null, null, TagValue.noValue("grandchild-tag"), true));
    assertThrows(NoSuchDomainException.class, () -> metalake.getDomain("Grandchild"));
    assertThrows(NoSuchTagException.class, () -> metalake.getTag("grandchild-tag"));
    assertThrows(
        DomainAlreadyExistsException.class,
        () ->
            metalake.createDomain(parent.name(), null, null, TagValue.noValue("unused-tag"), true));
    assertThrows(NoSuchTagException.class, () -> metalake.getTag("unused-tag"));
    assertThrows(IllegalArgumentException.class, () -> child.attachToParent(child.name()));
    assertThrows(IllegalArgumentException.class, () -> other.attachToParent(child.name()));
    assertThrows(IllegalArgumentException.class, () -> parent.attachToParent(other.name()));
    assertEquals(parent.name(), child.attachToParent(parent.name()).parent().orElseThrow());
    assertEquals(
        child.backingTag(), child.updateBackingTag(child.backingTag().orElseThrow()).backingTag());

    // Repeating visibility and detach operations is safe and does not mutate an older snapshot.
    assertEquals(Domain.Status.PUBLISHED, parent.publish().publish().status());
    assertEquals(Domain.Status.PUBLISHED, child.publish().publish().status());
    assertEquals(Domain.Status.DRAFT, child.unpublish().unpublish().status());
    assertEquals(Domain.Status.DRAFT, parent.unpublish().unpublish().status());
    assertTrue(parent.detachFromParent().parent().isEmpty());
    assertTrue(child.detachFromParent().detachFromParent().parent().isEmpty());
    assertEquals(parent.name(), child.parent().orElseThrow());
    assertNotNull(child.auditInfo().creator());
    assertNotNull(child.auditInfo().createTime());
  }

  @Test
  void testMissingDomainEndpointsAndInvalidBindingsLeaveNoChanges() {
    Domain removed = create("removed");
    metalake.deleteDomain(removed.name());
    assertThrows(
        NoSuchDomainException.class,
        () -> metalake.alterDomain(removed.name(), DomainChange.updateComment("missing")));
    assertThrows(NoSuchDomainException.class, removed::publish);
    assertThrows(NoSuchDomainException.class, removed::unpublish);
    assertThrows(NoSuchDomainException.class, removed::detachFromParent);
    assertThrows(NoSuchDomainException.class, () -> removed.attachToParent("missing-parent"));
    assertThrows(NoSuchDomainException.class, () -> removed.updateBackingTag(null));
    assertThrows(NoSuchDomainException.class, removed::listSubdomains);
    assertThrows(NoSuchDomainException.class, removed::listSubdomainsInfo);
    assertThrows(
        NoSuchDomainException.class,
        () -> removed.listMembers(DomainMemberFilter.builder().build()));
    assertThrows(
        NoSuchDomainException.class,
        () -> removed.createSubdomain("orphan", null, null, TagValue.noValue("orphan-tag"), true));
    assertThrows(NoSuchTagException.class, () -> metalake.getTag("orphan-tag"));
    assertFalse(metalake.deleteDomain(removed.name()));

    Domain domain = create("binding-errors");
    assertThrows(NoSuchDomainException.class, () -> domain.attachToParent("missing-parent"));
    assertTrue(metalake.getDomain(domain.name()).parent().isEmpty());
    assertThrows(
        NoSuchTagException.class, () -> domain.updateBackingTag(TagValue.noValue("absent-tag")));
    assertEquals(domain.backingTag(), metalake.getDomain(domain.name()).backingTag());

    // Failed creation must neither adopt an existing tag nor create a partial domain.
    assertThrows(
        NoSuchTagException.class,
        () ->
            metalake.createDomain(
                "missing-binding", null, null, TagValue.noValue("absent-tag"), false));
    assertThrows(
        TagAlreadyExistsException.class,
        () ->
            metalake.createDomain(
                "existing-binding", null, null, domain.backingTag().orElseThrow(), true));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            metalake.createDomain(
                "valued-new-tag", null, null, TagValue.of("invalid-new-tag", "value"), true));
    for (String name : List.of("missing-binding", "existing-binding", "valued-new-tag", "orphan")) {
      assertThrows(NoSuchDomainException.class, () -> metalake.getDomain(name));
    }
    assertThrows(NoSuchTagException.class, () -> metalake.getTag("invalid-new-tag"));
    assertNotNull(metalake.getTag(domain.backingTag().orElseThrow().name()));
  }

  @Test
  void testUnicodeNamesAndBackingTagValueLengthBoundaries() {
    Table table = createTable(createRelationalCatalog());
    String[] values = {"x".repeat(256), "😀".repeat(128)};
    for (int index = 0; index < values.length; index++) {
      String tagName = "unicode-boundary-" + index;
      metalake.createTag(tagName, null, Collections.emptyMap());
      String name = "财务/😀,区域" + index;
      TagValue binding = TagValue.of(tagName, values[index]);
      Domain domain =
          metalake.createDomain(
              name, "Unicode comment 中文😀", Map.of("region", "亚洲😀"), binding, false);
      assertEquals(name, metalake.getDomain(name).name());
      assertEquals(binding, metalake.getDomain(name).backingTag().orElseThrow());
      assertEquals("亚洲😀", metalake.getDomain(name).properties().get("region"));

      // Boundary values must survive JSON, storage and effective-tag membership resolution.
      table.supportsTags().associateTags(new TagValue[] {binding}, new TagValue[0]);
      assertTableRelations(domain, table, DomainRelation.direct());
      String renamed = "税务/😀,地区" + index;
      Domain changed = metalake.alterDomain(name, DomainChange.rename(renamed));
      assertEquals(renamed, metalake.getDomain(renamed).name());
      assertEquals(binding, changed.backingTag().orElseThrow());
      assertTableRelations(changed, table, DomainRelation.direct());
      assertThrows(NoSuchDomainException.class, () -> metalake.getDomain(name));

      // Both ASCII and supplementary characters use the same 256 UTF-16-unit limit.
      String invalid = values[index] + "x";
      assertThrows(
          IllegalArgumentException.class,
          () -> changed.updateBackingTag(TagValue.of(tagName, invalid)));
      assertEquals(binding, metalake.getDomain(renamed).backingTag().orElseThrow());
      String rejectedName = "rejected-boundary-" + index;
      assertThrows(
          IllegalArgumentException.class,
          () ->
              metalake.createDomain(
                  rejectedName, null, null, TagValue.of(tagName, invalid), false));
      assertThrows(NoSuchDomainException.class, () -> metalake.getDomain(rejectedName));
      assertTableRelations(changed, table, DomainRelation.direct());
    }
  }

  @Test
  void testRealResponseValueEqualityAndSnapshotProtection() {
    Table table = createTable(createRelationalCatalog());
    Domain created =
        metalake.createDomain(
            "value-contract",
            "original",
            Map.of("key", "value"),
            TagValue.noValue("value-contract-tag"),
            true);
    Domain loaded = metalake.getDomain(created.name());
    Domain listed = metalake.listDomainsInfo()[0];
    assertEquals(created, loaded);
    assertEquals(loaded, listed);
    assertEquals(created.hashCode(), loaded.hashCode());
    assertTrue(Set.of(created).contains(listed));
    assertThrows(
        UnsupportedOperationException.class, () -> loaded.properties().put("key", "changed"));
    assertEquals(Map.of("key", "value"), loaded.properties());

    table.supportsTags().associateTags(new String[] {"value-contract-tag"}, new String[0]);
    DomainMemberFilter filter =
        DomainMemberFilter.builder().withTypes(MetadataObject.Type.TABLE).build();
    DomainMember member = loaded.listMembers(filter)[0];
    DomainMember reloaded = loaded.listMembers(filter)[0];
    MetadataObject object = MetadataObjects.parse("ice.schema.orders", MetadataObject.Type.TABLE);
    assertEquals(member, reloaded);
    assertEquals(member.hashCode(), reloaded.hashCode());
    assertTrue(Set.of(member).contains(reloaded));
    assertEquals(object, member.object());
    assertEquals(object.hashCode(), member.object().hashCode());
    assertEquals("ice.schema", member.object().parent());

    // Returned arrays cannot change the member or the domain snapshot used as a set key.
    int memberHash = member.hashCode();
    member.relations()[0] = null;
    assertArrayEquals(new DomainRelation[] {DomainRelation.direct()}, member.relations());
    assertEquals(memberHash, member.hashCode());
    Domain reverse = table.supportsDomains().listDomainsInfo()[0];
    Domain reverseReloaded = table.supportsDomains().listDomainsInfo()[0];
    assertEquals(reverse, reverseReloaded);
    int reverseHash = reverse.hashCode();
    reverse.relations()[0] = null;
    assertArrayEquals(new DomainRelation[] {DomainRelation.direct()}, reverse.relations());
    assertEquals(reverseHash, reverse.hashCode());
    assertTrue(Set.of(reverse).contains(reverseReloaded));

    Domain changed =
        metalake.alterDomain(
            loaded.name(),
            DomainChange.updateComment("changed"),
            DomainChange.setProperty("key", "changed"));
    assertFalse(loaded.equals(changed));
    assertEquals("original", loaded.comment());
    assertEquals(Map.of("key", "value"), loaded.properties());
    assertEquals(created, loaded);
  }

  private Table createTable(Catalog catalog) {
    return catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of("schema", "orders"),
            new Column[] {Column.of("id", Types.LongType.get())},
            "",
            Collections.emptyMap());
  }

  private void assertTableRelations(Domain domain, Table table, DomainRelation... expected) {
    DomainMember[] members =
        domain.listMembers(
            DomainMemberFilter.builder()
                .withTypes(MetadataObject.Type.TABLE)
                .withNameContains("ice.schema.orders")
                .build());
    Domain[] reverse =
        Arrays.stream(table.supportsDomains().listDomainsInfo())
            .filter(candidate -> candidate.name().equals(domain.name()))
            .toArray(Domain[]::new);
    if (expected.length == 0) {
      assertEquals(0, members.length);
      assertEquals(0, reverse.length);
      return;
    }
    assertEquals(1, members.length);
    assertEquals("ice.schema.orders", members[0].object().fullName());
    assertEquals(MetadataObject.Type.TABLE, members[0].object().type());
    assertEquals(expected.length, members[0].relations().length);
    assertEquals(Set.of(expected), Set.of(members[0].relations()));
    assertEquals(1, reverse.length);
    assertEquals(expected.length, reverse[0].relations().length);
    assertEquals(Set.of(expected), Set.of(reverse[0].relations()));
    assertEquals(0, domain.relations().length);
  }

  private Domain create(String name) {
    return metalake.createDomain(
        name, null, null, TagValue.noValue(name.replace(".", "_") + " tag"), true);
  }

  private void forward(HttpExchange exchange, HttpClient upstream) throws IOException {
    try {
      // Forward the encoded path and repeated query parameters unchanged.
      HttpRequest.Builder request =
          HttpRequest.newBuilder(URI.create(serverUri + exchange.getRequestURI().toASCIIString()));
      for (String header : List.of("Authorization", "Content-Type", "Accept")) {
        String value = exchange.getRequestHeaders().getFirst(header);
        if (value != null) {
          request.header(header, value);
        }
      }
      request.method(
          exchange.getRequestMethod(),
          HttpRequest.BodyPublishers.ofByteArray(exchange.getRequestBody().readAllBytes()));

      HttpResponse<byte[]> response =
          upstream.send(request.build(), HttpResponse.BodyHandlers.ofByteArray());
      response
          .headers()
          .firstValue("Content-Type")
          .ifPresent(value -> exchange.getResponseHeaders().set("Content-Type", value));
      exchange.sendResponseHeaders(response.statusCode(), response.body().length);
      exchange.getResponseBody().write(response.body());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      exchange.sendResponseHeaders(502, -1);
    } finally {
      exchange.close();
    }
  }
}
