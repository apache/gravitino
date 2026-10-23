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
package org.apache.gravitino.iceberg.service.restriction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.AccessControlDispatcher;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.exceptions.IllegalNameIdentifierException;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.policy.PolicyDispatcher;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.Labels;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.BoundLiteralPredicate;
import org.apache.iceberg.rest.RemoteSigningConfig;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.restrictions.ReadRestrictions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** Tests effective policy resolution into one load-table read restriction. */
public class TestExperimentalIcebergReadRestrictionLoader {

  private static final String METALAKE = "metalake";
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "region", Types.StringType.get()),
          Types.NestedField.optional(3, "owner", Types.StringType.get()));

  @Test
  void testServiceLoaderDiscoversExperimentalImplementation() {
    assertInstanceOf(
        ExperimentalIcebergReadRestrictionLoader.class, IcebergReadRestrictionLoader.load());
  }

  @Test
  void testEnabledOnlyWhenPolicyDispatcherIsAvailable() throws Exception {
    GravitinoEnv environment = GravitinoEnv.getInstance();
    PolicyDispatcher original = environment.internalPolicyDispatcher();
    ExperimentalIcebergReadRestrictionLoader loader =
        new ExperimentalIcebergReadRestrictionLoader();
    try {
      FieldUtils.writeField(environment, "internalPolicyDispatcher", null, true);
      assertFalse(loader.enabled());

      FieldUtils.writeField(
          environment, "internalPolicyDispatcher", mock(PolicyDispatcher.class), true);
      assertTrue(loader.enabled());
    } finally {
      FieldUtils.writeField(environment, "internalPolicyDispatcher", original, true);
    }
  }

  @Test
  void testPreservesOptionalFieldsAfterSnapshotFiltering() {
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            SCHEMA, PartitionSpec.unpartitioned(), "s3://bucket/table", Collections.emptyMap());
    RemoteSigningConfig remoteSigningConfig = mock(RemoteSigningConfig.class);
    Labels labels = mock(Labels.class);
    LoadTableResponse original =
        LoadTableResponse.builder()
            .withTableMetadata(metadata)
            .withRemoteSigningConfig(remoteSigningConfig)
            .withLabels(labels)
            .build();
    LoadTableResponse filtered = LoadTableResponse.builder().withTableMetadata(metadata).build();

    LoadTableResponse result =
        new ExperimentalIcebergReadRestrictionLoader()
            .preserveSnapshotFilteredFields(original, filtered);

    assertSame(metadata, result.tableMetadata());
    assertSame(remoteSigningConfig, result.remoteSigningConfig());
    assertSame(labels, result.labels());
  }

  @Test
  void testLoadWithoutPolicyDispatcherReturnsOriginalResponse() throws Exception {
    LoadTableResponse response = response(SCHEMA);

    IcebergReadRestrictionLoader.Result result =
        load(null, TableIdentifier.of("schema", "table"), response);

    assertSame(response, result.response());
    assertFalse(result.etagSeed().isPresent());
  }

  @Test
  void testLoadWithoutApplicablePoliciesQueriesTableAndColumns() throws Exception {
    PolicyDispatcher dispatcher = mock(PolicyDispatcher.class);
    when(dispatcher.listPolicyInfosForMetadataObject(eq(METALAKE), any(MetadataObject.class)))
        .thenReturn(new PolicyEntity[0]);
    LoadTableResponse response = response(SCHEMA);

    IcebergReadRestrictionLoader.Result result =
        load(dispatcher, TableIdentifier.of("schema", "table"), response);

    assertSame(response, result.response());
    assertFalse(result.etagSeed().isPresent());
    ArgumentCaptor<MetadataObject> objects = ArgumentCaptor.forClass(MetadataObject.class);
    verify(dispatcher, times(4)).listPolicyInfosForMetadataObject(eq(METALAKE), objects.capture());
    assertEquals(MetadataObject.Type.TABLE, objects.getAllValues().get(0).type());
    for (int index = 1; index < 4; index++) {
      assertEquals(MetadataObject.Type.COLUMN, objects.getAllValues().get(index).type());
    }
  }

  @Test
  void testLoadBuildsRestrictedResponseAndSeed() throws Exception {
    PolicyDispatcher dispatcher = mock(PolicyDispatcher.class);
    when(dispatcher.listPolicyInfosForMetadataObject(eq(METALAKE), any(MetadataObject.class)))
        .thenAnswer(
            invocation -> {
              MetadataObject object = invocation.getArgument(1);
              if (object.type() == MetadataObject.Type.TABLE) {
                return new PolicyEntity[] {
                  rowPolicy(1, "regional", "filter := col(\"region\") == \"US\"")
                };
              }
              if (object.type() == MetadataObject.Type.COLUMN && "region".equals(object.name())) {
                return new PolicyEntity[] {
                  maskPolicy(2, "regional_mask", "mask := action(\"show-last-4\")")
                };
              }
              return new PolicyEntity[0];
            });
    RemoteSigningConfig remoteSigningConfig = mock(RemoteSigningConfig.class);
    Labels labels = mock(Labels.class);
    LoadTableResponse response =
        LoadTableResponse.builder()
            .withTableMetadata(response(SCHEMA).tableMetadata())
            .addAllConfig(Collections.singletonMap("test", "value"))
            .withRemoteSigningConfig(remoteSigningConfig)
            .withLabels(labels)
            .build();

    IcebergReadRestrictionLoader.Result result =
        load(dispatcher, TableIdentifier.of("schema", "table"), response);

    assertNotNull(result.response().readRestrictions().rowFilter());
    assertEquals(1, result.response().readRestrictions().columnProjections().size());
    assertEquals(2, result.response().readRestrictions().columnProjections().get(0).fieldId());
    assertTrue(result.etagSeed().isPresent());
    assertSame(response.tableMetadata(), result.response().tableMetadata());
    assertEquals(response.config(), result.response().config());
    assertSame(remoteSigningConfig, result.response().remoteSigningConfig());
    assertSame(labels, result.response().labels());
  }

  @Test
  void testLoadRejectsDottedTableNameWithPolicyDispatcher() throws Exception {
    PolicyDispatcher dispatcher = mock(PolicyDispatcher.class);

    IllegalNameIdentifierException exception =
        assertThrows(
            IllegalNameIdentifierException.class,
            () ->
                load(
                    dispatcher, TableIdentifier.of("schema", "table.with.dots"), response(SCHEMA)));

    assertTrue(exception.getMessage().contains("table.with.dots"));
  }

  @Test
  void testLoadRejectsDottedColumnNameWithPolicyDispatcher() throws Exception {
    Schema schema = new Schema(Types.NestedField.optional(1, "a.b", Types.StringType.get()));
    PolicyDispatcher dispatcher = mock(PolicyDispatcher.class);
    when(dispatcher.listPolicyInfosForMetadataObject(eq(METALAKE), any(MetadataObject.class)))
        .thenReturn(new PolicyEntity[0]);

    IllegalNameIdentifierException exception =
        assertThrows(
            IllegalNameIdentifierException.class,
            () -> load(dispatcher, TableIdentifier.of("schema", "table"), response(schema)));

    assertTrue(exception.getMessage().contains("a.b"));
  }

  @Test
  void testLoadRejectsDottedAncestorNamesWithPolicyDispatcher() throws Exception {
    PolicyDispatcher dispatcher = mock(PolicyDispatcher.class);
    TableIdentifier table = TableIdentifier.of("schema", "table");

    IllegalNameIdentifierException metalakeError =
        assertThrows(
            IllegalNameIdentifierException.class,
            () -> load(dispatcher, "metalake.with.dots", "catalog", table, response(SCHEMA)));
    IllegalNameIdentifierException catalogError =
        assertThrows(
            IllegalNameIdentifierException.class,
            () -> load(dispatcher, METALAKE, "catalog.with.dots", table, response(SCHEMA)));
    IllegalNameIdentifierException schemaError =
        assertThrows(
            IllegalNameIdentifierException.class,
            () ->
                load(
                    dispatcher,
                    METALAKE,
                    "catalog",
                    TableIdentifier.of("schema.with.dots", "table"),
                    response(SCHEMA)));

    assertTrue(metalakeError.getMessage().contains("metalake.with.dots"));
    assertTrue(catalogError.getMessage().contains("catalog.with.dots"));
    assertTrue(schemaError.getMessage().contains("schema.with.dots"));
  }

  @Test
  void testResolvesAndDeduplicatesCanonicalPolicies() {
    PolicyEntity[] rowPolicies =
        new PolicyEntity[] {
          rowPolicy(1, "first", "filter := col(\"region\") == \"US\" and col(\"id\") in [2, 1]"),
          rowPolicy(2, "second", "filter := col(\"id\") in [1, 2] and col(\"region\") == \"US\"")
        };
    Map<Integer, PolicyEntity[]> columnPolicies = new LinkedHashMap<>();
    columnPolicies.put(
        2,
        new PolicyEntity[] {
          maskPolicy(3, "mask_one", "mask := action(\"show-last-4\")"),
          maskPolicy(4, "mask_two", "mask := action(\"show-last-4\")")
        });

    ReadRestrictions restrictions =
        ExperimentalIcebergReadRestrictionLoader.resolve(
            rowPolicies, columnPolicies, SCHEMA, context("alice"));

    assertNotNull(restrictions.rowFilter());
    assertEquals(1, restrictions.columnProjections().size());
    assertEquals("show-last-4", restrictions.columnProjections().get(0).name());
    assertEquals(2, restrictions.columnProjections().get(0).fieldId());
  }

  @Test
  void testRejectsTableMaskOnIncompatibleFieldTypes() {
    PolicyEntity mask = maskPolicy(1, "string_mask", "mask := action(\"show-last-4\")");
    Map<Integer, PolicyEntity[]> policiesByField = new LinkedHashMap<>();
    for (int fieldId = 1; fieldId <= 3; fieldId++) {
      policiesByField.put(fieldId, new PolicyEntity[] {mask});
    }

    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () ->
                ExperimentalIcebergReadRestrictionLoader.resolveForLoad(
                    new PolicyEntity[0], policiesByField, SCHEMA, context("alice")));
    assertEquals(
        "Unable to resolve read restrictions for the requested table", exception.getMessage());
  }

  @Test
  void testRejectsReplaceWithNullOnRequiredField() {
    PolicyEntity mask = maskPolicy(1, "null_mask", "mask := action(\"replace-with-null\")");
    Map<Integer, PolicyEntity[]> policiesByField = new LinkedHashMap<>();
    policiesByField.put(1, new PolicyEntity[] {mask});
    policiesByField.put(2, new PolicyEntity[] {mask});

    assertThrows(
        IllegalArgumentException.class,
        () ->
            ExperimentalIcebergReadRestrictionLoader.resolve(
                new PolicyEntity[0], policiesByField, SCHEMA, context("alice")));
  }

  @Test
  void testUnselectedIncompatibleMaskDoesNotRejectTable() {
    PolicyEntity mask =
        maskPolicy(
            1,
            "conditional_mask",
            "mask := action(\"mask-to-fixed-value\") if true "
                + "else := action(\"replace-with-null\")");
    Map<Integer, PolicyEntity[]> policiesByField = new LinkedHashMap<>();
    policiesByField.put(1, new PolicyEntity[] {mask});

    ReadRestrictions restrictions =
        ExperimentalIcebergReadRestrictionLoader.resolve(
            new PolicyEntity[0], policiesByField, SCHEMA, context("alice"));

    assertEquals(1, restrictions.columnProjections().size());
    assertEquals("mask-to-fixed-value", restrictions.columnProjections().get(0).name());
    assertEquals(1, restrictions.columnProjections().get(0).fieldId());
  }

  @Test
  void testBindsSubjectForRowFilterAndMask() {
    PolicyEntity[] rowPolicies =
        new PolicyEntity[] {rowPolicy(1, "owner", "filter := col(\"owner\") == session_user()")};
    Map<Integer, PolicyEntity[]> columnPolicies = new LinkedHashMap<>();
    columnPolicies.put(
        2,
        new PolicyEntity[] {
          maskPolicy(
              2,
              "conditional_mask",
              "mask := action(\"show-last-4\") if is_group_member(\"auditors\") "
                  + "else := action(\"replace-with-null\")")
        });

    ReadRestrictions restrictions =
        ExperimentalIcebergReadRestrictionLoader.resolve(
            rowPolicies,
            columnPolicies,
            SCHEMA,
            context("alice", Collections.singletonMap("auditors", false)));

    BoundLiteralPredicate<?> rowFilter = (BoundLiteralPredicate<?>) restrictions.rowFilter();
    assertEquals(3, rowFilter.ref().fieldId());
    assertEquals("alice", rowFilter.literal().value().toString());
    assertEquals("replace-with-null", restrictions.columnProjections().get(0).name());
  }

  @Test
  void testRejectsDistinctRowFilters() {
    PolicyEntity[] policies =
        new PolicyEntity[] {
          rowPolicy(1, "us", "filter := col(\"region\") == \"US\""),
          rowPolicy(2, "eu", "filter := col(\"region\") == \"EU\"")
        };

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ExperimentalIcebergReadRestrictionLoader.resolve(
                    policies, Collections.emptyMap(), SCHEMA, context("alice")));
    assertEquals(
        "Conflicting row-filter policies apply to the requested table; only one distinct effective row-filter is supported",
        exception.getMessage());
  }

  @Test
  void testIgnoresAllowAllAlongsideRowFilter() {
    PolicyEntity[] policies =
        new PolicyEntity[] {
          rowPolicy(1, "allow", "filter := true"),
          rowPolicy(2, "us", "filter := col(\"region\") == \"US\"")
        };

    ReadRestrictions restrictions =
        ExperimentalIcebergReadRestrictionLoader.resolve(
            policies, Collections.emptyMap(), SCHEMA, context("alice"));
    assertNotNull(restrictions.rowFilter());
  }

  @Test
  void testAllAllowAllRowFiltersAreUnrestricted() {
    PolicyEntity[] policies =
        new PolicyEntity[] {
          rowPolicy(1, "first", "filter := true"), rowPolicy(2, "second", "filter := true")
        };

    ReadRestrictions restrictions =
        ExperimentalIcebergReadRestrictionLoader.resolve(
            policies, Collections.emptyMap(), SCHEMA, context("alice"));
    assertTrue(restrictions.isEmpty());
  }

  @Test
  void testLoadErrorDoesNotExposePolicyContent() {
    PolicyEntity[] policies =
        new PolicyEntity[] {
          rowPolicy(1, "secret_policy", "filter := col(\"secret_column\") == \"secret_value\"")
        };

    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () ->
                ExperimentalIcebergReadRestrictionLoader.resolveForLoad(
                    policies, Collections.emptyMap(), SCHEMA, context("alice")));
    assertEquals(
        "Unable to resolve read restrictions for the requested table", exception.getMessage());
    assertNull(exception.getCause());
  }

  @Test
  void testInconsistentIdentityDoesNotBecomeNonMembership() throws Exception {
    AccessControlDispatcher dispatcher = mock(AccessControlDispatcher.class);
    when(dispatcher.getGroup(METALAKE, "auditors")).thenReturn(mock(Group.class));

    Optional<Boolean> nonMember =
        PrincipalUtils.doAs(
            new UserPrincipal("alice"),
            () ->
                new ExperimentalIcebergReadRestrictionLoader.IdentityBindingContext(
                        "alice", METALAKE, dispatcher)
                    .groupMembership("auditors"));
    Optional<Boolean> inconsistent =
        PrincipalUtils.doAs(
            new UserPrincipal("bob"),
            () ->
                new ExperimentalIcebergReadRestrictionLoader.IdentityBindingContext(
                        "alice", METALAKE, dispatcher)
                    .groupMembership("auditors"));

    assertEquals(Optional.of(false), nonMember);
    assertFalse(inconsistent.isPresent());
  }

  @Test
  void testRejectsDistinctMasksForOneField() {
    Map<Integer, PolicyEntity[]> policies = new LinkedHashMap<>();
    policies.put(
        2,
        new PolicyEntity[] {
          maskPolicy(1, "first", "mask := action(\"show-first-4\")"),
          maskPolicy(2, "last", "mask := action(\"show-last-4\")")
        });

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ExperimentalIcebergReadRestrictionLoader.resolve(
                    new PolicyEntity[0], policies, SCHEMA, context("alice")));
    assertTrue(exception.getMessage().contains("Conflicting column-mask"));
  }

  @Test
  void testOmitsConstantTrueRowFilter() {
    ReadRestrictions restrictions =
        ExperimentalIcebergReadRestrictionLoader.resolve(
            new PolicyEntity[] {rowPolicy(1, "allow", "filter := true")},
            Collections.emptyMap(),
            SCHEMA,
            context("alice"));

    assertTrue(restrictions.isEmpty());
  }

  private static PolicyEntity rowPolicy(long id, String name, String expression) {
    return policy(id, name, Policy.BuiltInType.ROW_FILTER, PolicyContents.rowFilter(expression));
  }

  private static LoadTableResponse response(Schema schema) {
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            schema, PartitionSpec.unpartitioned(), "s3://bucket/table", Collections.emptyMap());
    return LoadTableResponse.builder().withTableMetadata(metadata).build();
  }

  private static IcebergReadRestrictionLoader.Result load(
      PolicyDispatcher dispatcher, TableIdentifier table, LoadTableResponse response)
      throws Exception {
    return load(dispatcher, METALAKE, "catalog", table, response);
  }

  private static IcebergReadRestrictionLoader.Result load(
      PolicyDispatcher dispatcher,
      String metalake,
      String catalog,
      TableIdentifier table,
      LoadTableResponse response)
      throws Exception {
    GravitinoEnv environment = GravitinoEnv.getInstance();
    PolicyDispatcher originalDispatcher = environment.internalPolicyDispatcher();
    Class<?> contextHolder =
        Class.forName(IcebergRESTServerContext.class.getName() + "$InstanceHolder");
    Object originalContext = FieldUtils.readStaticField(contextHolder, "INSTANCE", true);
    try {
      FieldUtils.writeField(environment, "internalPolicyDispatcher", dispatcher, true);
      IcebergConfigProvider configProvider = mock(IcebergConfigProvider.class);
      when(configProvider.getMetalakeName()).thenReturn(metalake);
      IcebergRESTServerContext.create(configProvider, false, true, false, null);
      IcebergRequestContext request = mock(IcebergRequestContext.class);
      when(request.catalogName()).thenReturn(catalog);
      when(request.userName()).thenReturn("alice");
      return new ExperimentalIcebergReadRestrictionLoader().load(request, table, response);
    } finally {
      FieldUtils.writeField(environment, "internalPolicyDispatcher", originalDispatcher, true);
      FieldUtils.writeStaticField(contextHolder, "INSTANCE", originalContext, true);
    }
  }

  private static PolicyEntity maskPolicy(long id, String name, String expression) {
    return policy(id, name, Policy.BuiltInType.COLUMN_MASK, PolicyContents.columnMask(expression));
  }

  private static PolicyEntity policy(
      long id, String name, Policy.BuiltInType type, PolicyContent content) {
    return PolicyEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(NamespaceUtil.ofPolicy(METALAKE))
        .withPolicyType(type)
        .withComment("")
        .withEnabled(true)
        .withContent(content)
        .withAuditInfo(AuditInfo.EMPTY)
        .build();
  }

  private static IcebergReadRestrictionConverter.BindingContext context(String user) {
    return context(user, Collections.emptyMap());
  }

  private static IcebergReadRestrictionConverter.BindingContext context(
      String user, Map<String, Boolean> memberships) {
    return new IcebergReadRestrictionConverter.BindingContext() {
      @Override
      public String sessionUser() {
        return user;
      }

      @Override
      public Optional<Boolean> groupMembership(String group) {
        return Optional.ofNullable(memberships.get(group));
      }
    };
  }
}
