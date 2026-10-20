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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.AccessControlDispatcher;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.BoundLiteralPredicate;
import org.apache.iceberg.rest.restrictions.ReadRestrictions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

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
    assertTrue(exception.getMessage().contains("Conflicting row-filter"));
  }

  @Test
  void testRejectsAllowAllAlongsideDistinctRowFilter() {
    PolicyEntity[] policies =
        new PolicyEntity[] {
          rowPolicy(1, "allow", "filter := true"),
          rowPolicy(2, "us", "filter := col(\"region\") == \"US\"")
        };

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ExperimentalIcebergReadRestrictionLoader.resolve(
                    policies, Collections.emptyMap(), SCHEMA, context("alice")));
    assertTrue(exception.getMessage().contains("Conflicting row-filter"));
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
