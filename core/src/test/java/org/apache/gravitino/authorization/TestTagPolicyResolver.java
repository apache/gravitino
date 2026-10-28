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

package org.apache.gravitino.authorization;

import static org.apache.gravitino.authorization.TagPolicyTestUtils.METALAKE;
import static org.apache.gravitino.authorization.TagPolicyTestUtils.customPolicy;
import static org.apache.gravitino.authorization.TagPolicyTestUtils.policy;
import static org.apache.gravitino.authorization.TagPolicyTestUtils.tag;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.TagPolicyTestUtils.InMemoryReader;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.policy.AllValuesSelector;
import org.apache.gravitino.policy.TagValueSelector;
import org.apache.gravitino.tag.TagAssignment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests effective-tag resolution, request reuse, and read-failure handling. */
class TestTagPolicyResolver {
  private static final MetadataObject TABLE =
      MetadataObjects.of("catalog.finance", "orders", MetadataObject.Type.TABLE);
  private static final MetadataObject SCHEMA =
      MetadataObjects.of("catalog", "finance", MetadataObject.Type.SCHEMA);
  private static final MetadataObject CATALOG =
      MetadataObjects.of(null, "catalog", MetadataObject.Type.CATALOG);

  private InMemoryReader reader;
  private TagPolicyResolver resolver;
  private AuthorizationRequestContext context;
  private TagEntity gold;
  private TagEntity bronze;
  private PolicyEntity access;

  @BeforeEach
  void setUp() {
    reader = new InMemoryReader();
    resolver = new TagPolicyResolver(reader);
    context = new AuthorizationRequestContext();
    gold = tag(1L, "certified", TagAssignment.ofValues("gold"));
    bronze = tag(1L, "certified", TagAssignment.ofValues("bronze"));
    access =
        policy(
            10L,
            true,
            Arrays.asList(Privilege.Name.SELECT_TABLE, Privilege.Name.MODIFY_TABLE),
            Arrays.asList("analyst", "engineer"));
    reader.assignments.put(CATALOG, Collections.singletonList(gold));
    reader.assignments.put(TABLE, Collections.singletonList(bronze));
    reader.bind(gold, access, TagValueSelector.of("gold"));
  }

  @Test
  void testTargetOverrideSurvivesAncestorResolutionAndRepeatedChecks() {
    Assertions.assertArrayEquals(
        new String[] {"gold"},
        resolver
            .resolve(METALAKE, CATALOG, context)
            .get(0)
            .tag()
            .assignment()
            .orElseThrow()
            .values());

    List<ResolvedTagPolicy> resolved = resolver.resolve(METALAKE, TABLE, context);
    Assertions.assertArrayEquals(
        new String[] {"bronze"}, resolved.get(0).tag().assignment().orElseThrow().values());
    Assertions.assertFalse(resolved.get(0).tag().inherited().orElseThrow());
    Assertions.assertSame(resolved, resolver.resolve(METALAKE, TABLE, context));
    Assertions.assertEquals(4, reader.tagReads.get());
    Assertions.assertEquals(2, reader.policyReads.get());
  }

  @Test
  void testDifferentNamesInheritAndNearestSchemaWins() {
    reader.assignments.remove(TABLE);
    reader.assignments.put(SCHEMA, Collections.singletonList(bronze));
    TagEntity domain = tag(2L, "domain", TagAssignment.ofValues("finance"));
    reader.assignments.put(CATALOG, Arrays.asList(gold, domain));
    reader.bind(domain, access, AllValuesSelector.get());

    List<ResolvedTagPolicy> result = resolver.resolve(METALAKE, TABLE, context);
    Assertions.assertEquals(2, result.size());
    Assertions.assertArrayEquals(
        new String[] {"bronze"}, result.get(0).tag().assignment().orElseThrow().values());
    ResolvedTagPolicy match = result.get(1);
    Assertions.assertEquals(2L, match.tag().id());
    Assertions.assertTrue(match.tag().inherited().orElseThrow());
  }

  @Test
  void testNoValueAssignmentStillOverridesValuedAncestor() {
    reader.assignments.put(
        TABLE, Collections.singletonList(tag(1L, "certified", TagAssignment.noValue())));
    List<ResolvedTagPolicy> result = resolver.resolve(METALAKE, TABLE, context);
    Assertions.assertFalse(result.get(0).tag().assignment().orElseThrow().hasValues());
  }

  @Test
  void testHierarchicalSchemasParticipateInNearestWins() {
    MetadataObject nestedTable =
        MetadataObjects.of("catalog.finance:risk:daily", "orders", MetadataObject.Type.TABLE);
    MetadataObject middle =
        MetadataObjects.of("catalog", "finance:risk", MetadataObject.Type.SCHEMA);
    reader.assignments.put(SCHEMA, Collections.singletonList(gold));
    reader.assignments.put(middle, Collections.singletonList(bronze));
    List<ResolvedTagPolicy> result = resolver.resolve(METALAKE, nestedTable, context);
    Assertions.assertArrayEquals(
        new String[] {"bronze"}, result.get(0).tag().assignment().orElseThrow().values());
    Assertions.assertEquals(5, reader.tagReads.get());
  }

  @Test
  void testFreshRequestSeesChangesButCurrentRequestKeepsItsSnapshot() {
    List<ResolvedTagPolicy> before = resolver.resolve(METALAKE, TABLE, context);
    reader.assignments.put(TABLE, Collections.singletonList(gold));
    Assertions.assertSame(before, resolver.resolve(METALAKE, TABLE, context));
    List<ResolvedTagPolicy> after =
        resolver.resolve(METALAKE, TABLE, new AuthorizationRequestContext());
    Assertions.assertArrayEquals(
        new String[] {"gold"}, after.get(0).tag().assignment().orElseThrow().values());
    Assertions.assertEquals(6, reader.tagReads.get());
  }

  @Test
  void testCacheSeparatesMetalakesAndObjectTypes() {
    resolver.resolve(METALAKE, TABLE, context);
    resolver.resolve("another", TABLE, context);
    MetadataObject view = MetadataObjects.of("catalog.finance", "orders", MetadataObject.Type.VIEW);
    resolver.resolve(METALAKE, view, context);
    Assertions.assertEquals(9, reader.tagReads.get());
    Assertions.assertEquals(3, reader.policyReads.get());
  }

  @Test
  void testConcurrentChecksResolveOnlyOnce() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(4);
    try {
      List<Callable<List<ResolvedTagPolicy>>> tasks = new ArrayList<>();
      for (int i = 0; i < 20; i++) {
        tasks.add(() -> resolver.resolve(METALAKE, TABLE, context));
      }
      List<Future<List<ResolvedTagPolicy>>> results = executor.invokeAll(tasks);
      List<ResolvedTagPolicy> first = results.get(0).get();
      for (Future<List<ResolvedTagPolicy>> result : results) {
        Assertions.assertSame(first, result.get());
      }
      Assertions.assertEquals(3, reader.tagReads.get());
      Assertions.assertEquals(1, reader.policyReads.get());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void testEmptyResultIsCachedAndDoesNotLoadPolicies() {
    reader.assignments.clear();
    Assertions.assertTrue(resolver.resolve(METALAKE, TABLE, context).isEmpty());
    Assertions.assertTrue(resolver.resolve(METALAKE, TABLE, context).isEmpty());
    Assertions.assertEquals(3, reader.tagReads.get());
    Assertions.assertEquals(0, reader.policyReads.get());
  }

  @Test
  void testAbsentBindingsAndDisabledPoliciesGrantNothing() {
    reader.bindings.clear();
    Assertions.assertTrue(resolver.resolve(METALAKE, TABLE, context).isEmpty());
    reader.bind(
        gold,
        policy(11L, false, List.of(Privilege.Name.SELECT_TABLE), List.of("analyst")),
        AllValuesSelector.get());
    Assertions.assertTrue(
        resolver.resolve(METALAKE, TABLE, new AuthorizationRequestContext()).isEmpty());
  }

  @Test
  void testTagReadFailureIsRetainedOnlyForThisRequest() {
    RuntimeException failure = new UncheckedIOException(new IOException("tag storage unavailable"));
    reader.tagFailure = failure;
    Assertions.assertSame(
        failure,
        Assertions.assertThrows(
            UncheckedIOException.class, () -> resolver.resolve(METALAKE, TABLE, context)));
    reader.tagFailure = null;
    Assertions.assertSame(
        failure,
        Assertions.assertThrows(
            UncheckedIOException.class, () -> resolver.resolve(METALAKE, TABLE, context)));
    Assertions.assertEquals(1, reader.tagReads.get());
    Assertions.assertFalse(
        resolver.resolve(METALAKE, TABLE, new AuthorizationRequestContext()).isEmpty());
  }

  @Test
  void testPolicyReadFailureIsNotAnEmptyOrPartiallyCachedResult() {
    RuntimeException failure =
        new UncheckedIOException(new IOException("policy storage unavailable"));
    reader.policyFailure = failure;
    Assertions.assertSame(
        failure,
        Assertions.assertThrows(
            UncheckedIOException.class, () -> resolver.resolve(METALAKE, TABLE, context)));
    reader.policyFailure = null;
    Assertions.assertSame(
        failure,
        Assertions.assertThrows(
            UncheckedIOException.class, () -> resolver.resolve(METALAKE, TABLE, context)));
    Assertions.assertEquals(3, reader.tagReads.get());
    Assertions.assertEquals(1, reader.policyReads.get());
  }

  @Test
  void testResolvedListCannotBeModified() {
    List<ResolvedTagPolicy> result = resolver.resolve(METALAKE, TABLE, context);
    Assertions.assertThrows(UnsupportedOperationException.class, result::clear);
  }

  @Test
  void testNonAccessPoliciesAreExcluded() {
    reader.bindings.clear();
    reader.bind(bronze, customPolicy(), AllValuesSelector.get());
    Assertions.assertTrue(resolver.resolve(METALAKE, TABLE, context).isEmpty());
  }

  @Test
  void testMultiplePoliciesResolveInDeterministicOrder() {
    reader.bindings.clear();
    reader.bind(
        bronze,
        policy(20L, true, List.of(Privilege.Name.SELECT_TABLE), List.of("analyst")),
        AllValuesSelector.get());
    reader.bind(bronze, access, AllValuesSelector.get());
    ResolvedTagPolicy match = resolver.resolve(METALAKE, TABLE, context).get(0);
    Assertions.assertEquals(10L, match.policy().id());
  }

  @Test
  void testIncompleteReaderResultFailsClosedForTheRequest() {
    reader.assignments.put(TABLE, null);
    RuntimeException failure =
        Assertions.assertThrows(
            NullPointerException.class, () -> resolver.resolve(METALAKE, TABLE, context));
    reader.assignments.put(TABLE, List.of(bronze));
    Assertions.assertSame(
        failure,
        Assertions.assertThrows(
            NullPointerException.class, () -> resolver.resolve(METALAKE, TABLE, context)));
    Assertions.assertEquals(1, reader.tagReads.get());
  }
}
