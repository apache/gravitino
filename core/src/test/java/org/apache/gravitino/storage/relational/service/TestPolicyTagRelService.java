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
package org.apache.gravitino.storage.relational.service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.RelationEdgeTarget;
import org.apache.gravitino.RelationUpdate;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.PolicyMetaMapper;
import org.apache.gravitino.storage.relational.mapper.PolicyTagRelMapper;
import org.apache.gravitino.storage.relational.mapper.TagMetaMapper;
import org.apache.gravitino.storage.relational.po.PolicyTagRelPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

/** Tests policy-to-tag relation persistence and lifecycle behavior. */
public class TestPolicyTagRelService extends TestJDBCBackend {

  private static final String METALAKE = "policy_tag_relation_metalake";
  private static final String FINANCE_SELECTOR = "{\"type\":\"TAG_VALUE\",\"value\":\"finance\"}";
  private static final String RISK_SELECTOR = "{\"type\":\"TAG_VALUE\",\"value\":\"risk\"}";

  @TestTemplate
  public void testSelectorUpsertBidirectionalReadAndIdempotentDelete() throws IOException {
    createAndInsertMakeLake(METALAKE);
    TagEntity tag =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("domain")
            .withNamespace(NamespaceUtil.ofTag(METALAKE))
            .withProperties(Collections.emptyMap())
            .withAuditInfo(AUDIT_INFO)
            .build();
    PolicyEntity policy =
        createPolicy(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofPolicy(METALAKE),
            "retention",
            AUDIT_INFO);
    backend.insert(tag, false);
    backend.insert(policy, false);

    RelationEdgeTarget financeTarget =
        RelationEdgeTarget.of(policy.nameIdentifier(), Entity.EntityType.POLICY, FINANCE_SELECTOR);
    RelationUpdate financeUpdate =
        RelationUpdate.of(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            tag.nameIdentifier(),
            Entity.EntityType.TAG,
            new RelationEdgeTarget[] {financeTarget},
            new RelationEdgeTarget[0]);
    backend.updateEntityRelations(financeUpdate);
    backend.updateEntityRelations(financeUpdate);

    List<RelationalEntity<?>> byTag =
        backend.batchListEntitiesByRelation(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            Collections.singletonList(tag.nameIdentifier()),
            Entity.EntityType.TAG);
    Assertions.assertEquals(1, byTag.size());
    Assertions.assertEquals(SupportsRelationOperations.Type.POLICY_TAG_REL, byTag.get(0).type());
    Assertions.assertEquals(tag.nameIdentifier(), byTag.get(0).source());
    Assertions.assertEquals(Entity.EntityType.TAG, byTag.get(0).sourceType());
    Assertions.assertEquals(policy, byTag.get(0).targetEntity());
    Assertions.assertEquals(FINANCE_SELECTOR, byTag.get(0).relationValue().orElse(null));
    Assertions.assertEquals(
        policy,
        backend.getEntityByRelation(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            tag.nameIdentifier(),
            Entity.EntityType.TAG,
            policy.nameIdentifier()));

    RelationEdgeTarget riskTarget =
        RelationEdgeTarget.of(policy.nameIdentifier(), Entity.EntityType.POLICY, RISK_SELECTOR);
    backend.updateEntityRelations(
        RelationUpdate.of(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            tag.nameIdentifier(),
            Entity.EntityType.TAG,
            new RelationEdgeTarget[] {riskTarget},
            new RelationEdgeTarget[0]));
    List<RelationalEntity<?>> byPolicy =
        backend.batchListEntitiesByRelation(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            Collections.singletonList(policy.nameIdentifier()),
            Entity.EntityType.POLICY);
    Assertions.assertEquals(1, byPolicy.size());
    Assertions.assertEquals(policy.nameIdentifier(), byPolicy.get(0).source());
    Assertions.assertEquals(Entity.EntityType.POLICY, byPolicy.get(0).sourceType());
    Assertions.assertEquals(tag, byPolicy.get(0).targetEntity());
    Assertions.assertEquals(RISK_SELECTOR, byPolicy.get(0).relationValue().orElse(null));

    RelationUpdate removeUpdate =
        RelationUpdate.of(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            tag.nameIdentifier(),
            Entity.EntityType.TAG,
            new RelationEdgeTarget[0],
            new RelationEdgeTarget[] {riskTarget});
    backend.updateEntityRelations(removeUpdate);
    backend.updateEntityRelations(removeUpdate);
    Assertions.assertTrue(
        backend
            .batchListEntitiesByRelation(
                SupportsRelationOperations.Type.POLICY_TAG_REL,
                Collections.singletonList(NameIdentifierUtil.ofTag(METALAKE, tag.name())),
                Entity.EntityType.TAG)
            .isEmpty());
  }

  @TestTemplate
  public void testBatchListRelationsByMultipleAnchors() throws IOException {
    createAndInsertMakeLake(METALAKE);
    TagEntity firstTag = createAssociation(METALAKE, "domain_a", "retention_a");
    TagEntity secondTag = createAssociation(METALAKE, "domain_b", "retention_b");
    String otherMetalake = METALAKE + "_other";
    createAndInsertMakeLake(otherMetalake);
    createAssociation(otherMetalake, "domain_a", "retention_a");

    List<RelationalEntity<?>> byTags =
        backend.batchListEntitiesByRelation(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            Arrays.asList(
                firstTag.nameIdentifier(),
                secondTag.nameIdentifier(),
                NameIdentifierUtil.ofTag(METALAKE, "missing_tag")),
            Entity.EntityType.TAG);
    Assertions.assertEquals(2, byTags.size());
    Assertions.assertEquals(
        Set.of("domain_a", "domain_b"),
        byTags.stream().map(relation -> relation.source().name()).collect(Collectors.toSet()));

    List<RelationalEntity<?>> byPolicies =
        backend.batchListEntitiesByRelation(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            Arrays.asList(
                NameIdentifierUtil.ofPolicy(METALAKE, "retention_a"),
                NameIdentifierUtil.ofPolicy(METALAKE, "retention_b"),
                NameIdentifierUtil.ofPolicy(METALAKE, "missing_policy")),
            Entity.EntityType.POLICY);
    Assertions.assertEquals(2, byPolicies.size());
    Assertions.assertEquals(
        Set.of("retention_a", "retention_b"),
        byPolicies.stream().map(relation -> relation.source().name()).collect(Collectors.toSet()));
  }

  @TestTemplate
  public void testUpdateRelationsRollsBackOnFailure() throws IOException {
    createAndInsertMakeLake(METALAKE);
    TagEntity tag = createAssociation(METALAKE, "domain", "retention");
    RelationEdgeTarget existingTarget =
        RelationEdgeTarget.of(
            NameIdentifierUtil.ofPolicy(METALAKE, "retention"), Entity.EntityType.POLICY, null);
    RelationEdgeTarget missingTarget =
        RelationEdgeTarget.of(
            NameIdentifierUtil.ofPolicy(METALAKE, "missing_policy"),
            Entity.EntityType.POLICY,
            null);

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            backend.updateEntityRelations(
                RelationUpdate.of(
                    SupportsRelationOperations.Type.POLICY_TAG_REL,
                    tag.nameIdentifier(),
                    Entity.EntityType.TAG,
                    new RelationEdgeTarget[] {missingTarget},
                    new RelationEdgeTarget[] {existingTarget})));

    List<RelationalEntity<?>> relations =
        backend.batchListEntitiesByRelation(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            Collections.singletonList(tag.nameIdentifier()),
            Entity.EntityType.TAG);
    Assertions.assertEquals(1, relations.size());
    Assertions.assertEquals("retention", relations.get(0).targetEntity().name());
  }

  @TestTemplate
  public void testEntityDeletesCascadePolicyTagRelations() throws IOException {
    createAndInsertMakeLake(METALAKE);
    TagEntity policyDeletedTag =
        createAssociation(METALAKE, "policy_deleted_tag", "deleted_policy");
    backend.delete(
        NameIdentifierUtil.ofPolicy(METALAKE, "deleted_policy"), Entity.EntityType.POLICY, false);
    Assertions.assertTrue(
        backend
            .batchListEntitiesByRelation(
                SupportsRelationOperations.Type.POLICY_TAG_REL,
                Collections.singletonList(policyDeletedTag.nameIdentifier()),
                Entity.EntityType.TAG)
            .isEmpty());

    TagEntity deletedTag = createAssociation(METALAKE, "deleted_tag", "surviving_policy");
    backend.delete(deletedTag.nameIdentifier(), Entity.EntityType.TAG, false);
    Assertions.assertTrue(
        backend
            .batchListEntitiesByRelation(
                SupportsRelationOperations.Type.POLICY_TAG_REL,
                Collections.singletonList(
                    NameIdentifierUtil.ofPolicy(METALAKE, "surviving_policy")),
                Entity.EntityType.POLICY)
            .isEmpty());
  }

  @TestTemplate
  public void testMetalakeCascadeDoesNotDeleteRelationsFromOtherMetalakes() throws IOException {
    BaseMetalake deletedMetalake = createAndInsertMakeLake(METALAKE);
    createAssociation(METALAKE, "deleted_domain", "deleted_retention");
    String survivingMetalake = METALAKE + "_surviving";
    createAndInsertMakeLake(survivingMetalake);
    TagEntity survivingTag =
        createAssociation(survivingMetalake, "surviving_domain", "surviving_retention");

    backend.delete(deletedMetalake.nameIdentifier(), Entity.EntityType.METALAKE, true);

    List<RelationalEntity<?>> survivingRelations =
        backend.batchListEntitiesByRelation(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            Collections.singletonList(survivingTag.nameIdentifier()),
            Entity.EntityType.TAG);
    Assertions.assertEquals(1, survivingRelations.size());
    Assertions.assertEquals("surviving_retention", survivingRelations.get(0).targetEntity().name());
  }

  @TestTemplate
  public void testConcurrentIdenticalAddsAreIdempotent() throws Exception {
    createAndInsertMakeLake(METALAKE);
    RelationEndpoints endpoints = createEndpoints(METALAKE, "domain", "retention");
    RelationUpdate add = relationUpdate(endpoints, null, true);

    runConcurrently(() -> updateRelationsUnchecked(add), () -> updateRelationsUnchecked(add));

    PolicyTagRelPO relation = getRelation(endpoints);
    Assertions.assertNotNull(relation);
    Assertions.assertEquals(1L, relation.getCurrentVersion());
    Assertions.assertEquals(
        1L,
        queryForLong(
            "SELECT COUNT(*) FROM policy_tag_relation_meta WHERE policy_id = "
                + endpoints.policy.id()
                + " AND tag_id = "
                + endpoints.tag.id()
                + " AND deleted_at = 0"));
  }

  @TestTemplate
  public void testConcurrentSelectorUpdatesAdvanceVersionAndAudit() throws Exception {
    createAndInsertMakeLake(METALAKE);
    RelationEndpoints endpoints = createEndpoints(METALAKE, "domain", "retention");
    backend.updateEntityRelations(relationUpdate(endpoints, null, true));

    runConcurrently(
        () -> updateRelationsUnchecked(relationUpdate(endpoints, FINANCE_SELECTOR, true)),
        () -> updateRelationsUnchecked(relationUpdate(endpoints, RISK_SELECTOR, true)));

    PolicyTagRelPO relation = getRelation(endpoints);
    Assertions.assertNotNull(relation);
    Assertions.assertEquals(3L, relation.getCurrentVersion());
    Assertions.assertEquals(3L, relation.getLastVersion());
    Assertions.assertTrue(Set.of(FINANCE_SELECTOR, RISK_SELECTOR).contains(relation.getSelector()));
    AuditInfo auditInfo =
        JsonUtils.anyFieldMapper().readValue(relation.getAuditInfo(), AuditInfo.class);
    Assertions.assertNotNull(auditInfo.creator());
    Assertions.assertNotNull(auditInfo.createTime());
    Assertions.assertNotNull(auditInfo.lastModifier());
    Assertions.assertNotNull(auditInfo.lastModifiedTime());
  }

  @TestTemplate
  public void testStaleRelationUpdatesAffectNoRows() throws Exception {
    createAndInsertMakeLake(METALAKE);
    RelationEndpoints endpoints = createEndpoints(METALAKE, "domain", "retention");
    backend.updateEntityRelations(relationUpdate(endpoints, null, true));
    PolicyTagRelPO observed = getRelation(endpoints);
    PolicyTagRelPO replacement = copyWithSelectorAndVersion(observed, FINANCE_SELECTOR, 2L);

    Assertions.assertEquals(
        Integer.valueOf(1),
        SessionUtils.doWithCommitAndFetchResult(
            PolicyTagRelMapper.class, mapper -> mapper.updateSelector(replacement, observed)));
    Assertions.assertEquals(
        Integer.valueOf(0),
        SessionUtils.doWithCommitAndFetchResult(
            PolicyTagRelMapper.class, mapper -> mapper.updateSelector(replacement, observed)));
    Assertions.assertEquals(
        Integer.valueOf(0),
        SessionUtils.doWithCommitAndFetchResult(
            PolicyTagRelMapper.class, mapper -> mapper.softDeleteByPair(observed)));

    PolicyTagRelPO current = getRelation(endpoints);
    Assertions.assertEquals(
        Integer.valueOf(1),
        SessionUtils.doWithCommitAndFetchResult(
            PolicyTagRelMapper.class, mapper -> mapper.softDeleteByPair(current)));
  }

  @TestTemplate
  public void testDeleteReAddDeleteUsesUniqueTombstones() throws Exception {
    createAndInsertMakeLake(METALAKE);
    RelationEndpoints endpoints = createEndpoints(METALAKE, "domain", "retention");
    backend.updateEntityRelations(relationUpdate(endpoints, null, true));
    backend.updateEntityRelations(relationUpdate(endpoints, null, false));
    backend.updateEntityRelations(relationUpdate(endpoints, null, true));
    backend.updateEntityRelations(relationUpdate(endpoints, null, false));

    String pairPredicate =
        "policy_id = "
            + endpoints.policy.id()
            + " AND tag_id = "
            + endpoints.tag.id()
            + " AND deleted_at > 0";
    Assertions.assertEquals(
        2L, queryForLong("SELECT COUNT(*) FROM policy_tag_relation_meta WHERE " + pairPredicate));
    Assertions.assertEquals(
        2L,
        queryForLong(
            "SELECT COUNT(DISTINCT tombstone_id) FROM policy_tag_relation_meta WHERE "
                + pairPredicate));
  }

  @TestTemplate
  public void testLegacyCleanupHonorsCutoffAndLimit() throws Exception {
    createAndInsertMakeLake(METALAKE);
    RelationEndpoints first = createEndpoints(METALAKE, "domain_a", "retention_a");
    RelationEndpoints second = createEndpoints(METALAKE, "domain_b", "retention_b");
    backend.updateEntityRelations(relationUpdate(first, null, true));
    backend.updateEntityRelations(relationUpdate(second, null, true));
    backend.updateEntityRelations(relationUpdate(first, null, false));
    backend.updateEntityRelations(relationUpdate(second, null, false));
    executeUpdate("UPDATE policy_tag_relation_meta SET deleted_at = 100 WHERE deleted_at > 0");

    Assertions.assertEquals(
        0, PolicyMetaService.getInstance().deletePolicyAndVersionMetasByLegacyTimeline(100L, 10));
    Assertions.assertEquals(
        1, PolicyMetaService.getInstance().deletePolicyAndVersionMetasByLegacyTimeline(101L, 1));
    Assertions.assertEquals(
        1L, queryForLong("SELECT COUNT(*) FROM policy_tag_relation_meta WHERE deleted_at > 0"));
    Assertions.assertEquals(
        1, PolicyMetaService.getInstance().deletePolicyAndVersionMetasByLegacyTimeline(101L, 10));
  }

  @TestTemplate
  public void testRelationIdentifiersAndTypesAreValidated() throws Exception {
    createAndInsertMakeLake(METALAKE);
    RelationEndpoints endpoints = createEndpoints(METALAKE, "domain", "retention");

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            backend.batchListEntitiesByRelation(
                SupportsRelationOperations.Type.POLICY_TAG_REL,
                Collections.singletonList(NameIdentifier.of("domain")),
                Entity.EntityType.TAG));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            backend.updateEntityRelations(
                RelationUpdate.of(
                    SupportsRelationOperations.Type.POLICY_TAG_REL,
                    endpoints.tag.nameIdentifier(),
                    Entity.EntityType.TAG,
                    new RelationEdgeTarget[] {
                      RelationEdgeTarget.of(
                          NameIdentifierUtil.ofPolicy(METALAKE + "_other", "retention"),
                          Entity.EntityType.POLICY,
                          null)
                    },
                    new RelationEdgeTarget[0])));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            backend.updateEntityRelations(
                RelationUpdate.of(
                    SupportsRelationOperations.Type.POLICY_TAG_REL,
                    endpoints.tag.nameIdentifier(),
                    Entity.EntityType.TAG,
                    new RelationEdgeTarget[] {
                      RelationEdgeTarget.of(
                          endpoints.policy.nameIdentifier(), Entity.EntityType.TAG, null)
                    },
                    new RelationEdgeTarget[0])));
  }

  @TestTemplate
  public void testEndpointDeletionWinsAgainstConcurrentRelationAdd() throws Exception {
    createAndInsertMakeLake(METALAKE);
    assertTagDeletionWins(createEndpoints(METALAKE, "deleted_tag", "policy_for_deleted_tag"));
    assertPolicyDeletionWins(createEndpoints(METALAKE, "tag_for_deleted_policy", "deleted_policy"));
  }

  private TagEntity createAssociation(String metalake, String tagName, String policyName)
      throws IOException {
    RelationEndpoints endpoints = createEndpoints(metalake, tagName, policyName);
    backend.updateEntityRelations(relationUpdate(endpoints, null, true));
    return endpoints.tag;
  }

  private RelationEndpoints createEndpoints(String metalake, String tagName, String policyName)
      throws IOException {
    TagEntity tag =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(tagName)
            .withNamespace(NamespaceUtil.ofTag(metalake))
            .withProperties(Collections.emptyMap())
            .withAuditInfo(AUDIT_INFO)
            .build();
    PolicyEntity policy =
        createPolicy(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofPolicy(metalake),
            policyName,
            AUDIT_INFO);
    backend.insert(tag, false);
    backend.insert(policy, false);
    return new RelationEndpoints(tag, policy);
  }

  private RelationUpdate relationUpdate(RelationEndpoints endpoints, String selector, boolean add) {
    RelationEdgeTarget target =
        RelationEdgeTarget.of(
            endpoints.policy.nameIdentifier(), Entity.EntityType.POLICY, selector);
    return RelationUpdate.of(
        SupportsRelationOperations.Type.POLICY_TAG_REL,
        endpoints.tag.nameIdentifier(),
        Entity.EntityType.TAG,
        add ? new RelationEdgeTarget[] {target} : new RelationEdgeTarget[0],
        add ? new RelationEdgeTarget[0] : new RelationEdgeTarget[] {target});
  }

  private PolicyTagRelPO getRelation(RelationEndpoints endpoints) {
    return SessionUtils.getWithoutCommit(
        PolicyTagRelMapper.class,
        mapper -> mapper.getByPolicyIdAndTagId(endpoints.policy.id(), endpoints.tag.id()));
  }

  private PolicyTagRelPO copyWithSelectorAndVersion(
      PolicyTagRelPO relation, String selector, long version) {
    return PolicyTagRelPO.builder()
        .withPolicyId(relation.getPolicyId())
        .withTagId(relation.getTagId())
        .withSelector(selector)
        .withAuditInfo(relation.getAuditInfo())
        .withCurrentVersion(version)
        .withLastVersion(version)
        .withDeletedAt(0L)
        .build();
  }

  private void runConcurrently(Runnable first, Runnable second) throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CyclicBarrier start = new CyclicBarrier(3);
    Callable<Void> firstTask = concurrentTask(start, first);
    Callable<Void> secondTask = concurrentTask(start, second);
    try {
      Future<Void> firstFuture = executor.submit(firstTask);
      Future<Void> secondFuture = executor.submit(secondTask);
      start.await(10, TimeUnit.SECONDS);
      firstFuture.get(10, TimeUnit.SECONDS);
      secondFuture.get(10, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }
  }

  private Callable<Void> concurrentTask(CyclicBarrier start, Runnable operation) {
    return () -> {
      start.await(10, TimeUnit.SECONDS);
      operation.run();
      return null;
    };
  }

  private void updateRelationsUnchecked(RelationUpdate update) {
    try {
      backend.updateEntityRelations(update);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void assertTagDeletionWins(RelationEndpoints endpoints) throws Exception {
    assertEndpointDeletionWins(
        endpoints,
        () ->
            SessionUtils.doWithoutCommit(
                TagMetaMapper.class,
                mapper ->
                    Assertions.assertEquals(
                        1,
                        mapper.softDeleteTagMetaByMetalakeAndTagName(
                            METALAKE, endpoints.tag.name()))),
        () ->
            SessionUtils.doWithoutCommit(
                PolicyTagRelMapper.class, mapper -> mapper.softDeleteByTagId(endpoints.tag.id())));
  }

  private void assertPolicyDeletionWins(RelationEndpoints endpoints) throws Exception {
    assertEndpointDeletionWins(
        endpoints,
        () ->
            SessionUtils.doWithoutCommit(
                PolicyMetaMapper.class,
                mapper ->
                    Assertions.assertEquals(
                        1,
                        mapper.softDeletePolicyByMetalakeAndPolicyName(
                            METALAKE, endpoints.policy.name()))),
        () ->
            SessionUtils.doWithoutCommit(
                PolicyTagRelMapper.class,
                mapper -> mapper.softDeleteByPolicyId(endpoints.policy.id())));
  }

  private void assertEndpointDeletionWins(
      RelationEndpoints endpoints, Runnable deleteEndpoint, Runnable deleteRelations)
      throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch endpointDeleted = new CountDownLatch(1);
    CountDownLatch releaseDelete = new CountDownLatch(1);
    try {
      Future<Void> deleteFuture =
          executor.submit(
              () -> {
                SessionUtils.doMultipleWithCommit(
                    () -> {
                      deleteEndpoint.run();
                      endpointDeleted.countDown();
                      await(releaseDelete);
                    },
                    deleteRelations);
                return null;
              });
      Assertions.assertTrue(endpointDeleted.await(10, TimeUnit.SECONDS));
      Future<Throwable> addFuture =
          executor.submit(
              () -> {
                try {
                  backend.updateEntityRelations(relationUpdate(endpoints, null, true));
                  return null;
                } catch (Throwable t) {
                  return t;
                }
              });

      Assertions.assertThrows(
          TimeoutException.class, () -> addFuture.get(200, TimeUnit.MILLISECONDS));
      releaseDelete.countDown();
      deleteFuture.get(10, TimeUnit.SECONDS);
      Throwable failure = addFuture.get(10, TimeUnit.SECONDS);
      Assertions.assertTrue(
          failure instanceof NoSuchEntityException,
          () -> "Expected NoSuchEntityException, but got " + failure);
    } finally {
      releaseDelete.countDown();
      executor.shutdownNow();
    }
  }

  private void await(CountDownLatch latch) {
    try {
      if (!latch.await(10, TimeUnit.SECONDS)) {
        throw new IllegalStateException("Timed out waiting for concurrent test operation");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while waiting for concurrent test operation", e);
    }
  }

  private long queryForLong(String sql) throws SQLException {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      Assertions.assertTrue(resultSet.next());
      return resultSet.getLong(1);
    }
  }

  private int executeUpdate(String sql) throws SQLException {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement()) {
      return statement.executeUpdate(sql);
    }
  }

  private static class RelationEndpoints {
    private final TagEntity tag;
    private final PolicyEntity policy;

    private RelationEndpoints(TagEntity tag, PolicyEntity policy) {
      this.tag = tag;
      this.policy = policy;
    }
  }
}
