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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.RelationEdgeTarget;
import org.apache.gravitino.RelationUpdate;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.PolicyTagRelMapper;
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
  public void testSelectorCreateConflictBidirectionalReadAndIdempotentDelete() throws IOException {
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
    Assertions.assertThrows(
        EntityAlreadyExistsException.class, () -> backend.updateEntityRelations(financeUpdate));

    RelationEdgeTarget riskTarget =
        RelationEdgeTarget.of(policy.nameIdentifier(), Entity.EntityType.POLICY, RISK_SELECTOR);
    Assertions.assertThrows(
        EntityAlreadyExistsException.class,
        () ->
            backend.updateEntityRelations(
                RelationUpdate.of(
                    SupportsRelationOperations.Type.POLICY_TAG_REL,
                    tag.nameIdentifier(),
                    Entity.EntityType.TAG,
                    new RelationEdgeTarget[] {riskTarget},
                    new RelationEdgeTarget[0])));

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

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            backend.updateEntityRelations(
                RelationUpdate.of(
                    SupportsRelationOperations.Type.POLICY_TAG_REL,
                    tag.nameIdentifier(),
                    Entity.EntityType.TAG,
                    new RelationEdgeTarget[] {riskTarget},
                    new RelationEdgeTarget[] {financeTarget})));
    List<RelationalEntity<?>> byPolicy =
        backend.batchListEntitiesByRelation(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            Collections.singletonList(policy.nameIdentifier()),
            Entity.EntityType.POLICY);
    Assertions.assertEquals(1, byPolicy.size());
    Assertions.assertEquals(policy.nameIdentifier(), byPolicy.get(0).source());
    Assertions.assertEquals(Entity.EntityType.POLICY, byPolicy.get(0).sourceType());
    Assertions.assertEquals(tag, byPolicy.get(0).targetEntity());
    Assertions.assertEquals(FINANCE_SELECTOR, byPolicy.get(0).relationValue().orElse(null));

    RelationUpdate removeUpdate =
        RelationUpdate.of(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            tag.nameIdentifier(),
            Entity.EntityType.TAG,
            new RelationEdgeTarget[0],
            new RelationEdgeTarget[] {financeTarget});
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
  public void testDuplicateAddRollsBackAllRelationWrites() throws IOException {
    createAndInsertMakeLake(METALAKE);
    TagEntity tag = createAssociation(METALAKE, "domain", "retention");
    PolicyEntity newPolicy =
        createPolicy(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofPolicy(METALAKE),
            "new_policy",
            AUDIT_INFO);
    backend.insert(newPolicy, false);

    RelationEdgeTarget newTarget =
        RelationEdgeTarget.of(newPolicy.nameIdentifier(), Entity.EntityType.POLICY, null);
    RelationEdgeTarget existingTarget =
        RelationEdgeTarget.of(
            NameIdentifierUtil.ofPolicy(METALAKE, "retention"), Entity.EntityType.POLICY, null);
    Assertions.assertThrows(
        EntityAlreadyExistsException.class,
        () ->
            backend.updateEntityRelations(
                RelationUpdate.of(
                    SupportsRelationOperations.Type.POLICY_TAG_REL,
                    tag.nameIdentifier(),
                    Entity.EntityType.TAG,
                    new RelationEdgeTarget[] {newTarget, existingTarget},
                    new RelationEdgeTarget[0])));

    List<RelationalEntity<?>> relations =
        backend.batchListEntitiesByRelation(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            Collections.singletonList(tag.nameIdentifier()),
            Entity.EntityType.TAG);
    Assertions.assertEquals(1, relations.size());
    Assertions.assertEquals("retention", relations.get(0).targetEntity().name());
    Assertions.assertNull(
        SessionUtils.getWithoutCommit(
            PolicyTagRelMapper.class,
            mapper -> mapper.getByPolicyIdAndTagId(newPolicy.id(), tag.id())));
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
  public void testConcurrentInsertsHaveOneWinner() throws Exception {
    createAndInsertMakeLake(METALAKE);
    RelationEndpoints endpoints = createEndpoints(METALAKE, "domain", "retention");
    PolicyTagRelPO relation = newRelation(endpoints, null);
    CyclicBarrier bothObservedAbsent = new CyclicBarrier(2);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    List<Integer> affectedRows;
    try {
      Future<Integer> first = executor.submit(concurrentInsertTask(relation, bothObservedAbsent));
      Future<Integer> second = executor.submit(concurrentInsertTask(relation, bothObservedAbsent));
      affectedRows =
          Arrays.asList(first.get(20, TimeUnit.SECONDS), second.get(20, TimeUnit.SECONDS));
    } finally {
      executor.shutdownNow();
    }
    Collections.sort(affectedRows);

    Assertions.assertEquals(Arrays.asList(0, 1), affectedRows);
    PolicyTagRelPO persisted = getRelation(endpoints);
    Assertions.assertNotNull(persisted);
    Assertions.assertNotNull(persisted.getId());
    Assertions.assertEquals(1L, persisted.getCurrentVersion());
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
  public void testStaleRelationDeleteAffectsNoRows() throws Exception {
    createAndInsertMakeLake(METALAKE);
    RelationEndpoints endpoints = createEndpoints(METALAKE, "domain", "retention");
    backend.updateEntityRelations(relationUpdate(endpoints, null, true));
    PolicyTagRelPO observed = getRelation(endpoints);
    PolicyTagRelPO replacement = copyWithVersion(observed, 2L);

    Assertions.assertEquals(
        Integer.valueOf(0),
        SessionUtils.doWithCommitAndFetchResult(
            PolicyTagRelMapper.class, mapper -> mapper.softDeleteByIdAndVersion(replacement)));

    PolicyTagRelPO current = getRelation(endpoints);
    Assertions.assertEquals(
        Integer.valueOf(1),
        SessionUtils.doWithCommitAndFetchResult(
            PolicyTagRelMapper.class, mapper -> mapper.softDeleteByIdAndVersion(current)));
  }

  @TestTemplate
  public void testStaleRelationCannotDeleteRecreatedRow() throws Exception {
    createAndInsertMakeLake(METALAKE);
    RelationEndpoints endpoints = createEndpoints(METALAKE, "domain", "retention");
    backend.updateEntityRelations(relationUpdate(endpoints, null, true));
    PolicyTagRelPO stale = getRelation(endpoints);

    backend.updateEntityRelations(relationUpdate(endpoints, null, false));
    backend.updateEntityRelations(relationUpdate(endpoints, null, true));
    PolicyTagRelPO recreated = getRelation(endpoints);
    Assertions.assertNotEquals(stale.getId(), recreated.getId());

    Assertions.assertEquals(
        Integer.valueOf(0),
        SessionUtils.doWithCommitAndFetchResult(
            PolicyTagRelMapper.class, mapper -> mapper.softDeleteByIdAndVersion(stale)));

    PolicyTagRelPO current = getRelation(endpoints);
    Assertions.assertNotNull(current);
    Assertions.assertEquals(recreated.getId(), current.getId());
  }

  @TestTemplate
  public void testDeleteReAddDeleteRetainsHistory() throws Exception {
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
        0, TagMetaService.getInstance().deleteTagMetasByLegacyTimeline(100L, 10));
    Assertions.assertEquals(
        1, TagMetaService.getInstance().deleteTagMetasByLegacyTimeline(101L, 1));
    Assertions.assertEquals(
        1L, queryForLong("SELECT COUNT(*) FROM policy_tag_relation_meta WHERE deleted_at > 0"));
    Assertions.assertEquals(
        1, TagMetaService.getInstance().deleteTagMetasByLegacyTimeline(101L, 10));
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

  private PolicyTagRelPO copyWithVersion(PolicyTagRelPO relation, long version) {
    return PolicyTagRelPO.builder()
        .withId(relation.getId())
        .withPolicyId(relation.getPolicyId())
        .withTagId(relation.getTagId())
        .withSelector(relation.getSelector())
        .withAuditInfo(relation.getAuditInfo())
        .withCurrentVersion(version)
        .withLastVersion(version)
        .withDeletedAt(0L)
        .build();
  }

  private PolicyTagRelPO newRelation(RelationEndpoints endpoints, String selector) {
    return PolicyTagRelPO.builder()
        .withPolicyId(endpoints.policy.id())
        .withTagId(endpoints.tag.id())
        .withSelector(selector)
        .withAuditInfo("{}")
        .withCurrentVersion(1L)
        .withLastVersion(1L)
        .withDeletedAt(0L)
        .build();
  }

  private Callable<Integer> concurrentInsertTask(
      PolicyTagRelPO relation, CyclicBarrier bothObservedAbsent) {
    return () -> {
      try (SqlSession sqlSession =
          SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(false)) {
        PolicyTagRelMapper mapper = sqlSession.getMapper(PolicyTagRelMapper.class);
        PolicyTagRelPO observed =
            mapper.getByPolicyIdAndTagId(relation.getPolicyId(), relation.getTagId());
        bothObservedAbsent.await(10, TimeUnit.SECONDS);
        Assertions.assertNull(observed);
        int inserted = mapper.insertIfAbsent(relation);
        sqlSession.commit();
        return inserted;
      }
    };
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
