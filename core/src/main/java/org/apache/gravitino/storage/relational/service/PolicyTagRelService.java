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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.RelationEdgeTarget;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.storage.relational.mapper.PolicyTagRelMapper;
import org.apache.gravitino.storage.relational.po.PolicyTagRelPO;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;

/** JDBC metadata service for policy-to-tag relations. */
public class PolicyTagRelService {

  private static final PolicyTagRelService INSTANCE = new PolicyTagRelService();

  /**
   * @return The singleton service instance.
   */
  public static PolicyTagRelService getInstance() {
    return INSTANCE;
  }

  private PolicyTagRelService() {}

  /**
   * Lists policy-to-tag relation edges from policy or tag anchors.
   *
   * @param anchors The policy or tag identifiers to query.
   * @param anchorType The entity type of every anchor.
   * @return The relation edges, including selector JSON as the relation value.
   */
  public List<RelationalEntity<?>> listRelations(
      List<NameIdentifier> anchors, Entity.EntityType anchorType) {
    if (anchors == null || anchors.isEmpty()) {
      return Collections.emptyList();
    }
    Preconditions.checkArgument(
        anchorType == Entity.EntityType.TAG || anchorType == Entity.EntityType.POLICY,
        "Policy-to-tag relations do not support anchor type %s",
        anchorType);
    validateSameMetalake(anchors);

    String metalake = anchors.get(0).namespace().level(0);
    List<String> anchorNames =
        anchors.stream().map(NameIdentifier::name).distinct().collect(Collectors.toList());
    List<PolicyTagRelPO> relations =
        SessionUtils.getWithoutCommit(
            PolicyTagRelMapper.class,
            mapper ->
                anchorType == Entity.EntityType.TAG
                    ? mapper.listByTagNames(metalake, anchorNames)
                    : mapper.listByPolicyNames(metalake, anchorNames));
    if (relations.isEmpty()) {
      return Collections.emptyList();
    }

    return anchorType == Entity.EntityType.TAG
        ? policyTargets(metalake, relations)
        : tagTargets(metalake, relations);
  }

  /**
   * Creates, replaces, or removes policy-to-tag relations for one tag.
   *
   * <p>An add for an existing pair replaces its selector. Repeating the same selector and removing
   * a missing pair are idempotent no-ops.
   *
   * @param tagIdentifier The source tag identifier.
   * @param targetsToAdd Policy targets to create or replace.
   * @param targetsToRemove Policy targets to remove.
   * @return All active policy targets for the tag after the update.
   * @throws IOException If selector audit information cannot be serialized.
   */
  public List<PolicyEntity> updateRelations(
      NameIdentifier tagIdentifier,
      RelationEdgeTarget[] targetsToAdd,
      RelationEdgeTarget[] targetsToRemove)
      throws IOException {
    List<PolicyEntity> updatedPolicies = new ArrayList<>();
    try {
      SessionUtils.doMultipleWithCommit(
          () -> {
            try {
              updatedPolicies.addAll(
                  updateRelationsWithoutCommit(tagIdentifier, targetsToAdd, targetsToRemove));
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          });
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
    return updatedPolicies;
  }

  /**
   * Soft-deletes all policy-to-tag relations for a deleted policy.
   *
   * @param metalake The metalake name.
   * @param policyName The deleted policy name.
   * @return The number of deleted relations.
   */
  public int deleteRelationsForPolicy(String metalake, String policyName) {
    return SessionUtils.doWithCommitAndFetchResult(
        PolicyTagRelMapper.class,
        mapper -> mapper.softDeleteByMetalakeAndPolicyName(metalake, policyName));
  }

  /**
   * Soft-deletes all policy-to-tag relations for a deleted tag.
   *
   * @param metalake The metalake name.
   * @param tagName The deleted tag name.
   * @return The number of deleted relations.
   */
  public int deleteRelationsForTag(String metalake, String tagName) {
    return SessionUtils.doWithCommitAndFetchResult(
        PolicyTagRelMapper.class,
        mapper -> mapper.softDeleteByMetalakeAndTagName(metalake, tagName));
  }

  /**
   * Soft-deletes all policy-to-tag relations for a deleted metalake.
   *
   * @param metalakeId The deleted metalake ID.
   * @return The number of deleted relations.
   */
  public int deleteRelationsForMetalake(long metalakeId) {
    return SessionUtils.doWithCommitAndFetchResult(
        PolicyTagRelMapper.class, mapper -> mapper.softDeleteByMetalakeId(metalakeId));
  }

  /**
   * Physically deletes expired policy-to-tag relation rows.
   *
   * @param legacyTimeline The exclusive deletion timestamp upper bound.
   * @param limit The maximum number of rows to delete.
   * @return The number of deleted relations.
   */
  public int deleteRelationsByLegacyTimeline(long legacyTimeline, int limit) {
    return SessionUtils.doWithCommitAndFetchResult(
        PolicyTagRelMapper.class, mapper -> mapper.deleteByLegacyTimeline(legacyTimeline, limit));
  }

  private List<PolicyEntity> updateRelationsWithoutCommit(
      NameIdentifier tagIdentifier,
      RelationEdgeTarget[] targetsToAdd,
      RelationEdgeTarget[] targetsToRemove)
      throws IOException {
    long tagId = EntityIdService.getEntityId(tagIdentifier, Entity.EntityType.TAG);
    String metalake = tagIdentifier.namespace().level(0);
    RelationEdgeTarget[] targetsToRemoveOrEmpty = nullToEmpty(targetsToRemove);
    RelationEdgeTarget[] targetsToAddOrEmpty = nullToEmpty(targetsToAdd);

    Map<String, Long> policyIdsToRemove = resolvePolicyIds(metalake, targetsToRemoveOrEmpty);
    for (RelationEdgeTarget target : targetsToRemoveOrEmpty) {
      long policyId = policyIdsToRemove.get(target.nameIdentifier().name());
      SessionUtils.doWithoutCommit(
          PolicyTagRelMapper.class, mapper -> mapper.softDeleteByPair(policyId, tagId));
    }

    Map<String, Long> policyIdsToAdd = resolvePolicyIds(metalake, targetsToAddOrEmpty);
    for (RelationEdgeTarget target : targetsToAddOrEmpty) {
      long policyId = policyIdsToAdd.get(target.nameIdentifier().name());
      upsert(policyId, tagId, target.relationValue().orElse(null));
    }

    return listRelations(Collections.singletonList(tagIdentifier), Entity.EntityType.TAG).stream()
        .map(relation -> (PolicyEntity) relation.targetEntity())
        .collect(Collectors.toList());
  }

  private static List<RelationalEntity<?>> policyTargets(
      String metalake, List<PolicyTagRelPO> relations) {
    Set<String> policyNames =
        relations.stream()
            .map(PolicyTagRelPO::getPolicyName)
            .collect(Collectors.toCollection(LinkedHashSet::new));
    List<NameIdentifier> policyIdentifiers =
        policyNames.stream()
            .map(name -> NameIdentifierUtil.ofPolicy(metalake, name))
            .collect(Collectors.toList());
    Map<String, PolicyEntity> policies =
        PolicyMetaService.getInstance().batchGetPolicyByIdentifier(policyIdentifiers).stream()
            .collect(Collectors.toMap(PolicyEntity::name, policy -> policy));

    List<RelationalEntity<?>> result = new ArrayList<>();
    for (PolicyTagRelPO relation : relations) {
      PolicyEntity policy = policies.get(relation.getPolicyName());
      if (policy != null) {
        result.add(
            new RelationalEntity<>(
                SupportsRelationOperations.Type.POLICY_TAG_REL,
                NameIdentifierUtil.ofTag(metalake, relation.getTagName()),
                Entity.EntityType.TAG,
                policy,
                relation.getSelector()));
      }
    }
    return result;
  }

  private static List<RelationalEntity<?>> tagTargets(
      String metalake, List<PolicyTagRelPO> relations) {
    Set<String> tagNames =
        relations.stream()
            .map(PolicyTagRelPO::getTagName)
            .collect(Collectors.toCollection(LinkedHashSet::new));
    List<NameIdentifier> tagIdentifiers =
        tagNames.stream()
            .map(name -> NameIdentifierUtil.ofTag(metalake, name))
            .collect(Collectors.toList());
    Map<String, TagEntity> tags =
        TagMetaService.getInstance().batchGetTagByIdentifier(tagIdentifiers).stream()
            .collect(Collectors.toMap(TagEntity::name, tag -> tag));

    List<RelationalEntity<?>> result = new ArrayList<>();
    for (PolicyTagRelPO relation : relations) {
      TagEntity tag = tags.get(relation.getTagName());
      if (tag != null) {
        result.add(
            new RelationalEntity<>(
                SupportsRelationOperations.Type.POLICY_TAG_REL,
                NameIdentifierUtil.ofPolicy(metalake, relation.getPolicyName()),
                Entity.EntityType.POLICY,
                tag,
                relation.getSelector()));
      }
    }
    return result;
  }

  private static void upsert(long policyId, long tagId, String selector) throws IOException {
    PolicyTagRelPO existing =
        SessionUtils.getWithoutCommit(
            PolicyTagRelMapper.class, mapper -> mapper.getByPolicyIdAndTagId(policyId, tagId));
    if (existing != null && Objects.equals(existing.getSelector(), selector)) {
      return;
    }

    PolicyTagRelPO relation =
        PolicyTagRelPO.builder()
            .withPolicyId(policyId)
            .withTagId(tagId)
            .withSelector(selector)
            .withAuditInfo(auditInfo(existing))
            .withCurrentVersion(1L)
            .withLastVersion(1L)
            .withDeletedAt(0L)
            .build();
    if (existing == null) {
      SessionUtils.doWithoutCommit(PolicyTagRelMapper.class, mapper -> mapper.insert(relation));
    } else {
      SessionUtils.doWithoutCommit(
          PolicyTagRelMapper.class, mapper -> mapper.updateSelector(relation));
    }
  }

  private static String auditInfo(PolicyTagRelPO existing) throws IOException {
    String principal = PrincipalUtils.getCurrentPrincipal().getName();
    Instant now = Instant.now();
    AuditInfo auditInfo;
    if (existing == null) {
      auditInfo = AuditInfo.builder().withCreator(principal).withCreateTime(now).build();
    } else {
      auditInfo = JsonUtils.anyFieldMapper().readValue(existing.getAuditInfo(), AuditInfo.class);
      auditInfo.merge(
          AuditInfo.builder().withLastModifier(principal).withLastModifiedTime(now).build(), false);
    }
    return JsonUtils.anyFieldMapper().writeValueAsString(auditInfo);
  }

  private static void validatePolicyTarget(String metalake, RelationEdgeTarget target) {
    Preconditions.checkArgument(target != null, "Policy relation target cannot be null");
    Preconditions.checkArgument(
        target.entityType() == Entity.EntityType.POLICY,
        "Policy-to-tag relation target must be POLICY, but is %s",
        target.entityType());
    Preconditions.checkArgument(
        target.nameIdentifier().namespace().length() > 0
            && metalake.equals(target.nameIdentifier().namespace().level(0)),
        "Policy and tag must belong to the same metalake");
  }

  private static Map<String, Long> resolvePolicyIds(String metalake, RelationEdgeTarget[] targets) {
    if (targets.length == 0) {
      return Collections.emptyMap();
    }

    Set<String> policyNames = new LinkedHashSet<>();
    for (RelationEdgeTarget target : targets) {
      validatePolicyTarget(metalake, target);
      policyNames.add(target.nameIdentifier().name());
    }
    List<NameIdentifier> policyIdentifiers =
        policyNames.stream()
            .map(name -> NameIdentifierUtil.ofPolicy(metalake, name))
            .collect(Collectors.toList());
    Map<String, Long> policyIds =
        PolicyMetaService.getInstance().batchGetPolicyByIdentifier(policyIdentifiers).stream()
            .collect(Collectors.toMap(PolicyEntity::name, PolicyEntity::id));

    for (String policyName : policyNames) {
      if (!policyIds.containsKey(policyName)) {
        throw new NoSuchEntityException(
            NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
            Entity.EntityType.POLICY.name().toLowerCase(),
            policyName);
      }
    }
    return policyIds;
  }

  private static void validateSameMetalake(List<NameIdentifier> identifiers) {
    String metalake = identifiers.get(0).namespace().level(0);
    Preconditions.checkArgument(
        identifiers.stream()
            .allMatch(
                identifier ->
                    identifier.namespace().length() > 0
                        && metalake.equals(identifier.namespace().level(0))),
        "All policy-to-tag relation anchors must belong to the same metalake");
  }

  private static RelationEdgeTarget[] nullToEmpty(RelationEdgeTarget[] targets) {
    return targets == null ? new RelationEdgeTarget[0] : Arrays.copyOf(targets, targets.length);
  }
}
