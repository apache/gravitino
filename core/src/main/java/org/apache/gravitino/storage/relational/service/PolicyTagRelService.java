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
import java.util.Comparator;
import java.util.LinkedHashMap;
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
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.storage.relational.mapper.PolicyMetaMapper;
import org.apache.gravitino.storage.relational.mapper.PolicyTagRelMapper;
import org.apache.gravitino.storage.relational.mapper.TagMetaMapper;
import org.apache.gravitino.storage.relational.po.PolicyPO;
import org.apache.gravitino.storage.relational.po.PolicyTagRelPO;
import org.apache.gravitino.storage.relational.po.TagPO;
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
    NameIdentifierUtil.checkTag(tagIdentifier);
    String metalake = tagIdentifier.namespace().level(0);
    RelationEdgeTarget[] targetsToAddOrEmpty = nullToEmpty(targetsToAdd);
    RelationEdgeTarget[] targetsToRemoveOrEmpty = nullToEmpty(targetsToRemove);
    validatePolicyTargets(metalake, targetsToAddOrEmpty);
    validatePolicyTargets(metalake, targetsToRemoveOrEmpty);

    List<PolicyEntity> updatedPolicies = new ArrayList<>();
    try {
      SessionUtils.doMultipleWithCommit(
          () -> {
            try {
              updatedPolicies.addAll(
                  updateRelationsWithoutCommit(
                      tagIdentifier, targetsToAddOrEmpty, targetsToRemoveOrEmpty));
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
   * @param policyId The deleted policy ID.
   * @return The number of deleted relations.
   */
  public int deleteRelationsForPolicy(long policyId) {
    return SessionUtils.doWithCommitAndFetchResult(
        PolicyTagRelMapper.class, mapper -> mapper.softDeleteByPolicyId(policyId));
  }

  /**
   * Soft-deletes all policy-to-tag relations for a deleted tag.
   *
   * @param tagId The deleted tag ID.
   * @return The number of deleted relations.
   */
  public int deleteRelationsForTag(long tagId) {
    return SessionUtils.doWithCommitAndFetchResult(
        PolicyTagRelMapper.class, mapper -> mapper.softDeleteByTagId(tagId));
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
    String metalake = tagIdentifier.namespace().level(0);
    TagPO tagPO = lockTag(tagIdentifier);
    long tagId = tagPO.getTagId();
    Map<String, Long> policyIds =
        resolveAndLockPolicyIds(metalake, tagPO.getMetalakeId(), targetsToAdd, targetsToRemove);

    for (RelationEdgeTarget target : targetsToRemove) {
      long policyId = policyIds.get(target.nameIdentifier().name());
      PolicyTagRelPO existing =
          SessionUtils.getWithoutCommit(
              PolicyTagRelMapper.class, mapper -> mapper.getByPolicyIdAndTagId(policyId, tagId));
      if (existing != null) {
        int deleted =
            SessionUtils.getWithoutCommit(
                PolicyTagRelMapper.class, mapper -> mapper.softDeleteByPair(existing));
        if (deleted != 1) {
          throw relationConflict(tagIdentifier);
        }
      }
    }

    for (RelationEdgeTarget target : targetsToAdd) {
      long policyId = policyIds.get(target.nameIdentifier().name());
      upsert(policyId, tagId, target.relationValue().orElse(null), tagIdentifier);
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

  private static void upsert(
      long policyId, long tagId, String selector, NameIdentifier tagIdentifier) throws IOException {
    PolicyTagRelPO existing =
        SessionUtils.getWithoutCommit(
            PolicyTagRelMapper.class, mapper -> mapper.getByPolicyIdAndTagId(policyId, tagId));
    if (existing != null && Objects.equals(existing.getSelector(), selector)) {
      return;
    }

    long nextVersion = existing == null ? 1L : existing.getCurrentVersion() + 1;
    PolicyTagRelPO relation =
        PolicyTagRelPO.builder()
            .withPolicyId(policyId)
            .withTagId(tagId)
            .withSelector(selector)
            .withAuditInfo(auditInfo(existing))
            .withCurrentVersion(nextVersion)
            .withLastVersion(nextVersion)
            .withDeletedAt(0L)
            .build();
    if (existing == null) {
      SessionUtils.doWithoutCommit(PolicyTagRelMapper.class, mapper -> mapper.insert(relation));
    } else {
      int updated =
          SessionUtils.getWithoutCommit(
              PolicyTagRelMapper.class, mapper -> mapper.updateSelector(relation, existing));
      if (updated != 1) {
        throw relationConflict(tagIdentifier);
      }
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

  private static OptimisticLockException relationConflict(NameIdentifier tagIdentifier) {
    return new OptimisticLockException(
        "A policy-to-tag relation for tag %s was modified concurrently; retry the operation",
        tagIdentifier);
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

  private static void validatePolicyTargets(String metalake, RelationEdgeTarget[] targets) {
    for (RelationEdgeTarget target : targets) {
      validatePolicyTarget(metalake, target);
    }
  }

  private static TagPO lockTag(NameIdentifier tagIdentifier) {
    String metalake = tagIdentifier.namespace().level(0);
    return SessionUtils.getWithoutCommit(
        TagMetaMapper.class,
        mapper -> {
          TagPO observed = mapper.selectTagMetaByMetalakeAndName(metalake, tagIdentifier.name());
          if (observed == null) {
            throw noSuchEntity(Entity.EntityType.TAG, tagIdentifier.name());
          }
          TagPO locked = mapper.selectTagByTagIdForUpdate(observed.getTagId());
          if (locked == null
              || !Objects.equals(locked.getTagName(), tagIdentifier.name())
              || !Objects.equals(locked.getMetalakeId(), observed.getMetalakeId())) {
            throw noSuchEntity(Entity.EntityType.TAG, tagIdentifier.name());
          }
          return locked;
        });
  }

  private static Map<String, Long> resolveAndLockPolicyIds(
      String metalake,
      Long metalakeId,
      RelationEdgeTarget[] targetsToAdd,
      RelationEdgeTarget[] targetsToRemove) {
    Set<String> policyNames = new LinkedHashSet<>();
    Arrays.stream(targetsToAdd)
        .map(target -> target.nameIdentifier().name())
        .forEach(policyNames::add);
    Arrays.stream(targetsToRemove)
        .map(target -> target.nameIdentifier().name())
        .forEach(policyNames::add);
    if (policyNames.isEmpty()) {
      return Collections.emptyMap();
    }

    List<PolicyPO> observedPolicies =
        SessionUtils.getWithoutCommit(
            PolicyMetaMapper.class,
            mapper ->
                mapper.listPolicyPOsByMetalakeAndPolicyNames(
                    metalake, new ArrayList<>(policyNames)));
    Map<String, PolicyPO> observedByName =
        observedPolicies.stream()
            .collect(Collectors.toMap(PolicyPO::getPolicyName, policy -> policy));

    for (String policyName : policyNames) {
      if (!observedByName.containsKey(policyName)) {
        throw noSuchEntity(Entity.EntityType.POLICY, policyName);
      }
    }

    List<PolicyPO> orderedPolicies =
        observedPolicies.stream()
            .sorted(Comparator.comparing(PolicyPO::getPolicyId))
            .collect(Collectors.toList());
    return SessionUtils.getWithoutCommit(
        PolicyMetaMapper.class,
        mapper -> {
          Map<String, Long> result = new LinkedHashMap<>();
          for (PolicyPO observed : orderedPolicies) {
            PolicyPO locked = mapper.selectPolicyByPolicyIdForShare(observed.getPolicyId());
            if (locked == null
                || !Objects.equals(locked.getPolicyName(), observed.getPolicyName())
                || !Objects.equals(locked.getMetalakeId(), metalakeId)) {
              throw noSuchEntity(Entity.EntityType.POLICY, observed.getPolicyName());
            }
            result.put(locked.getPolicyName(), locked.getPolicyId());
          }
          return result;
        });
  }

  private static NoSuchEntityException noSuchEntity(Entity.EntityType type, String name) {
    return new NoSuchEntityException(
        NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, type.name().toLowerCase(), name);
  }

  private static void validateSameMetalake(List<NameIdentifier> identifiers) {
    Preconditions.checkArgument(
        identifiers.stream()
            .allMatch(identifier -> identifier != null && identifier.namespace().length() > 0),
        "All policy-to-tag relation anchors must have a metalake namespace");
    String metalake = identifiers.get(0).namespace().level(0);
    Preconditions.checkArgument(
        identifiers.stream()
            .allMatch(identifier -> metalake.equals(identifier.namespace().level(0))),
        "All policy-to-tag relation anchors must belong to the same metalake");
  }

  private static RelationEdgeTarget[] nullToEmpty(RelationEdgeTarget[] targets) {
    return targets == null ? new RelationEdgeTarget[0] : Arrays.copyOf(targets, targets.length);
  }
}
