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
import org.apache.gravitino.EntityAlreadyExistsException;
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
   * Creates or removes policy-to-tag relations for one tag.
   *
   * <p>An add for an existing policy and tag pair conflicts regardless of its selector. Removing a
   * missing pair is an idempotent no-op. The same pair cannot be added and removed in one update.
   *
   * @param tagIdentifier The source tag identifier.
   * @param targetsToAdd Policy targets to create.
   * @param targetsToRemove Policy targets to remove.
   * @return All active policy targets for the tag after the update.
   * @throws IOException If selector audit information cannot be serialized.
   * @throws EntityAlreadyExistsException If a relation to add already exists.
   * @throws IllegalArgumentException If the same relation is both added and removed.
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
    validateNoOverlappingTargets(targetsToAddOrEmpty, targetsToRemoveOrEmpty);

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

  private List<PolicyEntity> updateRelationsWithoutCommit(
      NameIdentifier tagIdentifier,
      RelationEdgeTarget[] targetsToAdd,
      RelationEdgeTarget[] targetsToRemove)
      throws IOException {
    String metalake = tagIdentifier.namespace().level(0);
    TagPO tagPO = lockTag(tagIdentifier);
    long tagId = tagPO.getTagId();
    Map<String, Long> policyIds = resolvePolicyIds(metalake, targetsToAdd, targetsToRemove);

    for (RelationEdgeTarget target : targetsToRemove) {
      long policyId = policyIds.get(target.nameIdentifier().name());
      PolicyTagRelPO existing =
          SessionUtils.getWithoutCommit(
              PolicyTagRelMapper.class, mapper -> mapper.getByPolicyIdAndTagId(policyId, tagId));
      if (existing != null) {
        int deleted =
            SessionUtils.getWithoutCommit(
                PolicyTagRelMapper.class, mapper -> mapper.softDeleteByIdAndVersion(existing));
        if (deleted != 1) {
          throw relationConflict(tagIdentifier);
        }
      }
    }

    for (RelationEdgeTarget target : targetsToAdd) {
      long policyId = policyIds.get(target.nameIdentifier().name());
      insertIfAbsent(
          tagIdentifier,
          target.nameIdentifier(),
          policyId,
          tagId,
          target.relationValue().orElse(null));
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

  private static void insertIfAbsent(
      NameIdentifier tagIdentifier,
      NameIdentifier policyIdentifier,
      long policyId,
      long tagId,
      String selector)
      throws IOException {
    PolicyTagRelPO relation =
        PolicyTagRelPO.builder()
            .withPolicyId(policyId)
            .withTagId(tagId)
            .withSelector(selector)
            .withAuditInfo(auditInfo())
            .withCurrentVersion(1L)
            .withLastVersion(1L)
            .withDeletedAt(0L)
            .build();
    int inserted =
        SessionUtils.getWithoutCommit(
            PolicyTagRelMapper.class, mapper -> mapper.insertIfAbsent(relation));
    if (inserted == 1) {
      return;
    }

    // A zero-row insert means another writer won the active-pair uniqueness race. Re-read the
    // pair to distinguish an existing relation from another concurrent state transition.
    PolicyTagRelPO winner =
        SessionUtils.getWithoutCommit(
            PolicyTagRelMapper.class, mapper -> mapper.getByPolicyIdAndTagId(policyId, tagId));
    if (winner != null) {
      throw relationAlreadyExists(tagIdentifier, policyIdentifier);
    }
    throw relationConflict(tagIdentifier);
  }

  private static String auditInfo() throws IOException {
    String principal = PrincipalUtils.getCurrentPrincipal().getName();
    Instant now = Instant.now();
    AuditInfo auditInfo = AuditInfo.builder().withCreator(principal).withCreateTime(now).build();
    return JsonUtils.anyFieldMapper().writeValueAsString(auditInfo);
  }

  private static OptimisticLockException relationConflict(NameIdentifier tagIdentifier) {
    return new OptimisticLockException(
        "A policy-to-tag relation for tag %s was modified concurrently; retry the operation",
        tagIdentifier);
  }

  private static EntityAlreadyExistsException relationAlreadyExists(
      NameIdentifier tagIdentifier, NameIdentifier policyIdentifier) {
    return new EntityAlreadyExistsException(
        "The policy-to-tag relation between tag %s and policy %s already exists",
        tagIdentifier, policyIdentifier);
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

  private static void validateNoOverlappingTargets(
      RelationEdgeTarget[] targetsToAdd, RelationEdgeTarget[] targetsToRemove) {
    Set<String> policyNamesToAdd =
        Arrays.stream(targetsToAdd)
            .map(target -> target.nameIdentifier().name())
            .collect(Collectors.toSet());
    for (RelationEdgeTarget target : targetsToRemove) {
      Preconditions.checkArgument(
          !policyNamesToAdd.contains(target.nameIdentifier().name()),
          "Policy-to-tag relation target %s cannot be both added and removed",
          target.nameIdentifier());
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

  private static Map<String, Long> resolvePolicyIds(
      String metalake, RelationEdgeTarget[] targetsToAdd, RelationEdgeTarget[] targetsToRemove) {
    Set<String> policyNames = new LinkedHashSet<>();
    Arrays.stream(targetsToAdd)
        .map(target -> target.nameIdentifier().name())
        .forEach(policyNames::add);
    Arrays.stream(targetsToRemove)
        .map(target -> target.nameIdentifier().name())
        .forEach(policyNames::add);
    if (policyNames.isEmpty()) {
      // A mutable map, like the one built below: returning Collections.emptyMap() here would mix
      // mutable and immutable return values, which Error Prone rejects.
      return new LinkedHashMap<>();
    }

    List<PolicyPO> policies =
        SessionUtils.getWithoutCommit(
            PolicyMetaMapper.class,
            mapper ->
                mapper.listPolicyPOsByMetalakeAndPolicyNames(
                    metalake, new ArrayList<>(policyNames)));
    // Lock the policy rows in policy-ID order so that two relation changes touching the same
    // policies queue up instead of deadlocking. The tag row is locked before this, so every path
    // through this service takes its locks in the same tag-then-policy order.
    List<PolicyPO> sortedPolicies = new ArrayList<>(policies);
    sortedPolicies.sort(Comparator.comparingLong(PolicyPO::getPolicyId));
    Map<String, Long> policyIds = new LinkedHashMap<>();
    for (PolicyPO observedPolicy : sortedPolicies) {
      PolicyPO lockedPolicy = PolicyMetaService.lockPolicy(observedPolicy);
      policyIds.put(lockedPolicy.getPolicyName(), lockedPolicy.getPolicyId());
    }

    for (String policyName : policyNames) {
      if (!policyIds.containsKey(policyName)) {
        throw noSuchEntity(Entity.EntityType.POLICY, policyName);
      }
    }
    return policyIds;
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
