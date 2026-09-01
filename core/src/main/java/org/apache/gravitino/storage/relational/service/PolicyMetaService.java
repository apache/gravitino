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

import static org.apache.gravitino.metrics.source.MetricsSource.GRAVITINO_RELATIONAL_STORE_METRIC_NAME;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.GenericEntity;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.mapper.PolicyMetaMapper;
import org.apache.gravitino.storage.relational.mapper.PolicyMetadataObjectRelMapper;
import org.apache.gravitino.storage.relational.mapper.PolicyTagRelMapper;
import org.apache.gravitino.storage.relational.mapper.PolicyVersionMapper;
import org.apache.gravitino.storage.relational.mapper.SecurableObjectMapper;
import org.apache.gravitino.storage.relational.mapper.TagMetadataObjectRelMapper;
import org.apache.gravitino.storage.relational.po.MetalakePO;
import org.apache.gravitino.storage.relational.po.PolicyMaxVersionPO;
import org.apache.gravitino.storage.relational.po.PolicyMetadataObjectRelPO;
import org.apache.gravitino.storage.relational.po.PolicyPO;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PolicyMetaService {
  private static final PolicyMetaService INSTANCE = new PolicyMetaService();
  private static final Logger LOG = LoggerFactory.getLogger(PolicyMetaService.class);

  public static PolicyMetaService getInstance() {
    return INSTANCE;
  }

  private PolicyMetaService() {}

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listPoliciesByNamespace")
  public List<PolicyEntity> listPoliciesByNamespace(Namespace namespace) {
    String metalakeName = namespace.level(0);
    List<PolicyPO> policyPOs =
        SessionUtils.getWithoutCommit(
            PolicyMetaMapper.class, mapper -> mapper.listPolicyPOsByMetalake(metalakeName));
    return policyPOs.stream()
        .map(policyPO -> POConverters.fromPolicyPO(policyPO, namespace))
        .collect(Collectors.toList());
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getPolicyByIdentifier")
  public PolicyEntity getPolicyByIdentifier(NameIdentifier ident) {
    String metalakeName = ident.namespace().level(0);
    PolicyPO policyPO = getPolicyPOByMetalakeAndName(metalakeName, ident.name());
    return POConverters.fromPolicyPO(policyPO, ident.namespace());
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "insertPolicy")
  public void insertPolicy(PolicyEntity policyEntity, boolean overwritten) throws IOException {
    Namespace ns = policyEntity.namespace();
    String metalakeName = ns.level(0);

    try {
      MetalakePO metalakePO =
          SessionUtils.getWithoutCommit(
              MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeMetaByName(metalakeName));
      if (metalakePO == null) {
        throw new NoSuchEntityException(
            NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
            Entity.EntityType.METALAKE.name().toLowerCase(),
            metalakeName);
      }

      PolicyPO.Builder builder = PolicyPO.builder().withMetalakeId(metalakePO.getMetalakeId());
      PolicyPO policyPO = POConverters.initializePolicyPOWithVersion(policyEntity, builder);

      SessionUtils.doMultipleWithCommit(
          () -> lockMetalakeForPolicyCreate(metalakePO),
          () -> insertPolicyWithoutCommit(policyEntity, policyPO, overwritten));
    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(e, Entity.EntityType.POLICY, policyEntity.toString());
      throw e;
    }
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "updatePolicy")
  public <E extends Entity & HasIdentifier> PolicyEntity updatePolicy(
      NameIdentifier ident, Function<E, E> updater) throws IOException {
    String metalakeName = ident.namespace().level(0);

    PolicyPO oldPolicyPO = getPolicyPOByMetalakeAndName(metalakeName, ident.name());
    PolicyEntity oldPolicyEntity = POConverters.fromPolicyPO(oldPolicyPO, ident.namespace());
    PolicyEntity updatedPolicyEntity = (PolicyEntity) updater.apply((E) oldPolicyEntity);
    Preconditions.checkArgument(
        Objects.equals(oldPolicyEntity.id(), updatedPolicyEntity.id()),
        "The updated policy entity id: %s must have the same id as the old entity id %s",
        updatedPolicyEntity.id(),
        oldPolicyEntity.id());

    try {
      PolicyPO newPolicyPO =
          POConverters.updatePolicyPOWithVersion(oldPolicyPO, updatedPolicyEntity);
      SessionUtils.doMultipleWithCommit(
          () -> updatePolicyRootWithVersion(ident, oldPolicyPO, newPolicyPO),
          () ->
              SessionUtils.doWithoutCommit(
                  PolicyVersionMapper.class,
                  mapper -> mapper.insertPolicyVersion(newPolicyPO.getPolicyVersionPO())));
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.POLICY, updatedPolicyEntity.nameIdentifier().toString());
      throw re;
    }

    return updatedPolicyEntity;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deletePolicy")
  public boolean deletePolicy(NameIdentifier ident) {
    PolicyPO policyPO;
    try {
      policyPO = getPolicyPOByMetalakeAndName(ident.namespace().level(0), ident.name());
    } catch (NoSuchEntityException e) {
      return false;
    }
    return deletePolicy(ident, policyPO);
  }

  boolean deletePolicy(NameIdentifier ident, PolicyPO policyPO) {
    long policyId = policyPO.getPolicyId();

    SessionUtils.doMultipleWithCommit(
        () -> deletePolicyWithVersion(ident, policyPO),
        () ->
            SessionUtils.doWithoutCommit(
                PolicyVersionMapper.class,
                mapper -> mapper.softDeletePolicyVersionsByPolicyId(policyId)),
        () ->
            SessionUtils.doWithoutCommit(
                PolicyMetadataObjectRelMapper.class,
                mapper -> mapper.softDeletePolicyMetadataObjectRelsByPolicyId(policyId)),
        () ->
            SessionUtils.doWithoutCommit(
                PolicyTagRelMapper.class, mapper -> mapper.softDeleteByPolicyId(policyId)),
        () ->
            SessionUtils.doWithoutCommit(
                TagMetadataObjectRelMapper.class,
                mapper ->
                    mapper.softDeleteTagMetadataObjectRelsByMetadataObject(
                        policyId, MetadataObject.Type.POLICY.name())),
        () ->
            SessionUtils.doWithoutCommit(
                OwnerMetaMapper.class,
                mapper ->
                    mapper.softDeleteOwnerRelByMetadataObjectIdAndType(
                        policyId, MetadataObject.Type.POLICY.name())),
        () ->
            SessionUtils.doWithoutCommit(
                SecurableObjectMapper.class,
                mapper ->
                    mapper.softDeleteObjectRelsByMetadataObject(
                        policyId, MetadataObject.Type.POLICY.name())));
    return true;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listPoliciesForMetadataObject")
  public List<PolicyEntity> listPoliciesForMetadataObject(
      NameIdentifier objectIdent, Entity.EntityType objectType)
      throws NoSuchEntityException, IOException {
    MetadataObject metadataObject = NameIdentifierUtil.toMetadataObject(objectIdent, objectType);
    String metalake = objectIdent.namespace().level(0);

    List<PolicyPO> PolicyPOs;
    try {
      Long metadataObjectId = EntityIdService.getEntityId(objectIdent, objectType);

      PolicyPOs =
          SessionUtils.getWithoutCommit(
              PolicyMetadataObjectRelMapper.class,
              mapper ->
                  mapper.listPolicyPOsByMetadataObjectIdAndType(
                      metadataObjectId, metadataObject.type().toString()));
    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(e, Entity.EntityType.POLICY, objectIdent.toString());
      throw e;
    }

    return PolicyPOs.stream()
        .map(PolicyPO -> POConverters.fromPolicyPO(PolicyPO, NamespaceUtil.ofPolicy(metalake)))
        .collect(Collectors.toList());
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getPolicyForMetadataObject")
  public PolicyEntity getPolicyForMetadataObject(
      NameIdentifier objectIdent, Entity.EntityType objectType, NameIdentifier policyIdent)
      throws NoSuchEntityException, IOException {
    MetadataObject metadataObject = NameIdentifierUtil.toMetadataObject(objectIdent, objectType);
    String metalake = objectIdent.namespace().level(0);

    PolicyPO policyPO;
    try {
      Long metadataObjectId = EntityIdService.getEntityId(objectIdent, objectType);

      policyPO =
          SessionUtils.getWithoutCommit(
              PolicyMetadataObjectRelMapper.class,
              mapper ->
                  mapper.getPolicyPOsByMetadataObjectAndPolicyName(
                      metadataObjectId, metadataObject.type().toString(), policyIdent.name()));
    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(e, Entity.EntityType.POLICY, policyIdent.toString());
      throw e;
    }

    if (policyPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.POLICY.name().toLowerCase(),
          policyIdent.name());
    }

    return POConverters.fromPolicyPO(policyPO, NamespaceUtil.ofPolicy(metalake));
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listAssociatedEntitiesForPolicy")
  public List<GenericEntity> listAssociatedEntitiesForPolicy(NameIdentifier policyIdent)
      throws IOException {
    String metalakeName = policyIdent.namespace().level(0);
    String policyName = policyIdent.name();

    try {
      List<PolicyMetadataObjectRelPO> policyMetadataObjectRelPOs =
          SessionUtils.doWithCommitAndFetchResult(
              PolicyMetadataObjectRelMapper.class,
              mapper ->
                  mapper.listPolicyMetadataObjectRelsByMetalakeAndPolicyName(
                      metalakeName, policyName));

      return policyMetadataObjectRelPOs.stream()
          .map(
              r ->
                  GenericEntity.builder()
                      .withId(r.getMetadataObjectId())
                      .withEntityType(
                          MetadataObjectUtil.toEntityType(
                              MetadataObject.Type.valueOf(r.getMetadataObjectType())))
                      .build())
          .collect(Collectors.toList());

    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(e, Entity.EntityType.POLICY, policyIdent.toString());
      throw e;
    }
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "associatePoliciesWithMetadataObject")
  public List<PolicyEntity> associatePoliciesWithMetadataObject(
      NameIdentifier objectIdent,
      Entity.EntityType objectType,
      NameIdentifier[] policiesToAdd,
      NameIdentifier[] policiesToRemove)
      throws NoSuchEntityException, EntityAlreadyExistsException, IOException {
    try {
      return SessionUtils.doWithCommitAndFetchResult(
          PolicyMetaMapper.class,
          ignored ->
              associatePoliciesWithMetadataObjectWithoutCommit(
                  objectIdent, objectType, policiesToAdd, policiesToRemove));
    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(e, Entity.EntityType.POLICY, objectIdent.toString());
      throw e;
    }
  }

  private List<PolicyEntity> associatePoliciesWithMetadataObjectWithoutCommit(
      NameIdentifier objectIdent,
      Entity.EntityType objectType,
      NameIdentifier[] policiesToAdd,
      NameIdentifier[] policiesToRemove) {
    MetadataObject metadataObject = NameIdentifierUtil.toMetadataObject(objectIdent, objectType);
    String metalake = objectIdent.namespace().level(0);

    Long metadataObjectId = EntityIdService.getEntityId(objectIdent, objectType);

    // Fetch all the policies need to associate with the metadata object.
    List<String> policyNamesToAdd =
        Arrays.stream(policiesToAdd).map(NameIdentifier::name).collect(Collectors.toList());
    List<PolicyPO> policyPOsToAdd =
        policyNamesToAdd.isEmpty()
            ? Collections.emptyList()
            : getPolicyPOsByMetalakeAndNames(metalake, policyNamesToAdd);

    // Fetch all the policies need to remove from the metadata object.
    List<String> policyNamesToRemove =
        Arrays.stream(policiesToRemove).map(NameIdentifier::name).collect(Collectors.toList());
    List<PolicyPO> policyPOsToRemove =
        policyNamesToRemove.isEmpty()
            ? Collections.emptyList()
            : getPolicyPOsByMetalakeAndNames(metalake, policyNamesToRemove);
    Map<Long, PolicyPO> lockedPolicyPOs =
        lockPoliciesForAssociation(policyPOsToAdd, policyPOsToRemove);
    policyPOsToAdd = currentPolicyPOs(policyPOsToAdd, lockedPolicyPOs);
    policyPOsToRemove = currentPolicyPOs(policyPOsToRemove, lockedPolicyPOs);

    if (!policyPOsToAdd.isEmpty()) {
      List<PolicyMetadataObjectRelPO> policyRelsToAdd =
          policyPOsToAdd.stream()
              .map(
                  policyPO ->
                      POConverters.initializePolicyMetadataObjectRelPOWithVersion(
                          policyPO.getPolicyId(),
                          metadataObjectId,
                          metadataObject.type().toString()))
              .collect(Collectors.toList());
      SessionUtils.doWithoutCommit(
          PolicyMetadataObjectRelMapper.class,
          mapper -> mapper.batchInsertPolicyMetadataObjectRels(policyRelsToAdd));
    }
    if (!policyPOsToRemove.isEmpty()) {
      List<Long> policyIdsToRemove =
          policyPOsToRemove.stream().map(PolicyPO::getPolicyId).collect(Collectors.toList());
      SessionUtils.doWithoutCommit(
          PolicyMetadataObjectRelMapper.class,
          mapper ->
              mapper.batchDeletePolicyMetadataObjectRelsByPolicyIdsAndMetadataObject(
                  metadataObjectId, metadataObject.type().toString(), policyIdsToRemove));
    }

    // Fetch all the policies associated with the metadata object after the operation.
    List<PolicyPO> policyPOs =
        SessionUtils.getWithoutCommit(
            PolicyMetadataObjectRelMapper.class,
            mapper ->
                mapper.listPolicyPOsByMetadataObjectIdAndType(
                    metadataObjectId, metadataObject.type().toString()));

    return policyPOs.stream()
        .map(policyPO -> POConverters.fromPolicyPO(policyPO, NamespaceUtil.ofPolicy(metalake)))
        .collect(Collectors.toList());
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deletePolicyAndVersionMetasByLegacyTimeline")
  public int deletePolicyAndVersionMetasByLegacyTimeline(Long legacyTimeline, int limit) {
    int policyDeletedCount =
        SessionUtils.doWithCommitAndFetchResult(
            PolicyMetaMapper.class,
            mapper -> mapper.deletePolicyMetasByLegacyTimeline(legacyTimeline, limit));

    int policyVersionDeletedCount =
        SessionUtils.doWithCommitAndFetchResult(
            PolicyVersionMapper.class,
            mapper -> mapper.deletePolicyVersionsByLegacyTimeline(legacyTimeline, limit));

    return policyDeletedCount + policyVersionDeletedCount;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deletePolicyVersionsByRetentionCount")
  public int deletePolicyVersionsByRetentionCount(Long versionRetentionCount, int limit) {
    // get the current version of all policies.
    List<PolicyMaxVersionPO> policyMaxVersions =
        SessionUtils.getWithoutCommit(
            PolicyVersionMapper.class,
            mapper -> mapper.selectPolicyVersionsByRetentionCount(versionRetentionCount));

    // soft delete old versions that are smaller than or equal to (maxVersion -
    // versionRetentionCount).
    int totalDeletedCount = 0;
    for (PolicyMaxVersionPO policyMaxVersion : policyMaxVersions) {
      long versionRetentionLine = policyMaxVersion.getVersion() - versionRetentionCount;
      int deletedCount =
          SessionUtils.doWithCommitAndFetchResult(
              PolicyVersionMapper.class,
              mapper ->
                  mapper.softDeletePolicyVersionsByRetentionLine(
                      policyMaxVersion.getPolicyId(), versionRetentionLine, limit));
      totalDeletedCount += deletedCount;

      // log the deletion by max policy version.
      LOG.info(
          "Soft delete policyVersions count: {} which versions are smaller than or equal to"
              + " versionRetentionLine: {}, the current policyId and maxVersion is: <{}, {}>.",
          deletedCount,
          versionRetentionLine,
          policyMaxVersion.getPolicyId(),
          policyMaxVersion.getVersion());
    }
    return totalDeletedCount;
  }

  void lockMetalakeForPolicyCreate(MetalakePO observedMetalakePO) {
    OccWriteSupport.lockParentForChildWrite(
        observedMetalakePO.getMetalakeName(),
        Entity.EntityType.METALAKE,
        () ->
            SessionUtils.getWithoutCommit(
                MetalakeMetaMapper.class,
                mapper ->
                    mapper.selectMetalakeMetaByIdForShare(observedMetalakePO.getMetalakeId())),
        null,
        current -> Objects.equals(current.getMetalakeName(), observedMetalakePO.getMetalakeName()));
  }

  private void insertPolicyWithoutCommit(
      PolicyEntity policyEntity, PolicyPO initializedPolicyPO, boolean overwritten) {
    if (!overwritten) {
      insertNewPolicyWithoutCommit(initializedPolicyPO);
      return;
    }

    PolicyPO existingPolicyPO = findAndLockPolicyForOverwrite(initializedPolicyPO);
    if (existingPolicyPO == null) {
      insertNewPolicyWithoutCommit(initializedPolicyPO);
      return;
    }

    PolicyPO replacementPolicyPO =
        POConverters.updatePolicyPOWithVersion(existingPolicyPO, policyEntity);
    updatePolicyRootWithVersion(
        policyEntity.nameIdentifier(), existingPolicyPO, replacementPolicyPO);
    SessionUtils.doWithoutCommit(
        PolicyVersionMapper.class,
        mapper -> mapper.insertPolicyVersion(replacementPolicyPO.getPolicyVersionPO()));
  }

  private void insertNewPolicyWithoutCommit(PolicyPO policyPO) {
    SessionUtils.doWithoutCommit(
        PolicyMetaMapper.class, mapper -> mapper.insertPolicyMeta(policyPO));
    SessionUtils.doWithoutCommit(
        PolicyVersionMapper.class,
        mapper -> mapper.insertPolicyVersion(policyPO.getPolicyVersionPO()));
  }

  private PolicyPO findAndLockPolicyForOverwrite(PolicyPO initializedPolicyPO) {
    PolicyPO existingPolicyPO =
        SessionUtils.getWithoutCommit(
            PolicyMetaMapper.class,
            mapper -> mapper.selectPolicyByPolicyIdForUpdate(initializedPolicyPO.getPolicyId()));
    if (existingPolicyPO != null) {
      return existingPolicyPO;
    }

    PolicyPO sameNamePolicyPO =
        SessionUtils.getWithoutCommit(
            PolicyMetaMapper.class,
            mapper ->
                mapper.selectPolicyMetaByMetalakeIdAndName(
                    initializedPolicyPO.getMetalakeId(), initializedPolicyPO.getPolicyName()));
    if (sameNamePolicyPO == null) {
      return null;
    }
    return SessionUtils.getWithoutCommit(
        PolicyMetaMapper.class,
        mapper -> mapper.selectPolicyByPolicyIdForUpdate(sameNamePolicyPO.getPolicyId()));
  }

  private void updatePolicyRootWithVersion(
      NameIdentifier identifier, PolicyPO oldPolicyPO, PolicyPO newPolicyPO) {
    int updated =
        SessionUtils.getWithoutCommit(
            PolicyMetaMapper.class, mapper -> mapper.updatePolicyMeta(newPolicyPO, oldPolicyPO));
    if (updated == 0) {
      throw policyWriteFailure(identifier, oldPolicyPO);
    }
  }

  private void deletePolicyWithVersion(NameIdentifier identifier, PolicyPO observedPolicyPO) {
    OccWriteSupport.deleteWithVersion(
        () ->
            SessionUtils.getWithoutCommit(
                PolicyMetaMapper.class,
                mapper ->
                    mapper.softDeletePolicyByIdAndVersion(
                        observedPolicyPO.getPolicyId(), observedPolicyPO.getCurrentVersion())),
        () -> policyWriteFailure(identifier, observedPolicyPO));
  }

  private RuntimeException policyWriteFailure(
      NameIdentifier identifier, PolicyPO observedPolicyPO) {
    return OccWriteSupport.writeFailure(
        identifier,
        Entity.EntityType.POLICY,
        () ->
            SessionUtils.getWithoutCommit(
                PolicyMetaMapper.class,
                mapper -> mapper.selectPolicyByPolicyIdForUpdate(observedPolicyPO.getPolicyId())),
        null,
        current ->
            Objects.equals(current.getPolicyName(), observedPolicyPO.getPolicyName())
                && Objects.equals(current.getMetalakeId(), observedPolicyPO.getMetalakeId()));
  }

  private Map<Long, PolicyPO> lockPoliciesForAssociation(
      List<PolicyPO> policyPOsToAdd, List<PolicyPO> policyPOsToRemove) {
    Map<Long, PolicyPO> observedPolicyPOs = new LinkedHashMap<>();
    policyPOsToAdd.forEach(policyPO -> observedPolicyPOs.put(policyPO.getPolicyId(), policyPO));
    policyPOsToRemove.forEach(policyPO -> observedPolicyPOs.put(policyPO.getPolicyId(), policyPO));
    List<PolicyPO> sortedPolicyPOs = new ArrayList<>(observedPolicyPOs.values());
    sortedPolicyPOs.sort(Comparator.comparingLong(PolicyPO::getPolicyId));

    Map<Long, PolicyPO> lockedPolicyPOs = new LinkedHashMap<>();
    for (PolicyPO observedPolicyPO : sortedPolicyPOs) {
      PolicyPO lockedPolicyPO =
          SessionUtils.getWithoutCommit(
              PolicyMetaMapper.class,
              mapper -> mapper.selectPolicyByPolicyIdForUpdate(observedPolicyPO.getPolicyId()));
      if (lockedPolicyPO == null
          || !Objects.equals(lockedPolicyPO.getPolicyName(), observedPolicyPO.getPolicyName())
          || !Objects.equals(lockedPolicyPO.getMetalakeId(), observedPolicyPO.getMetalakeId())) {
        throw new NoSuchEntityException(
            NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
            Entity.EntityType.POLICY.name().toLowerCase(),
            observedPolicyPO.getPolicyName());
      }
      lockedPolicyPOs.put(lockedPolicyPO.getPolicyId(), lockedPolicyPO);
    }
    return lockedPolicyPOs;
  }

  private static List<PolicyPO> currentPolicyPOs(
      List<PolicyPO> observedPolicyPOs, Map<Long, PolicyPO> lockedPolicyPOs) {
    return observedPolicyPOs.stream()
        .map(policyPO -> lockedPolicyPOs.get(policyPO.getPolicyId()))
        .collect(Collectors.toList());
  }

  private PolicyPO getPolicyPOByMetalakeAndName(String metalakeName, String policyName) {
    PolicyPO policyPO =
        SessionUtils.getWithoutCommit(
            PolicyMetaMapper.class,
            mapper -> mapper.selectPolicyMetaByMetalakeAndName(metalakeName, policyName));

    if (policyPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.POLICY.name().toLowerCase(),
          policyName);
    }
    return policyPO;
  }

  private List<PolicyPO> getPolicyPOsByMetalakeAndNames(
      String metalakeName, List<String> policyNames) {
    return SessionUtils.getWithoutCommit(
        PolicyMetaMapper.class,
        mapper -> mapper.listPolicyPOsByMetalakeAndPolicyNames(metalakeName, policyNames));
  }

  /**
   * Get policy id by policy name
   *
   * @param metalakeId metalake id
   * @param policyName policy name
   * @return policy id
   */
  public long getPolicyIdByPolicyName(long metalakeId, String policyName) {
    PolicyPO policyPO =
        SessionUtils.getWithoutCommit(
            PolicyMetaMapper.class,
            mapper -> mapper.selectPolicyMetaByMetalakeIdAndName(metalakeId, policyName));
    if (policyPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.POLICY.name().toLowerCase(),
          policyName);
    }
    return policyPO.getPolicyId();
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "batchGetPolicyByIdentifier")
  public List<PolicyEntity> batchGetPolicyByIdentifier(List<NameIdentifier> identifiers) {
    NameIdentifier firstIdent = identifiers.get(0);
    String metalakeName = firstIdent.namespace().level(0);
    List<String> policyNames =
        identifiers.stream().map(NameIdentifier::name).collect(Collectors.toList());

    return SessionUtils.doWithCommitAndFetchResult(
        PolicyMetaMapper.class,
        mapper -> {
          List<PolicyPO> policyPOs =
              mapper.batchSelectPolicyByIdentifier(metalakeName, policyNames);
          return POConverters.fromPolicyPOs(policyPOs, firstIdent.namespace());
        });
  }
}
