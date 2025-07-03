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
import java.util.List;
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
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.storage.relational.mapper.PolicyMetaMapper;
import org.apache.gravitino.storage.relational.mapper.PolicyVersionMapper;
import org.apache.gravitino.storage.relational.po.PolicyMaxVersionPO;
import org.apache.gravitino.storage.relational.po.PolicyPO;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PolicyMetaService {
  private static final PolicyMetaService INSTANCE = new PolicyMetaService();
  private static final Logger LOG = LoggerFactory.getLogger(PolicyMetaService.class);

  public static PolicyMetaService getInstance() {
    return INSTANCE;
  }

  private PolicyMetaService() {}

  public List<PolicyEntity> listPoliciesByNamespace(Namespace namespace) {
    String metalakeName = namespace.level(0);
    List<PolicyPO> policyPOs =
        SessionUtils.getWithoutCommit(
            PolicyMetaMapper.class, mapper -> mapper.listPolicyPOsByMetalake(metalakeName));
    return policyPOs.stream()
        .map(policyPO -> POConverters.fromPolicyPO(policyPO, namespace))
        .collect(Collectors.toList());
  }

  public PolicyEntity getPolicyByIdentifier(NameIdentifier ident) {
    String metalakeName = ident.namespace().level(0);
    PolicyPO policyPO = getPolicyPOByMetalakeAndName(metalakeName, ident.name());
    return POConverters.fromPolicyPO(policyPO, ident.namespace());
  }

  public void insertPolicy(PolicyEntity policyEntity, boolean overwritten) throws IOException {
    Namespace ns = policyEntity.namespace();
    String metalakeName = ns.level(0);

    try {
      Long metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(metalakeName);

      PolicyPO.Builder builder = PolicyPO.builder().withMetalakeId(metalakeId);
      PolicyPO policyPO = POConverters.initializePolicyPOWithVersion(policyEntity, builder);

      // insert both policy meta table and policy version table
      SessionUtils.doMultipleWithCommit(
          () ->
              SessionUtils.doWithoutCommit(
                  PolicyMetaMapper.class,
                  mapper -> {
                    if (overwritten) {
                      mapper.insertPolicyMetaOnDuplicateKeyUpdate(policyPO);
                    } else {
                      mapper.insertPolicyMeta(policyPO);
                    }
                  }),
          () ->
              SessionUtils.doWithoutCommit(
                  PolicyVersionMapper.class,
                  mapper -> {
                    if (overwritten) {
                      mapper.insertPolicyVersionOnDuplicateKeyUpdate(policyPO.getPolicyVersionPO());
                    } else {
                      mapper.insertPolicyVersion(policyPO.getPolicyVersionPO());
                    }
                  }));
    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(e, Entity.EntityType.POLICY, policyEntity.toString());
      throw e;
    }
  }

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

    Integer updateResult;
    try {
      boolean checkNeedUpdateVersion =
          POConverters.checkPolicyVersionNeedUpdate(
              oldPolicyPO.getPolicyVersionPO(), updatedPolicyEntity);
      PolicyPO newPolicyPO =
          POConverters.updatePolicyPOWithVersion(
              oldPolicyPO, updatedPolicyEntity, checkNeedUpdateVersion);
      if (checkNeedUpdateVersion) {
        SessionUtils.doMultipleWithCommit(
            () ->
                SessionUtils.doWithoutCommit(
                    PolicyVersionMapper.class,
                    mapper -> mapper.insertPolicyVersion(newPolicyPO.getPolicyVersionPO())),
            () ->
                SessionUtils.doWithoutCommit(
                    PolicyMetaMapper.class,
                    mapper -> mapper.updatePolicyMeta(newPolicyPO, oldPolicyPO)));
        // we set the updateResult to 1 to indicate that the update is successful
        updateResult = 1;
      } else {
        updateResult =
            SessionUtils.doWithCommitAndFetchResult(
                PolicyMetaMapper.class,
                mapper -> mapper.updatePolicyMeta(newPolicyPO, oldPolicyPO));
      }
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.POLICY, updatedPolicyEntity.nameIdentifier().toString());
      throw re;
    }

    if (updateResult > 0) {
      return updatedPolicyEntity;
    } else {
      throw new IOException("Failed to update the entity: " + updatedPolicyEntity);
    }
  }

  public boolean deletePolicy(NameIdentifier ident) {
    String metalakeName = ident.namespace().level(0);
    int[] policyMetaDeletedCount = new int[] {0};
    int[] policyVersionDeletedCount = new int[] {0};

    // We should delete meta and version info
    SessionUtils.doMultipleWithCommit(
        () ->
            policyMetaDeletedCount[0] =
                SessionUtils.doWithoutCommitAndFetchResult(
                    PolicyMetaMapper.class,
                    mapper ->
                        mapper.softDeletePolicyByMetalakeAndPolicyName(metalakeName, ident.name())),
        () ->
            policyVersionDeletedCount[0] =
                SessionUtils.doWithoutCommitAndFetchResult(
                    PolicyVersionMapper.class,
                    mapper ->
                        mapper.softDeletePolicyVersionByMetalakeAndPolicyName(
                            metalakeName, ident.name())));
    return policyMetaDeletedCount[0] + policyVersionDeletedCount[0] > 0;
  }

  public List<PolicyEntity> listPoliciesForMetadataObject(
      NameIdentifier objectIdent, MetadataObject.Type objectType)
      throws NoSuchEntityException, IOException {
    // todo: implement this method
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public PolicyEntity getPolicyForMetadataObject(
      NameIdentifier objectIdent, MetadataObject.Type objectType, NameIdentifier policyIdent)
      throws NoSuchEntityException, IOException {
    // todo: implement this method
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public List<MetadataObject> listAssociatedMetadataObjectsForPolicy(NameIdentifier policyIdent)
      throws IOException {
    // todo: implement this method
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public List<PolicyEntity> associatePoliciesWithMetadataObject(
      NameIdentifier objectIdent,
      MetadataObject.Type objectType,
      NameIdentifier[] policiesToAdd,
      NameIdentifier[] policiesToRemove)
      throws NoSuchEntityException, EntityAlreadyExistsException, IOException {
    // todo: implement this method
    throw new UnsupportedOperationException("Not implemented yet");
  }

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

  private PolicyPO getPolicyPOByMetalakeAndName(String metalakeName, String policyName) {
    PolicyPO policyPO =
        SessionUtils.getWithoutCommit(
            PolicyMetaMapper.class,
            mapper -> mapper.selectTagMetaByMetalakeAndName(metalakeName, policyName));

    if (policyPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.POLICY.name().toLowerCase(),
          policyName);
    }
    return policyPO;
  }
}
