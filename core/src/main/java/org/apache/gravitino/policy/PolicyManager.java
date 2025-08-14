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

package org.apache.gravitino.policy;

import static org.apache.gravitino.metalake.MetalakeManager.checkMetalake;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchPolicyException;
import org.apache.gravitino.exceptions.PolicyAlreadyAssociatedException;
import org.apache.gravitino.exceptions.PolicyAlreadyExistsException;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLockUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.GenericEntity;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.relational.service.MetadataObjectService;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PolicyManager implements PolicyDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(PolicyManager.class);

  private final IdGenerator idGenerator;
  private final EntityStore entityStore;

  public PolicyManager(IdGenerator idGenerator, EntityStore entityStore) {
    if (!(entityStore instanceof SupportsRelationOperations)) {
      String errorMsg =
          "PolicyManager cannot run with entity store that does not support policy operations, "
              + "please configure the entity store to use relational entity store and restart the Gravitino server";
      LOG.error(errorMsg);
      throw new RuntimeException(errorMsg);
    }

    this.idGenerator = idGenerator;
    this.entityStore = entityStore;
  }

  @Override
  public String[] listPolicies(String metalake) {
    return Arrays.stream(listPolicyInfos(metalake)).map(PolicyEntity::name).toArray(String[]::new);
  }

  @Override
  public PolicyEntity[] listPolicyInfos(String metalake) {
    NameIdentifier metalakeIdent = NameIdentifierUtil.ofMetalake(metalake);
    checkMetalake(metalakeIdent, entityStore);
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(NamespaceUtil.ofPolicy(metalake).levels()),
        LockType.READ,
        () -> {
          try {
            return entityStore
                .list(
                    NamespaceUtil.ofPolicy(metalake), PolicyEntity.class, Entity.EntityType.POLICY)
                .toArray(new PolicyEntity[0]);
          } catch (IOException ioe) {
            LOG.error("Failed to list policies under metalake {}", metalake, ioe);
            throw new RuntimeException(ioe);
          }
        });
  }

  @Override
  public PolicyEntity getPolicy(String metalake, String policyName) throws NoSuchPolicyException {
    checkMetalake(NameIdentifier.of(metalake), entityStore);
    return TreeLockUtils.doWithTreeLock(
        NameIdentifierUtil.ofPolicy(metalake, policyName),
        LockType.READ,
        () -> {
          try {
            return entityStore.get(
                NameIdentifierUtil.ofPolicy(metalake, policyName),
                Entity.EntityType.POLICY,
                PolicyEntity.class);
          } catch (NoSuchEntityException e) {
            throw new NoSuchPolicyException(
                "Policy with name %s under metalake %s does not exist", policyName, metalake);
          } catch (IOException ioe) {
            LOG.error("Failed to get policy {} under metalake {}", policyName, metalake, ioe);
            throw new RuntimeException(ioe);
          }
        });
  }

  @Override
  public PolicyEntity createPolicy(
      String metalake,
      String policyName,
      Policy.BuiltInType type,
      String comment,
      boolean enabled,
      PolicyContent content)
      throws PolicyAlreadyExistsException {
    NameIdentifier metalakeIdent = NameIdentifierUtil.ofMetalake(metalake);
    checkMetalake(metalakeIdent, entityStore);
    return TreeLockUtils.doWithTreeLock(
        NameIdentifierUtil.ofPolicy(metalake, policyName),
        LockType.WRITE,
        () -> {
          PolicyEntity policyEntity =
              PolicyEntity.builder()
                  .withId(idGenerator.nextId())
                  .withName(policyName)
                  .withNamespace(NamespaceUtil.ofPolicy(metalake))
                  .withComment(comment)
                  .withPolicyType(type)
                  .withEnabled(enabled)
                  .withContent(content)
                  .withAuditInfo(
                      AuditInfo.builder()
                          .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                          .withCreateTime(Instant.now())
                          .build())
                  .build();

          try {
            entityStore.put(policyEntity, false /* overwritten */);
            return policyEntity;
          } catch (EntityAlreadyExistsException e) {
            throw new PolicyAlreadyExistsException(
                "Policy with name %s under metalake %s already exists", policyName, metalake);
          } catch (IOException ioe) {
            LOG.error("Failed to create policy {} under metalake {}", policyName, metalake, ioe);
            throw new RuntimeException(ioe);
          }
        });
  }

  @Override
  public PolicyEntity alterPolicy(String metalake, String policyName, PolicyChange... changes) {
    NameIdentifier metalakeIdent = NameIdentifierUtil.ofMetalake(metalake);
    checkMetalake(metalakeIdent, entityStore);
    return TreeLockUtils.doWithTreeLock(
        NameIdentifierUtil.ofPolicy(metalake, policyName),
        LockType.WRITE,
        () -> {
          try {
            return entityStore.update(
                NameIdentifierUtil.ofPolicy(metalake, policyName),
                PolicyEntity.class,
                Entity.EntityType.POLICY,
                policyEntity -> updatePolicyEntity(policyEntity, changes));
          } catch (NoSuchEntityException e) {
            throw new NoSuchPolicyException(
                "Policy with name %s under metalake %s does not exist", policyName, metalake);
          } catch (EntityAlreadyExistsException e) {
            throw new RuntimeException(
                String.format(
                    "Trying to alter policy %s under metalake %s, but the new name already exists",
                    policyName, metalake));
          } catch (IOException ioe) {
            LOG.error("Failed to alter policy {} under metalake {}", policyName, metalake, ioe);
            throw new RuntimeException(ioe);
          }
        });
  }

  @Override
  public void enablePolicy(String metalake, String policyName) throws NoSuchPolicyException {
    changePolicyEnabledState(metalake, policyName, true);
  }

  @Override
  public void disablePolicy(String metalake, String policyName) throws NoSuchPolicyException {
    changePolicyEnabledState(metalake, policyName, false);
  }

  @Override
  public boolean deletePolicy(String metalake, String policyName) {
    NameIdentifier metalakeIdent = NameIdentifierUtil.ofMetalake(metalake);
    checkMetalake(metalakeIdent, entityStore);
    return TreeLockUtils.doWithTreeLock(
        NameIdentifierUtil.ofPolicy(metalake, policyName),
        LockType.WRITE,
        () -> {
          try {
            return entityStore.delete(
                NameIdentifierUtil.ofPolicy(metalake, policyName), Entity.EntityType.POLICY);
          } catch (IOException ioe) {
            LOG.error("Failed to delete policy {} under metalake {}", policyName, metalake, ioe);
            throw new RuntimeException(ioe);
          }
        });
  }

  @Override
  public MetadataObject[] listMetadataObjectsForPolicy(String metalake, String policyName) {
    NameIdentifier policyIdent = NameIdentifierUtil.ofPolicy(metalake, policyName);
    checkMetalake(NameIdentifier.of(metalake), entityStore);

    return TreeLockUtils.doWithTreeLock(
        policyIdent,
        LockType.READ,
        () -> {
          try {
            if (!entityStore.exists(policyIdent, Entity.EntityType.POLICY)) {
              throw new NoSuchPolicyException(
                  "Policy with name %s under metalake %s does not exist", policyName, metalake);
            }

            List<GenericEntity> entities =
                entityStore
                    .relationOperations()
                    .listEntitiesByRelation(
                        SupportsRelationOperations.Type.POLICY_METADATA_OBJECT_REL,
                        policyIdent,
                        Entity.EntityType.POLICY);
            return MetadataObjectService.fromGenericEntities(entities)
                .toArray(new MetadataObject[0]);
          } catch (IOException e) {
            LOG.error("Failed to list metadata objects for policy {}", policyName, e);
            throw new RuntimeException(e);
          }
        });
  }

  @Override
  public PolicyEntity[] listPolicyInfosForMetadataObject(
      String metalake, MetadataObject metadataObject) {
    NameIdentifier entityIdent = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);
    Entity.EntityType entityType = MetadataObjectUtil.toEntityType(metadataObject);

    MetadataObjectUtil.checkMetadataObject(metalake, metadataObject);
    checkMetalake(NameIdentifier.of(metalake), entityStore);

    return TreeLockUtils.doWithTreeLock(
        entityIdent,
        LockType.READ,
        () -> {
          try {
            return entityStore.relationOperations()
                .listEntitiesByRelation(
                    SupportsRelationOperations.Type.POLICY_METADATA_OBJECT_REL,
                    entityIdent,
                    entityType,
                    true /* allFields */)
                .stream()
                .map(entity -> (PolicyEntity) entity)
                .toArray(PolicyEntity[]::new);
          } catch (NoSuchEntityException e) {
            throw new NoSuchMetadataObjectException(
                e,
                "Failed to list policies for metadata object %s due to not found",
                metadataObject);
          } catch (IOException e) {
            LOG.error("Failed to list policies for metadata object {}", metadataObject, e);
            throw new RuntimeException(e);
          }
        });
  }

  @Override
  public String[] associatePoliciesForMetadataObject(
      String metalake,
      MetadataObject metadataObject,
      String[] policiesToAdd,
      String[] policiesToRemove) {
    Preconditions.checkArgument(
        !metadataObject.type().equals(MetadataObject.Type.METALAKE)
            && !metadataObject.type().equals(MetadataObject.Type.ROLE)
            && !metadataObject.type().equals(MetadataObject.Type.COLUMN),
        "Cannot associate policies for unsupported metadata object type %s",
        metadataObject.type());

    NameIdentifier entityIdent = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);
    Entity.EntityType entityType = MetadataObjectUtil.toEntityType(metadataObject);

    MetadataObjectUtil.checkMetadataObject(metalake, metadataObject);

    // Remove all the policies that are both set to add and remove
    Set<String> policiesToAddSet =
        policiesToAdd == null ? Sets.newHashSet() : Sets.newHashSet(policiesToAdd);
    Set<String> policiesToRemoveSet =
        policiesToRemove == null ? Sets.newHashSet() : Sets.newHashSet(policiesToRemove);
    Set<String> common = Sets.intersection(policiesToAddSet, policiesToRemoveSet).immutableCopy();
    policiesToAddSet.removeAll(common);
    policiesToRemoveSet.removeAll(common);

    NameIdentifier[] policiesToAddIdent =
        policiesToAddSet.stream()
            .map(p -> NameIdentifierUtil.ofPolicy(metalake, p))
            .toArray(NameIdentifier[]::new);
    NameIdentifier[] policiesToRemoveIdent =
        policiesToRemoveSet.stream()
            .map(p -> NameIdentifierUtil.ofPolicy(metalake, p))
            .toArray(NameIdentifier[]::new);

    checkMetalake(NameIdentifier.of(metalake), entityStore);
    return TreeLockUtils.doWithTreeLock(
        entityIdent,
        LockType.READ,
        () ->
            TreeLockUtils.doWithTreeLock(
                NameIdentifier.of(NamespaceUtil.ofPolicy(metalake).levels()),
                LockType.WRITE,
                () -> {
                  try {
                    List<PolicyEntity> updatedPolicies =
                        entityStore
                            .relationOperations()
                            .updateEntityRelations(
                                SupportsRelationOperations.Type.POLICY_METADATA_OBJECT_REL,
                                entityIdent,
                                entityType,
                                policiesToAddIdent,
                                policiesToRemoveIdent);
                    return updatedPolicies.stream().map(PolicyEntity::name).toArray(String[]::new);
                  } catch (NoSuchEntityException e) {
                    throw new NoSuchMetadataObjectException(
                        e,
                        "Failed to associate policies for metadata object %s due to not found",
                        metadataObject);
                  } catch (EntityAlreadyExistsException e) {
                    throw new PolicyAlreadyAssociatedException(
                        e,
                        "Failed to associate policies for metadata object due to some policies %s already "
                            + "associated to the metadata object %s",
                        Arrays.toString(policiesToAdd),
                        metadataObject);
                  } catch (IOException e) {
                    LOG.error(
                        "Failed to associate policies for metadata object {}", metadataObject, e);
                    throw new RuntimeException(e);
                  }
                }));
  }

  @Override
  public PolicyEntity getPolicyForMetadataObject(
      String metalake, MetadataObject metadataObject, String policyName) {
    NameIdentifier entityIdent = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);
    Entity.EntityType entityType = MetadataObjectUtil.toEntityType(metadataObject);
    NameIdentifier policyIdent = NameIdentifierUtil.ofPolicy(metalake, policyName);

    MetadataObjectUtil.checkMetadataObject(metalake, metadataObject);
    checkMetalake(NameIdentifier.of(metalake), entityStore);

    return TreeLockUtils.doWithTreeLock(
        entityIdent,
        LockType.READ,
        () -> {
          try {
            return entityStore
                .relationOperations()
                .getEntityByRelation(
                    SupportsRelationOperations.Type.POLICY_METADATA_OBJECT_REL,
                    entityIdent,
                    entityType,
                    policyIdent);
          } catch (NoSuchEntityException e) {
            if (e.getMessage().contains("No such policy entity")) {
              throw new NoSuchPolicyException(
                  e, "Policy %s does not exist for metadata object %s", policyName, metadataObject);
            } else {
              throw new NoSuchMetadataObjectException(
                  e,
                  "Failed to get policy for metadata object %s due to not found",
                  metadataObject);
            }
          } catch (IOException e) {
            LOG.error("Failed to get policy for metadata object {}", metadataObject, e);
            throw new RuntimeException(e);
          }
        });
  }

  private void changePolicyEnabledState(
      String metalake, String policyName, boolean expectedEnabledState) {
    NameIdentifier metalakeIdent = NameIdentifierUtil.ofMetalake(metalake);
    checkMetalake(metalakeIdent, entityStore);
    TreeLockUtils.doWithTreeLock(
        NameIdentifierUtil.ofPolicy(metalake, policyName),
        LockType.WRITE,
        () -> {
          if (policyEnabled(metalake, policyName) == expectedEnabledState) {
            return null;
          }

          try {
            entityStore.update(
                NameIdentifierUtil.ofPolicy(metalake, policyName),
                PolicyEntity.class,
                Entity.EntityType.POLICY,
                policyEntity -> {
                  PolicyEntity.Builder builder = newPolicyBuilder(policyEntity);
                  builder.withEnabled(expectedEnabledState);
                  return builder.build();
                });
            return null;
          } catch (IOException ioe) {
            LOG.error(
                "Failed to change policy {} enabled state under metalake {}",
                policyName,
                metalake,
                ioe);
            throw new RuntimeException(ioe);
          }
        });
  }

  private PolicyEntity.Builder newPolicyBuilder(PolicyEntity policyEntity) {
    return PolicyEntity.builder()
        .withId(policyEntity.id())
        .withName(policyEntity.name())
        .withNamespace(policyEntity.namespace())
        .withComment(policyEntity.comment())
        .withPolicyType(policyEntity.policyType())
        .withEnabled(policyEntity.enabled())
        .withContent(policyEntity.content())
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator(policyEntity.auditInfo().creator())
                .withCreateTime(policyEntity.auditInfo().createTime())
                .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                .withLastModifiedTime(Instant.now())
                .build());
  }

  private boolean policyEnabled(String metalake, String policyName) throws NoSuchPolicyException {
    NameIdentifier policyIdent = NameIdentifierUtil.ofPolicy(metalake, policyName);
    try {
      PolicyEntity policyEntity =
          entityStore.get(policyIdent, Entity.EntityType.POLICY, PolicyEntity.class);
      return policyEntity.enabled();
    } catch (NoSuchEntityException e) {
      throw new NoSuchPolicyException(
          "Policy with name %s under metalake %s does not exist", policyName, metalake);
    } catch (IOException ioe) {
      LOG.error("Failed to get policy {} under metalake {}", policyName, metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  private PolicyEntity updatePolicyEntity(PolicyEntity policyEntity, PolicyChange... changes) {
    String newName = policyEntity.name();
    String newComment = policyEntity.comment();
    PolicyContent newContent = policyEntity.content();

    for (PolicyChange change : changes) {
      if (change instanceof PolicyChange.RenamePolicy) {
        newName = ((PolicyChange.RenamePolicy) change).getNewName();
      } else if (change instanceof PolicyChange.UpdatePolicyComment) {
        newComment = ((PolicyChange.UpdatePolicyComment) change).getNewComment();
      } else if (change instanceof PolicyChange.UpdateContent) {
        PolicyChange.UpdateContent updateContent = (PolicyChange.UpdateContent) change;
        Policy.BuiltInType policyType =
            Policy.BuiltInType.fromPolicyType(updateContent.getPolicyType());
        Preconditions.checkArgument(
            policyEntity.policyType() == policyType,
            "Policy type mismatch: expected %s but got %s",
            policyEntity.policyType(),
            updateContent.getPolicyType());

        if (policyType != Policy.BuiltInType.CUSTOM) {
          // cannot change the supported object types for built-in policies
          Preconditions.checkArgument(
              Sets.difference(
                      policyEntity.content().supportedObjectTypes(),
                      updateContent.getContent().supportedObjectTypes())
                  .isEmpty(),
              "Policy content type mismatch: expected %s but got %s");
        }

        newContent = updateContent.getContent();
      } else {
        throw new IllegalArgumentException("Unsupported policy change: " + change);
      }
    }

    PolicyEntity.Builder builder = newPolicyBuilder(policyEntity);
    builder.withName(newName);
    builder.withComment(newComment);
    builder.withContent(newContent);

    return builder.build();
  }
}
