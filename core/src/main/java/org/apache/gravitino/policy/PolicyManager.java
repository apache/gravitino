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

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Set;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchPolicyException;
import org.apache.gravitino.exceptions.PolicyAlreadyExistsException;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLockUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unused") // todo: remove this when all the methods are implemented
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
    return Arrays.stream(listPolicyInfos(metalake)).map(Policy::name).toArray(String[]::new);
  }

  @Override
  public Policy[] listPolicyInfos(String metalake) {
    NameIdentifier metalakeIdent = NameIdentifierUtil.ofMetalake(metalake);
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(NamespaceUtil.ofPolicy(metalake).levels()),
        LockType.READ,
        () -> {
          checkMetalake(metalakeIdent, entityStore);

          try {
            return entityStore
                .list(
                    NamespaceUtil.ofPolicy(metalake), PolicyEntity.class, Entity.EntityType.POLICY)
                .toArray(new Policy[0]);
          } catch (IOException ioe) {
            LOG.error("Failed to list policies under metalake {}", metalake, ioe);
            throw new RuntimeException(ioe);
          }
        });
  }

  @Override
  public Policy getPolicy(String metalake, String policyName) throws NoSuchPolicyException {
    return TreeLockUtils.doWithTreeLock(
        NameIdentifierUtil.ofPolicy(metalake, policyName),
        LockType.READ,
        () -> {
          checkMetalake(NameIdentifier.of(metalake), entityStore);

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
  public Policy createPolicy(
      String metalake,
      String policyName,
      String type,
      String comment,
      boolean enabled,
      boolean exclusive,
      boolean inheritable,
      Set<MetadataObject.Type> supportedObjectTypes,
      PolicyContent content)
      throws PolicyAlreadyExistsException {
    NameIdentifier metalakeIdent = NameIdentifierUtil.ofMetalake(metalake);
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(NamespaceUtil.ofPolicy(metalake).levels()),
        LockType.WRITE,
        () -> {
          checkMetalake(metalakeIdent, entityStore);

          PolicyEntity policyEntity =
              PolicyEntity.builder()
                  .withId(idGenerator.nextId())
                  .withName(policyName)
                  .withNamespace(NamespaceUtil.ofPolicy(metalake))
                  .withComment(comment)
                  .withPolicyType(type)
                  .withEnabled(enabled)
                  .withExclusive(exclusive)
                  .withInheritable(inheritable)
                  .withSupportedObjectTypes(supportedObjectTypes)
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
  public Policy alterPolicy(String metalake, String policyName, PolicyChange... changes) {
    NameIdentifier metalakeIdent = NameIdentifierUtil.ofMetalake(metalake);
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(NamespaceUtil.ofPolicy(metalake).levels()),
        LockType.WRITE,
        () -> {
          checkMetalake(metalakeIdent, entityStore);

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
                "Policy with name "
                    + policyName
                    + " under metalake "
                    + metalake
                    + " already exists");
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
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(NamespaceUtil.ofPolicy(metalake).levels()),
        LockType.WRITE,
        () -> {
          checkMetalake(metalakeIdent, entityStore);

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
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public String[] listPoliciesForMetadataObject(String metalake, MetadataObject metadataObject) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Policy[] listPolicyInfosForMetadataObject(String metalake, MetadataObject metadataObject) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public String[] associatePoliciesForMetadataObject(
      String metalake,
      MetadataObject metadataObject,
      String[] policiesToAdd,
      String[] policiesToRemove) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Policy getPolicyForMetadataObject(
      String metalake, MetadataObject metadataObject, String policyName) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  private void changePolicyEnabledState(
      String metalake, String policyName, boolean expectedEnabledState) {
    NameIdentifier metalakeIdent = NameIdentifierUtil.ofMetalake(metalake);
    TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(NamespaceUtil.ofPolicy(metalake).levels()),
        LockType.WRITE,
        () -> {
          checkMetalake(metalakeIdent, entityStore);
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
        .withExclusive(policyEntity.exclusive())
        .withInheritable(policyEntity.inheritable())
        .withSupportedObjectTypes(policyEntity.supportedObjectTypes())
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
