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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.meta.GenericEntity;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.storage.relational.service.MetadataObjectService;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;

/**
 * Rejects write paths that would place an orphan-file-removal policy with a fixed {@code location}
 * onto a non-table object, or onto a table whose storage root does not contain that location.
 */
public final class OrphanLocationPolicyGuard {

  private final EntityStore entityStore;

  /**
   * @param entityStore store used to read policy-tag and tag-object relations
   */
  public OrphanLocationPolicyGuard(EntityStore entityStore) {
    this.entityStore = entityStore;
  }

  /**
   * When associating a policy with a tag, ensures every object already carrying the tag is
   * compatible with a non-null orphan {@code location}.
   *
   * @param metalake metalake name
   * @param tagName tag that would receive the policy
   * @param policy policy being associated
   */
  public void requireCompatibleWhenAssociatingPolicy(
      String metalake, String tagName, PolicyEntity policy) {
    String location = orphanLocation(policy);
    if (location == null) {
      return;
    }
    for (MetadataObject object : listObjectsForTag(metalake, tagName)) {
      requireCompatible(metalake, policy.name(), tagName, location, object);
    }
  }

  /**
   * When assigning tags to a metadata object, ensures each tag's orphan policies with {@code
   * location} are compatible with that object.
   *
   * @param metalake metalake name
   * @param metadataObject object receiving the tags
   * @param tagNames tags being added
   */
  public void requireCompatibleWhenAssigningTags(
      String metalake, MetadataObject metadataObject, Iterable<String> tagNames) {
    for (String tagName : tagNames) {
      for (PolicyEntity policy : listPoliciesForTag(metalake, tagName)) {
        String location = orphanLocation(policy);
        if (location == null) {
          continue;
        }
        requireCompatible(metalake, policy.name(), tagName, location, metadataObject);
      }
    }
  }

  /**
   * When creating or updating policy content that sets {@code location}, ensures every object
   * reached through associated tags is compatible.
   *
   * @param metalake metalake name
   * @param policyName policy name
   * @param content new content
   */
  public void requireCompatibleWithTagPlacements(
      String metalake, String policyName, PolicyContent content) {
    String location = orphanLocation(content);
    if (location == null) {
      return;
    }
    for (String tagName : listTagNamesForPolicy(metalake, policyName)) {
      for (MetadataObject object : listObjectsForTag(metalake, tagName)) {
        requireCompatible(metalake, policyName, tagName, location, object);
      }
    }
  }

  private void requireCompatible(
      String metalake,
      String policyName,
      String tagName,
      String location,
      MetadataObject metadataObject) {
    if (metadataObject.type() != MetadataObject.Type.TABLE) {
      throw new IllegalArgumentException(
          String.format(
              "Orphan-file-removal policy %s has location %s and is associated with tag %s, which"
                  + " cannot be assigned to %s %s; only TABLE is allowed",
              policyName, location, tagName, metadataObject.type(), metadataObject));
    }

    String tableLocation = loadTableLocation(metalake, metadataObject);
    if (StringUtils.isBlank(tableLocation)) {
      throw new IllegalArgumentException(
          String.format(
              "Orphan-file-removal policy %s has location %s but table %s has no '%s' property;"
                  + " cannot verify containment for tag %s",
              policyName, location, metadataObject, Table.PROPERTY_LOCATION, tagName));
    }
    try {
      IcebergOrphanFileRemovalContent.validateLocationWithinTable(tableLocation, location);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format(
              "Orphan-file-removal policy %s location %s is not within table %s location %s"
                  + " (tag %s): %s",
              policyName, location, metadataObject, tableLocation, tagName, e.getMessage()),
          e);
    }
  }

  private static String orphanLocation(PolicyEntity policy) {
    return orphanLocation(policy.content());
  }

  private static String orphanLocation(PolicyContent content) {
    if (!(content instanceof IcebergOrphanFileRemovalContent)) {
      return null;
    }
    return ((IcebergOrphanFileRemovalContent) content).location();
  }

  private String loadTableLocation(String metalake, MetadataObject tableObject) {
    TableDispatcher dispatcher = GravitinoEnv.getInstance().internalTableDispatcher();
    if (dispatcher == null) {
      dispatcher = GravitinoEnv.getInstance().tableDispatcher();
    }
    if (dispatcher == null) {
      throw new IllegalStateException("TableDispatcher is not initialized");
    }
    NameIdentifier tableIdent = MetadataObjectUtil.toEntityIdent(metalake, tableObject);
    try {
      Table table = dispatcher.loadTable(tableIdent);
      Map<String, String> properties = table.properties();
      return properties == null ? null : properties.get(Table.PROPERTY_LOCATION);
    } catch (NoSuchTableException e) {
      throw new IllegalArgumentException(
          String.format(
              "Table %s does not exist while validating orphan location policy", tableObject),
          e);
    }
  }

  private List<MetadataObject> listObjectsForTag(String metalake, String tagName) {
    NameIdentifier tagId = NameIdentifierUtil.ofTag(metalake, tagName);
    try {
      if (!entityStore.exists(tagId, Entity.EntityType.TAG)) {
        return List.of();
      }
      List<GenericEntity> entities =
          entityStore
              .relationOperations()
              .listEntitiesByRelation(
                  SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
                  tagId,
                  Entity.EntityType.TAG);
      return MetadataObjectService.fromGenericEntities(entities);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private List<PolicyEntity> listPoliciesForTag(String metalake, String tagName) {
    NameIdentifier tagId = NameIdentifierUtil.ofTag(metalake, tagName);
    try {
      if (!entityStore.exists(tagId, Entity.EntityType.TAG)) {
        return List.of();
      }
      return entityStore
          .relationOperations()
          .batchListEntitiesByRelation(
              SupportsRelationOperations.Type.POLICY_TAG_REL, List.of(tagId), Entity.EntityType.TAG)
          .stream()
          .map(RelationalEntity::targetEntity)
          .map(PolicyEntity.class::cast)
          .toList();
    } catch (NoSuchEntityException e) {
      return List.of();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private List<String> listTagNamesForPolicy(String metalake, String policyName) {
    NameIdentifier policyId = NameIdentifierUtil.ofPolicy(metalake, policyName);
    try {
      return entityStore
          .relationOperations()
          .batchListEntitiesByRelation(
              SupportsRelationOperations.Type.POLICY_TAG_REL,
              List.of(policyId),
              Entity.EntityType.POLICY)
          .stream()
          .map(rel -> rel.targetEntity().name())
          .toList();
    } catch (NoSuchEntityException e) {
      return List.of();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
