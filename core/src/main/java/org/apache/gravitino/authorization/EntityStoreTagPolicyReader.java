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

package org.apache.gravitino.authorization;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.json.PolicyAssociationSelectorSerde;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.policy.AllValuesSelector;
import org.apache.gravitino.policy.PolicyAssociationSelector;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Reads direct tag assignments and policy bindings through the entity store's relation API. */
public final class EntityStoreTagPolicyReader implements TagPolicyReader {
  private final EntityStore entityStore;

  /**
   * Creates a reader backed by the relation store.
   *
   * @param entityStore the store to read
   */
  public EntityStoreTagPolicyReader(EntityStore entityStore) {
    this.entityStore = Objects.requireNonNull(entityStore, "entityStore must not be null");
  }

  /** {@inheritDoc} */
  @Override
  public List<TagEntity> loadTags(String metalake, MetadataObject object) {
    try {
      return entityStore
          .relationOperations()
          .listEntitiesByRelation(
              SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
              MetadataObjectUtil.toEntityIdent(metalake, object),
              MetadataObjectUtil.toEntityType(object));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read tag assignments for " + object, e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public List<ResolvedTagPolicy> loadPolicyBindings(
      String metalake, List<TagEntity> effectiveTags) {
    if (effectiveTags.isEmpty()) {
      return Collections.emptyList();
    }
    Map<NameIdentifier, TagEntity> tagsByIdentifier = new LinkedHashMap<>();
    for (TagEntity tag : effectiveTags) {
      tagsByIdentifier.put(NameIdentifierUtil.ofTag(metalake, tag.name()), tag);
    }
    try {
      List<RelationalEntity<?>> relations =
          entityStore
              .relationOperations()
              .batchListEntitiesByRelation(
                  SupportsRelationOperations.Type.POLICY_TAG_REL,
                  new ArrayList<>(tagsByIdentifier.keySet()),
                  Entity.EntityType.TAG);
      List<ResolvedTagPolicy> bindings = new ArrayList<>();
      for (RelationalEntity<?> relation : relations) {
        TagEntity tag =
            Objects.requireNonNull(
                tagsByIdentifier.get(relation.source()),
                "Policy binding has an unexpected source tag");
        // SQL NULL matches this tag with any assigned value or no value.
        PolicyAssociationSelector selector =
            relation
                .relationValue()
                .map(PolicyAssociationSelectorSerde::deserialize)
                .orElseGet(AllValuesSelector::get);
        bindings.add(new ResolvedTagPolicy(tag, (PolicyEntity) relation.targetEntity(), selector));
      }
      return List.copyOf(bindings);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read policy bindings in " + metalake, e);
    }
  }
}
