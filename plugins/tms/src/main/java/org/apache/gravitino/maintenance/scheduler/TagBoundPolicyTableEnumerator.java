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
package org.apache.gravitino.maintenance.scheduler;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.meta.GenericEntity;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.storage.relational.service.PolicyTagRelService;
import org.apache.gravitino.storage.relational.service.TagMetaService;
import org.apache.gravitino.utils.NameIdentifierUtil;

/**
 * Lists tables linked to a policy through its tag associations.
 *
 * <p>Walks {@code policy → tags → metadata objects} and keeps distinct {@link
 * Entity.EntityType#TABLE} ids. Catalog/schema inheritance is not expanded here.
 */
public final class TagBoundPolicyTableEnumerator implements PolicyTableEnumerator {

  private final PolicyTagRelService policyTagRelService;
  private final TagMetaService tagMetaService;

  /** Uses the singleton relational services. */
  public TagBoundPolicyTableEnumerator() {
    this(PolicyTagRelService.getInstance(), TagMetaService.getInstance());
  }

  /**
   * @param policyTagRelService policy-tag relation service
   * @param tagMetaService tag metadata service
   */
  public TagBoundPolicyTableEnumerator(
      PolicyTagRelService policyTagRelService, TagMetaService tagMetaService) {
    this.policyTagRelService = policyTagRelService;
    this.tagMetaService = tagMetaService;
  }

  @Override
  public List<Long> listTableIds(PolicyEntity policy, int limit) {
    if (limit <= 0) {
      return List.of();
    }
    String metalake = policy.namespace().level(0);
    NameIdentifier policyIdent = NameIdentifierUtil.ofPolicy(metalake, policy.name());
    List<RelationalEntity<?>> tagEdges =
        policyTagRelService.listRelations(List.of(policyIdent), Entity.EntityType.POLICY);
    Set<Long> tableIds = new LinkedHashSet<>();
    for (RelationalEntity<?> edge : tagEdges) {
      Object target = edge.targetEntity();
      if (!(target instanceof TagEntity)) {
        continue;
      }
      TagEntity tag = (TagEntity) target;
      try {
        List<GenericEntity> objects =
            tagMetaService.listAssociatedMetadataObjectsForTag(tag.nameIdentifier());
        for (GenericEntity object : objects) {
          if (object.type() == Entity.EntityType.TABLE) {
            tableIds.add(object.id());
            if (tableIds.size() >= limit) {
              return new ArrayList<>(tableIds);
            }
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to list objects for tag " + tag.nameIdentifier(), e);
      }
    }
    return new ArrayList<>(tableIds);
  }
}
