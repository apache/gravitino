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
package org.apache.gravitino.listener.api.event.article;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Sets;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.kb.ArticleStatus;
import org.apache.gravitino.kb.KnowledgeBaseArticleEntity;
import org.apache.gravitino.kb.KnowledgeBaseArticleEntity.Binding;
import org.apache.gravitino.listener.api.event.OperationType;

/** Metadata and effective relation changes from a committed Article mutation. */
@DeveloperApi
public final class ArticleMutationEvent extends ArticleEvent {
  private static final Comparator<Binding> BINDING_ORDER =
      Comparator.comparing(Binding::targetType).thenComparingLong(Binding::targetId);

  private final Set<Binding> addedBindings;
  private final Set<Binding> removedBindings;
  private final Set<Long> addedDomainIds;
  private final Set<Long> removedDomainIds;
  private final Map<String, String> facts;

  /**
   * Captures the differences between the full transaction snapshots using stable target IDs.
   *
   * @param user the initiating user
   * @param identifier the actual Article identifier, including a generated name on creation
   * @param operation the mutation
   * @param before the prior metadata, or null for creation
   * @param after the committed metadata, or null for deletion
   * @param bodyChanged whether stored body content or format changed
   * @param assignedOwner the initial owner assigned during creation, if any
   */
  public ArticleMutationEvent(
      String user,
      NameIdentifier identifier,
      OperationType operation,
      @Nullable KnowledgeBaseArticleEntity before,
      @Nullable KnowledgeBaseArticleEntity after,
      boolean bodyChanged,
      @Nullable String assignedOwner) {
    super(user, identifier, operation);
    KnowledgeBaseArticleEntity article =
        after != null ? after : Preconditions.checkNotNull(before, "before");
    Set<Binding> oldBindings = before == null ? Set.of() : before.bindings();
    Set<Binding> newBindings = after == null ? Set.of() : after.bindings();
    addedBindings =
        ImmutableSortedSet.copyOf(BINDING_ORDER, Sets.difference(newBindings, oldBindings));
    removedBindings =
        ImmutableSortedSet.copyOf(BINDING_ORDER, Sets.difference(oldBindings, newBindings));
    Set<Long> oldDomains = before == null ? Set.of() : before.domainIds();
    Set<Long> newDomains = after == null ? Set.of() : after.domainIds();
    addedDomainIds = ImmutableSortedSet.copyOf(Sets.difference(newDomains, oldDomains));
    removedDomainIds = ImmutableSortedSet.copyOf(Sets.difference(oldDomains, newDomains));

    ImmutableMap.Builder<String, String> audit = ImmutableMap.builder();
    audit.put("articleId", Long.toString(article.id()));
    audit.put("revision", Long.toString(article.revision()));
    // Reserve the change fields even when empty. Event merges these facts with request parameters.
    audit.put("bodyChanged", Boolean.toString(bodyChanged));
    audit.put("assignedOwner", assignedOwner == null ? "" : assignedOwner);
    ArticleStatus oldStatus = before == null ? null : before.status();
    ArticleStatus newStatus = after == null ? null : after.status();
    audit.put("previousStatus", oldStatus == null ? "" : oldStatus.name());
    audit.put("status", newStatus == null ? "" : newStatus.name());
    ImmutableSet.Builder<String> fields = ImmutableSet.builder();
    if (before != null && after != null) {
      if (!Objects.equals(before.title(), after.title())) {
        fields.add("title");
      }
      if (!Objects.equals(before.comment(), after.comment())) {
        fields.add("comment");
      }
      if (!before.synonyms().equals(after.synonyms())) {
        fields.add("synonyms");
      }
      if (!Objects.equals(before.sourceReference(), after.sourceReference())) {
        fields.add("sourceReference");
      }
      if (!before.properties().equals(after.properties())) {
        fields.add("properties");
      }
    }
    audit.put("changedFields", String.join(",", fields.build()));
    audit.put("addedBindings", targets(addedBindings));
    audit.put("removedBindings", targets(removedBindings));
    audit.put("addedDomainIds", addedDomainIds.toString());
    audit.put("removedDomainIds", removedDomainIds.toString());
    facts = audit.build();
  }

  /**
   * @return newly bound Term and asset identities
   */
  public Set<Binding> addedBindings() {
    return addedBindings;
  }

  /**
   * @return Term and asset identities removed by the mutation
   */
  public Set<Binding> removedBindings() {
    return removedBindings;
  }

  /**
   * @return the stable IDs of Domains joined by the Article
   */
  public Set<Long> addedDomainIds() {
    return addedDomainIds;
  }

  /**
   * @return the stable IDs of Domains the Article left
   */
  public Set<Long> removedDomainIds() {
    return removedDomainIds;
  }

  @Override
  protected Map<String, String> ownCustomInfo() {
    return facts;
  }

  private static String targets(Set<Binding> bindings) {
    return bindings.stream()
        .map(binding -> binding.targetType() + ":" + binding.targetId())
        .collect(Collectors.joining(","));
  }
}
