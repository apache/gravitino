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
package org.apache.gravitino.kb;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Field;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.AuditInfo;

/**
 * Article metadata and stable binding identities. A write may also carry a replacement body. Read
 * results and transaction hooks contain metadata only.
 */
@Getter
@Accessors(fluent = true)
@EqualsAndHashCode(exclude = {"auditInfo", "bodyUpdate", "domains"})
public final class KnowledgeBaseArticleEntity implements Entity, HasIdentifier, Auditable {
  /** Stable identity of a bound Term or asset. */
  public record Binding(String targetType, long targetId) {
    private static final Set<String> TYPES =
        Set.of(
            "GLOSSARY_TERM",
            "CATALOG",
            "SCHEMA",
            "TABLE",
            "VIEW",
            "FILESET",
            "TOPIC",
            "MODEL",
            "SEMANTIC_MODEL",
            "COLUMN");

    /** Validates the stored target type. */
    public Binding {
      Preconditions.checkArgument(
          targetType != null && TYPES.contains(targetType),
          "Unsupported Article target type: %s",
          targetType);
    }
  }

  private static final Field ID = Field.required("id", Long.class, "Article id");
  private static final Field NAME = Field.required("name", String.class, "Article name");
  private static final Field TITLE = Field.required("title", String.class, "Article title");
  private static final Field STATUS =
      Field.required("status", ArticleStatus.class, "Article status");
  private static final Field COMMENT = Field.optional("comment", String.class, "Article comment");
  private static final Field SYNONYMS = Field.required("synonyms", List.class, "Article synonyms");
  private static final Field SOURCE_REFERENCE =
      Field.optional("sourceReference", String.class, "Article sourceReference");
  private static final Field PROPERTIES =
      Field.required("properties", Map.class, "Article properties");
  private static final Field REVISION = Field.required("revision", Long.class, "Article revision");
  private static final Field DOMAIN_IDS =
      Field.required("domainIds", Set.class, "Stable Domain IDs");
  private static final Field BINDINGS = Field.required("bindings", Set.class, "Article bindings");
  private static final Field AUDIT_INFO =
      Field.required("auditInfo", AuditInfo.class, "Article auditInfo");
  private final Namespace namespace;
  private final Long id;
  private final String name;
  private final String title;
  /** Body supplied on create or explicit replacement. Absent from persisted metadata results. */
  @Nullable private final ArticleBody bodyUpdate;

  private final ArticleStatus status;
  @Nullable private final String comment;
  private final List<String> synonyms;
  @Nullable private final String sourceReference;
  private final Map<String, String> properties;
  private final long revision;
  private final Set<Long> domainIds;
  /** Current Domain names visible to the caller in manager results. */
  private final List<String> domains;

  private final Set<Binding> bindings;
  private final AuditInfo auditInfo;

  @Builder(toBuilder = true, setterPrefix = "with")
  private KnowledgeBaseArticleEntity(
      Namespace namespace,
      Long id,
      String name,
      String title,
      @Nullable ArticleBody bodyUpdate,
      ArticleStatus status,
      @Nullable String comment,
      List<String> synonyms,
      @Nullable String sourceReference,
      Map<String, String> properties,
      long revision,
      Set<Long> domainIds,
      List<String> domains,
      Set<Binding> bindings,
      AuditInfo auditInfo) {
    Preconditions.checkArgument(
        namespace != null && namespace.length() == 1,
        "Expected a metalake namespace: %s",
        namespace);
    this.namespace = namespace;
    this.id = Preconditions.checkNotNull(id, "id");
    this.name = KnowledgeBaseArticle.normalizeName(name);
    this.title = ArticleValidation.title(title);
    this.bodyUpdate = bodyUpdate;
    this.status = status == null ? ArticleStatus.PUBLISHED : status;
    this.comment =
        ArticleValidation.length(comment, KnowledgeBaseArticle.COMMENT_MAX_LENGTH, "Comment");
    this.synonyms = synonyms == null ? List.of() : ArticleValidation.synonyms(synonyms);
    this.sourceReference =
        ArticleValidation.length(
            sourceReference, KnowledgeBaseArticle.SOURCE_REFERENCE_MAX_LENGTH, "Source reference");
    this.properties = properties == null ? Map.of() : Map.copyOf(properties);
    Preconditions.checkArgument(revision >= 0, "Negative revision");
    this.revision = revision == 0 ? 1 : revision;
    this.domainIds = domainIds == null ? Set.of() : Set.copyOf(domainIds);
    this.domains = domains == null ? List.of() : List.copyOf(domains);
    this.bindings = bindings == null ? Set.of() : Set.copyOf(bindings);
    this.auditInfo = Preconditions.checkNotNull(auditInfo, "auditInfo");
  }

  @Override
  public EntityType type() {
    return EntityType.KNOWLEDGE_BASE_ARTICLE;
  }

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> values = new LinkedHashMap<>();
    values.put(ID, id);
    values.put(NAME, name);
    values.put(TITLE, title);
    values.put(STATUS, status);
    values.put(COMMENT, comment);
    values.put(SYNONYMS, synonyms);
    values.put(SOURCE_REFERENCE, sourceReference);
    values.put(PROPERTIES, properties);
    values.put(REVISION, revision);
    values.put(DOMAIN_IDS, domainIds);
    values.put(BINDINGS, bindings);
    values.put(AUDIT_INFO, auditInfo);
    return Collections.unmodifiableMap(values);
  }
}
