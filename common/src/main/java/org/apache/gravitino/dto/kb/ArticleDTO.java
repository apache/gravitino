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
package org.apache.gravitino.dto.kb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.authorization.OwnerDTO;
import org.apache.gravitino.kb.ArticleObjectIdentifier;
import org.apache.gravitino.kb.ArticleStatus;
import org.apache.gravitino.kb.ArticleTermBinding;
import org.apache.gravitino.kb.KnowledgeBaseArticle;

/** Current Article metadata, including bounded previews of the caller's visible bindings. */
@Getter
public class ArticleDTO implements KnowledgeBaseArticle {
  private final String name;
  private final String title;

  @JsonSerialize(using = ToStringSerializer.class)
  private final ArticleStatus status;

  private final List<String> domains;
  @Nullable private final String comment;
  private final List<String> synonyms;
  @Nullable private final String sourceReference;
  private final Map<String, String> properties;
  private final List<ArticleTermDTO> terms;
  private final boolean termsHasMore;
  private final List<ArticleObjectDTO> objects;
  private final boolean objectsHasMore;

  @JsonInclude(JsonInclude.Include.ALWAYS)
  @Nullable
  private final OwnerDTO owner;

  private final String revisionToken;

  @JsonProperty("audit_info")
  private final AuditDTO auditInfo;

  /**
   * Creates metadata for a single revision of an Article.
   *
   * @param name the immutable Article name
   * @param title the display title
   * @param status the publication status
   * @param domains the visible Domain names
   * @param comment the optional description
   * @param synonyms other names for the topic
   * @param sourceReference the optional source reference
   * @param properties custom properties
   * @param terms the page of visible bound Terms
   * @param termsHasMore whether more visible Terms follow
   * @param objects the page of visible bound assets
   * @param objectsHasMore whether more visible assets follow
   * @param owner the current owner, if assigned
   * @param revisionToken the editing token for this snapshot
   * @param auditInfo the Article audit metadata
   */
  @JsonCreator
  public ArticleDTO(
      @JsonProperty("name") String name,
      @JsonProperty("title") String title,
      @JsonProperty("status") ArticleStatus status,
      @JsonProperty("domains") List<String> domains,
      @JsonProperty("comment") @Nullable String comment,
      @JsonProperty("synonyms") List<String> synonyms,
      @JsonProperty("sourceReference") @Nullable String sourceReference,
      @JsonProperty("properties") Map<String, String> properties,
      @JsonProperty("terms") List<ArticleTermDTO> terms,
      @JsonProperty("termsHasMore") boolean termsHasMore,
      @JsonProperty("objects") List<ArticleObjectDTO> objects,
      @JsonProperty("objectsHasMore") boolean objectsHasMore,
      @JsonProperty("owner") @Nullable OwnerDTO owner,
      @JsonProperty("revisionToken") String revisionToken,
      @JsonProperty("audit_info") AuditDTO auditInfo) {
    this.name = name;
    this.title = title;
    this.status = status;
    this.domains = ImmutableList.copyOf(domains);
    this.comment = comment;
    this.synonyms = ImmutableList.copyOf(synonyms);
    this.sourceReference = sourceReference;
    this.properties = ImmutableMap.copyOf(properties);
    this.terms = ImmutableList.copyOf(terms);
    this.termsHasMore = termsHasMore;
    this.objects = ImmutableList.copyOf(objects);
    this.objectsHasMore = objectsHasMore;
    this.owner = owner;
    this.revisionToken = revisionToken;
    this.auditInfo = auditInfo;
  }

  /** {@inheritDoc} */
  @Override
  public String name() {
    return name;
  }

  /** {@inheritDoc} */
  @Override
  public String title() {
    return title;
  }

  /** {@inheritDoc} */
  @Override
  public ArticleStatus status() {
    return status;
  }

  /** {@inheritDoc} */
  @Override
  public String[] domains() {
    return domains.toArray(new String[0]);
  }

  /** {@inheritDoc} */
  @Override
  @Nullable
  public OwnerDTO owner() {
    return owner;
  }

  /** {@inheritDoc} */
  @Override
  public AuditDTO auditInfo() {
    return auditInfo;
  }

  /** {@inheritDoc} */
  @Override
  public ArticleTermBinding[] terms() {
    return terms.toArray(new ArticleTermBinding[0]);
  }

  /** {@inheritDoc} */
  @Override
  public boolean termsHasMore() {
    return termsHasMore;
  }

  /** {@inheritDoc} */
  @Override
  @Nullable
  public String comment() {
    return comment;
  }

  /** {@inheritDoc} */
  @Override
  public String[] synonyms() {
    return synonyms.toArray(new String[0]);
  }

  /** {@inheritDoc} */
  @Override
  @Nullable
  public String sourceReference() {
    return sourceReference;
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, String> properties() {
    return properties;
  }

  /** {@inheritDoc} */
  @Override
  public ArticleObjectIdentifier[] objects() {
    return objects.stream()
        .map(ArticleObjectDTO::toIdentifier)
        .toArray(ArticleObjectIdentifier[]::new);
  }

  /** {@inheritDoc} */
  @Override
  public boolean objectsHasMore() {
    return objectsHasMore;
  }

  /** {@inheritDoc} */
  @Override
  public String revisionToken() {
    return revisionToken;
  }
}
