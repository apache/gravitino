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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.annotation.Evolving;

/**
 * The request for creating a Knowledge Base Article together with its first version and its initial
 * Domains and asset bindings, matching the single-save create form.
 *
 * <p>{@code name} is optional; when it is omitted the server derives it from the title. Term
 * bindings are added later once the Glossary API is available.
 */
@Evolving
public class ArticleCreate {

  private final String name;
  private final String title;
  private final ArticleBody body;
  private final List<String> domains;
  private final ArticleStatus status;
  private final String comment;
  private final List<String> synonyms;
  private final String sourceReference;
  private final Map<String, String> properties;
  private final List<ArticleObjectIdentifier> objects;

  /**
   * @return a new create builder
   */
  public static Builder builder() {
    return new Builder();
  }

  private ArticleCreate(Builder builder) {
    this.name = ArticleValidation.name(builder.name);
    this.title = ArticleValidation.title(builder.title);
    this.body = builder.body;
    this.domains = ArticleValidation.domainNames(builder.domains.build());
    this.status = builder.status;
    this.comment =
        ArticleValidation.length(
            builder.comment, KnowledgeBaseArticle.COMMENT_MAX_LENGTH, "Comment");
    this.synonyms = ArticleValidation.synonyms(builder.synonyms.build());
    this.sourceReference =
        ArticleValidation.length(
            builder.sourceReference,
            KnowledgeBaseArticle.SOURCE_REFERENCE_MAX_LENGTH,
            "Source reference");
    this.properties = builder.properties.build();
    this.objects = builder.objects.build();
  }

  /**
   * @return the explicit name, or {@code null} to derive it from the title
   */
  @Nullable
  public String name() {
    return name;
  }

  /**
   * @return the title
   */
  public String title() {
    return title;
  }

  /**
   * @return the body of the first version
   */
  public ArticleBody body() {
    return body;
  }

  /**
   * @return the initial Domain names
   */
  public List<String> domains() {
    return domains;
  }

  /**
   * @return the requested initial status, or {@code null} for the default of APPROVED
   */
  @Nullable
  public ArticleStatus status() {
    return status;
  }

  /**
   * @return the short comment, or {@code null}
   */
  @Nullable
  public String comment() {
    return comment;
  }

  /**
   * @return the synonyms
   */
  public List<String> synonyms() {
    return synonyms;
  }

  /**
   * @return the source reference, or {@code null}
   */
  @Nullable
  public String sourceReference() {
    return sourceReference;
  }

  /**
   * @return the custom properties
   */
  public Map<String, String> properties() {
    return properties;
  }

  /**
   * @return the initially bound metadata objects
   */
  public List<ArticleObjectIdentifier> objects() {
    return objects;
  }

  /** Builder for {@link ArticleCreate}. */
  public static class Builder {
    private String name;
    private String title;
    private ArticleBody body;
    private final ImmutableList.Builder<String> domains = ImmutableList.builder();
    private ArticleStatus status;
    private String comment;
    private final ImmutableList.Builder<String> synonyms = ImmutableList.builder();
    private String sourceReference;
    private final ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
    private final ImmutableList.Builder<ArticleObjectIdentifier> objects = ImmutableList.builder();

    private Builder() {}

    /**
     * Sets the explicit name; when omitted the server derives the name from the title. The server
     * validates the name's allowed characters.
     *
     * @param name the article name
     * @return this builder
     */
    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Sets the required title.
     *
     * @param title the title
     * @return this builder
     */
    public Builder withTitle(String title) {
      this.title = title;
      return this;
    }

    /**
     * Sets the required body.
     *
     * @param body the body
     * @return this builder
     */
    public Builder withBody(ArticleBody body) {
      this.body = body;
      return this;
    }

    /**
     * Adds a Domain by name.
     *
     * @param domain the Domain name
     * @return this builder
     */
    public Builder addDomain(String domain) {
      this.domains.add(domain);
      return this;
    }

    /**
     * Adds Domains by name.
     *
     * @param domains the Domain names
     * @return this builder
     */
    public Builder addDomains(String... domains) {
      this.domains.addAll(Arrays.asList(domains));
      return this;
    }

    /**
     * Sets the requested initial status.
     *
     * @param status the status
     * @return this builder
     */
    public Builder withStatus(ArticleStatus status) {
      this.status = status;
      return this;
    }

    /**
     * Sets the short comment.
     *
     * @param comment the comment
     * @return this builder
     */
    public Builder withComment(String comment) {
      this.comment = comment;
      return this;
    }

    /**
     * Adds a synonym.
     *
     * @param synonym the synonym
     * @return this builder
     */
    public Builder addSynonym(String synonym) {
      this.synonyms.add(synonym);
      return this;
    }

    /**
     * Adds synonyms.
     *
     * @param synonyms the synonyms
     * @return this builder
     */
    public Builder addSynonyms(String... synonyms) {
      this.synonyms.addAll(Arrays.asList(synonyms));
      return this;
    }

    /**
     * Sets the source reference.
     *
     * @param sourceReference where the article came from
     * @return this builder
     */
    public Builder withSourceReference(String sourceReference) {
      this.sourceReference = sourceReference;
      return this;
    }

    /**
     * Adds a custom property.
     *
     * @param key the property name
     * @param value the property value
     * @return this builder
     */
    public Builder withProperty(String key, String value) {
      this.properties.put(key, value);
      return this;
    }

    /**
     * Adds custom properties.
     *
     * @param properties the properties
     * @return this builder
     */
    public Builder withProperties(Map<String, String> properties) {
      this.properties.putAll(properties);
      return this;
    }

    /**
     * Adds an initially bound metadata object.
     *
     * @param object the bound object
     * @return this builder
     */
    public Builder addObject(ArticleObjectIdentifier object) {
      this.objects.add(object);
      return this;
    }

    /**
     * Adds initially bound metadata objects.
     *
     * @param objects the bound objects
     * @return this builder
     */
    public Builder addObjects(ArticleObjectIdentifier... objects) {
      this.objects.addAll(Arrays.asList(objects));
      return this;
    }

    /**
     * Builds the request.
     *
     * @return the create request
     */
    public ArticleCreate build() {
      Preconditions.checkArgument(body != null, "Article body must not be null");
      return new ArticleCreate(this);
    }
  }
}
