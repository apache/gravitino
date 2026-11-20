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
import javax.annotation.Nullable;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.authorization.Owner;

/**
 * A filter for listing Knowledge Base Articles. All fields are optional; {@link #builder()} returns
 * a filter that matches every article visible to the caller.
 */
@Evolving
public class ArticleListFilter {

  /** Default page size used when no {@code limit} is supplied. */
  public static final int DEFAULT_LIMIT = 20;

  /** Maximum number of articles that can be requested in one page. */
  public static final int MAX_LIMIT = 1_000;

  private final String query;
  private final String domain;
  private final ArticleStatus status;
  private final Owner owner;
  private final int limit;
  private final int offset;

  /**
   * @return a new filter builder
   */
  public static Builder builder() {
    return new Builder();
  }

  private ArticleListFilter(Builder builder) {
    this.query = builder.query;
    this.domain = builder.domain;
    this.status = builder.status;
    this.owner = builder.owner;
    this.limit = builder.limit;
    this.offset = builder.offset;
  }

  /**
   * @return the substring matched against the title, the synonyms and the body, or {@code null}
   */
  @Nullable
  public String query() {
    return query;
  }

  /**
   * @return the Domain name filter; a parent Domain also returns articles in its caller-readable
   *     subdomains, or {@code null}
   */
  @Nullable
  public String domain() {
    return domain;
  }

  /**
   * @return the status filter, or {@code null}
   */
  @Nullable
  public ArticleStatus status() {
    return status;
  }

  /**
   * @return the direct owner filter (a user or group), or {@code null}
   */
  @Nullable
  public Owner owner() {
    return owner;
  }

  /**
   * @return the page size
   */
  public int limit() {
    return limit;
  }

  /**
   * @return the zero-based page offset
   */
  public int offset() {
    return offset;
  }

  /** Builder for {@link ArticleListFilter}. */
  public static class Builder {
    private String query;
    private String domain;
    private ArticleStatus status;
    private Owner owner;
    private int limit = DEFAULT_LIMIT;
    private int offset = 0;

    private Builder() {}

    /**
     * Sets the substring matched against the title, the synonyms and the body.
     *
     * @param query the search text
     * @return this builder
     */
    public Builder withQuery(String query) {
      this.query = query;
      return this;
    }

    /**
     * Sets the Domain name filter; a parent Domain also matches its caller-readable subdomains.
     *
     * @param domain the Domain name
     * @return this builder
     */
    public Builder withDomain(String domain) {
      this.domain = domain;
      return this;
    }

    /**
     * Sets the status filter.
     *
     * @param status the status
     * @return this builder
     */
    public Builder withStatus(ArticleStatus status) {
      this.status = status;
      return this;
    }

    /**
     * Sets the direct owner filter.
     *
     * @param owner the owner as a user or group identifier
     * @return this builder
     */
    public Builder withOwner(Owner owner) {
      this.owner = owner;
      return this;
    }

    /**
     * Sets the page size.
     *
     * @param limit the page size, from 1 through {@value ArticleListFilter#MAX_LIMIT}
     * @return this builder
     */
    public Builder withLimit(int limit) {
      this.limit = limit;
      return this;
    }

    /**
     * Sets the zero-based page offset.
     *
     * @param offset the page offset, at least 0
     * @return this builder
     */
    public Builder withOffset(int offset) {
      this.offset = offset;
      return this;
    }

    /**
     * Builds the filter.
     *
     * @return the filter
     */
    public ArticleListFilter build() {
      Preconditions.checkArgument(
          limit > 0 && limit <= MAX_LIMIT, "limit must be between 1 and %s", MAX_LIMIT);
      Preconditions.checkArgument(offset >= 0, "offset must not be negative");
      return new ArticleListFilter(this);
    }
  }
}
