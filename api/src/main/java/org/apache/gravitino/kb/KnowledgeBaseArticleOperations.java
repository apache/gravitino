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

import javax.annotation.Nullable;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.ArticleRevisionConflictException;
import org.apache.gravitino.exceptions.KnowledgeBaseArticleAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchDomainException;
import org.apache.gravitino.exceptions.NoSuchKnowledgeBaseArticleException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;

/**
 * Metalake-scoped operations on Knowledge Base Articles and their Term and asset bindings.
 *
 * <p>Page visible results before returning them. Page sizes range from 1 through {@value
 * ArticleListFilter#MAX_LIMIT}; offsets are zero-based and nonnegative.
 */
@Evolving
public interface KnowledgeBaseArticleOperations {

  /**
   * Lists visible summaries matching the filter. Set {@link ArticlePage#hasMore()} when an
   * additional readable match follows the requested page.
   *
   * <p>A Term or asset filter resolves its target first and requires read access to it; a missing
   * or hidden target fails the request as not found instead of returning an empty page.
   *
   * @param filter the list filter
   * @return one page of matching articles
   * @throws NoSuchMetalakeException if the metalake does not exist
   * @throws org.apache.gravitino.exceptions.NoSuchTermException if a Term filter target does not
   *     exist or is invisible to the caller
   */
  ArticlePage listArticles(ArticleListFilter filter) throws NoSuchMetalakeException;

  /**
   * Loads an article by name.
   *
   * @param name the article name
   * @return the article
   * @throws NoSuchKnowledgeBaseArticleException if the article does not exist or is invisible to
   *     the caller
   */
  default KnowledgeBaseArticle loadArticle(String name) throws NoSuchKnowledgeBaseArticleException {
    return loadArticle(name, ArticleListFilter.DEFAULT_LIMIT, 0);
  }

  /**
   * Loads current metadata and a visible binding preview from the same revision.
   *
   * @param name the article name
   * @param limit maximum preview size
   * @param offset number of visible bindings to skip
   * @return the article with the requested preview
   * @throws NoSuchKnowledgeBaseArticleException if the article does not exist or is invisible to
   *     the caller
   */
  KnowledgeBaseArticle loadArticle(String name, int limit, int offset)
      throws NoSuchKnowledgeBaseArticleException;

  /**
   * Loads the stored body and editing token from the same revision.
   *
   * @param name the article name
   * @return the content visible to the caller
   * @throws NoSuchKnowledgeBaseArticleException if the article does not exist or is invisible
   */
  ArticleContent getArticleContent(String name) throws NoSuchKnowledgeBaseArticleException;

  /**
   * Creates an article with current content and initial bindings. Each added Domain requires
   * MANAGE_DOMAIN in addition to Article create authority. The creator becomes Owner when
   * authorization is enabled. Newly bound Terms must be approved, and each bound object must be
   * readable by the caller; otherwise the request fails without creating anything. A derived name
   * that keeps colliding after the implementation's bounded retries also fails the request.
   *
   * @param articleCreate the create request
   * @return the created article
   * @throws KnowledgeBaseArticleAlreadyExistsException if the explicit article name is already
   *     used, or every derived-name candidate conflicts during the bounded allocation retries
   * @throws org.apache.gravitino.exceptions.UnapprovedTermException if a readable Term to bind is
   *     not approved
   * @throws org.apache.gravitino.exceptions.NoSuchTermException if a Term to bind does not exist or
   *     is invisible to the caller
   */
  KnowledgeBaseArticle createArticle(ArticleCreate articleCreate)
      throws KnowledgeBaseArticleAlreadyExistsException;

  /**
   * Applies a set of changes to an article atomically. A failure leaves all fields and bindings
   * unchanged.
   *
   * @param name the article name
   * @param changes the changes to apply
   * @return the altered article
   * @throws NoSuchKnowledgeBaseArticleException if the article does not exist or is invisible to
   *     the caller
   */
  default KnowledgeBaseArticle alterArticle(String name, KnowledgeBaseArticleChange... changes) {
    return alterArticle(name, changes, null);
  }

  /**
   * Applies all changes atomically. Domain replacement requires Article edit authority and
   * MANAGE_DOMAIN on each added or removed Domain. After authorization, compare a supplied token
   * against the locked Article before detecting a no-op. A matching no-op retains the token.
   *
   * @param name the article name
   * @param changes the changes, including any binding deltas
   * @param expectedRevision opaque token from Article metadata or content, or null for an ordinary
   *     edit
   * @return the updated article with its editing token
   * @throws org.apache.gravitino.exceptions.NoSuchTermException if a named Term is missing or
   *     invisible
   * @throws org.apache.gravitino.exceptions.UnapprovedTermException if a newly bound Term is not
   *     approved
   * @throws ArticleRevisionConflictException if the token refers to another identity or a stale
   *     revision
   * @throws IllegalArgumentException if the token is malformed
   * @throws NoSuchKnowledgeBaseArticleException if the article does not exist or is invisible to
   *     the caller
   */
  KnowledgeBaseArticle alterArticle(
      String name, KnowledgeBaseArticleChange[] changes, @Nullable String expectedRevision)
      throws ArticleRevisionConflictException, NoSuchKnowledgeBaseArticleException;

  /**
   * Deletes an article.
   *
   * @param name the article name
   * @return true if the article was deleted, false if absent, previously deleted or hidden
   */
  boolean deleteArticle(String name);

  /**
   * Detaches one Article from a Domain. With authorization enabled, this requires MANAGE_DOMAIN on
   * the Domain, independent of Article edit permission. An absent Article or membership is a no-op.
   * An effective detach advances the Article revision once, which can invalidate a client's
   * conditional-edit token read before the detach.
   *
   * @param name the Article name
   * @param domain the Domain name
   * @throws NoSuchDomainException if the Domain does not exist or is invisible to the caller
   */
  void detachDomain(String name, String domain) throws NoSuchDomainException;

  /**
   * Lists the metadata objects bound to the article.
   *
   * @param name the article name
   * @return the bound objects visible to the caller
   * @throws NoSuchKnowledgeBaseArticleException if the article does not exist or is invisible to
   *     the caller
   */
  default AssetBindingPage listAssetBindings(String name)
      throws NoSuchKnowledgeBaseArticleException {
    return listAssetBindings(name, ArticleListFilter.DEFAULT_LIMIT, 0);
  }

  /**
   * Lists a window of visible bindings.
   *
   * @param name the article name
   * @param limit maximum page size
   * @param offset number of visible bindings to skip
   * @return the requested binding page
   * @throws NoSuchKnowledgeBaseArticleException if the article does not exist or is invisible to
   *     the caller
   */
  AssetBindingPage listAssetBindings(String name, int limit, int offset)
      throws NoSuchKnowledgeBaseArticleException;

  /**
   * @param name the article name
   * @return the first page of readable bound Terms
   * @throws NoSuchKnowledgeBaseArticleException if the article does not exist or is invisible to
   *     the caller
   */
  default TermBindingPage listTermBindings(String name) throws NoSuchKnowledgeBaseArticleException {
    return listTermBindings(name, ArticleListFilter.DEFAULT_LIMIT, 0);
  }

  /**
   * Lists readable Terms bound to the Article.
   *
   * @param name the article name
   * @param limit maximum page size
   * @param offset readable bindings to skip
   * @return the requested page
   * @throws NoSuchKnowledgeBaseArticleException if the article does not exist or is invisible to
   *     the caller
   */
  TermBindingPage listTermBindings(String name, int limit, int offset)
      throws NoSuchKnowledgeBaseArticleException;

  /**
   * @param term direct binding target
   * @return the first page of published articles bound to the target
   */
  default ArticlePage listArticlesForTerm(String term) {
    return listArticlesForTerm(term, false);
  }

  /**
   * @param term direct binding target
   * @param includeDeprecated whether to include deprecated articles
   * @return the first page of matching articles
   */
  default ArticlePage listArticlesForTerm(String term, boolean includeDeprecated) {
    return listArticlesForTerm(term, includeDeprecated, ArticleListFilter.DEFAULT_LIMIT, 0);
  }

  /**
   * @param term direct binding target
   * @param includeDeprecated whether to include deprecated articles
   * @param limit maximum page size
   * @param offset matching articles to skip
   * @return the requested page of visible matches
   */
  default ArticlePage listArticlesForTerm(
      String term, boolean includeDeprecated, int limit, int offset) {
    return listArticles(
        ArticleListFilter.builder()
            .withTerm(term)
            .withIncludeDeprecated(includeDeprecated)
            .withLimit(limit)
            .withOffset(offset)
            .build());
  }

  /**
   * @param object direct binding target
   * @return the first page of published articles bound to the target
   */
  default ArticlePage listArticlesForObject(ArticleObjectIdentifier object) {
    return listArticlesForObject(object, false);
  }

  /**
   * @param object direct binding target
   * @param includeDeprecated whether to include deprecated articles
   * @return the first page of matching articles
   */
  default ArticlePage listArticlesForObject(
      ArticleObjectIdentifier object, boolean includeDeprecated) {
    return listArticlesForObject(object, includeDeprecated, ArticleListFilter.DEFAULT_LIMIT, 0);
  }

  /**
   * @param object direct binding target
   * @param includeDeprecated whether to include deprecated articles
   * @param limit maximum page size
   * @param offset matching articles to skip
   * @return the requested page of visible matches
   */
  default ArticlePage listArticlesForObject(
      ArticleObjectIdentifier object, boolean includeDeprecated, int limit, int offset) {
    return listArticles(
        ArticleListFilter.builder()
            .withObject(object)
            .withIncludeDeprecated(includeDeprecated)
            .withLimit(limit)
            .withOffset(offset)
            .build());
  }
}
