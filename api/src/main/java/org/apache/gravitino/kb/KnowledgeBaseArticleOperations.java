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

import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.KnowledgeBaseArticleAlreadyExistsException;
import org.apache.gravitino.exceptions.KnowledgeBaseArticleVersionConflictException;
import org.apache.gravitino.exceptions.NoSuchKnowledgeBaseArticleException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;

/**
 * Metalake-scoped operations on Knowledge Base Articles and their asset bindings.
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
   * @param filter the list filter
   * @return one page of matching articles
   * @throws NoSuchMetalakeException if the metalake does not exist
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
   * Loads current content and a visible binding preview from the same revision.
   *
   * @param name the article name
   * @param limit maximum preview size
   * @param offset number of visible bindings to skip
   * @return the article with the requested preview
   */
  KnowledgeBaseArticle loadArticle(String name, int limit, int offset)
      throws NoSuchKnowledgeBaseArticleException;

  /**
   * Creates an article with current content, Domains and initial bindings.
   *
   * @param articleCreate the create request
   * @return the created article
   * @throws KnowledgeBaseArticleAlreadyExistsException if the explicit name is already used, or the
   *     title is already claimed in the same title-uniqueness scope
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
   * @throws NoSuchKnowledgeBaseArticleException if the article does not exist
   * @throws KnowledgeBaseArticleAlreadyExistsException if the title is already claimed in a
   *     requested Domain or the default scope
   * @throws KnowledgeBaseArticleVersionConflictException if a concurrent update to the same article
   *     was detected
   */
  KnowledgeBaseArticle alterArticle(String name, KnowledgeBaseArticleChange... changes)
      throws NoSuchKnowledgeBaseArticleException, KnowledgeBaseArticleAlreadyExistsException,
          KnowledgeBaseArticleVersionConflictException;

  /**
   * Deletes an article.
   *
   * @param name the article name
   * @return true if the article was deleted, false if it did not exist
   */
  boolean deleteArticle(String name) throws KnowledgeBaseArticleVersionConflictException;

  /**
   * Lists the metadata objects bound to the article.
   *
   * @param name the article name
   * @return the bound objects visible to the caller
   * @throws NoSuchKnowledgeBaseArticleException if the article does not exist
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
   */
  AssetBindingPage listAssetBindings(String name, int limit, int offset)
      throws NoSuchKnowledgeBaseArticleException;

  /**
   * Adds and removes asset bindings in one transaction. A request is atomic: if any target is
   * missing or invisible, no binding changes.
   *
   * @param name the article name
   * @param objectsToAdd the objects to bind
   * @param objectsToRemove the objects to unbind
   * @return the first page of visible bindings after the change
   * @throws NoSuchKnowledgeBaseArticleException if the article does not exist
   * @throws KnowledgeBaseArticleVersionConflictException on an optimistic-lock conflict
   */
  AssetBindingPage alterAssetBindings(
      String name,
      ArticleObjectIdentifier[] objectsToAdd,
      ArticleObjectIdentifier[] objectsToRemove)
      throws NoSuchKnowledgeBaseArticleException, KnowledgeBaseArticleVersionConflictException;

  /**
   * Reverse lookup: lists the articles bound to a metadata object.
   *
   * @param object the target object
   * @param includeDeprecated whether to include deprecated articles; draft articles are never
   *     returned
   * @return the matching published articles
   */
  default ArticlePage listArticlesForObject(
      ArticleObjectIdentifier object, boolean includeDeprecated) {
    return listArticlesForObject(object, includeDeprecated, ArticleListFilter.DEFAULT_LIMIT, 0);
  }

  /**
   * Lists a page of visible published articles bound to an asset.
   *
   * @param object target asset
   * @param includeDeprecated whether to include deprecated articles
   * @param limit maximum page size
   * @param offset number of visible matches to skip
   * @return a page of visible matches with its continuation flag
   */
  ArticlePage listArticlesForObject(
      ArticleObjectIdentifier object, boolean includeDeprecated, int limit, int offset);
}
