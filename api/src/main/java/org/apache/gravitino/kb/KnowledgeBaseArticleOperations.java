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

/** Metalake-scoped operations on Knowledge Base Articles and their asset bindings. */
@Evolving
public interface KnowledgeBaseArticleOperations {

  /**
   * Lists the article summaries matching the filter. The {@link ArticlePage#total()} counts only
   * articles the caller may read.
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
  KnowledgeBaseArticle loadArticle(String name) throws NoSuchKnowledgeBaseArticleException;

  /**
   * Creates an article together with its first version, Domains and asset bindings.
   *
   * @param articleCreate the create request
   * @return the created article
   * @throws KnowledgeBaseArticleAlreadyExistsException if the explicit name is already used
   * @throws KnowledgeBaseArticleVersionConflictException if the title is already used by another
   *     live article
   */
  KnowledgeBaseArticle createArticle(ArticleCreate articleCreate)
      throws KnowledgeBaseArticleAlreadyExistsException,
          KnowledgeBaseArticleVersionConflictException;

  /**
   * Applies a set of changes to an article atomically. Changes to the title, body or Domains create
   * exactly one new version.
   *
   * @param name the article name
   * @param changes the changes to apply
   * @return the altered article
   * @throws NoSuchKnowledgeBaseArticleException if the article does not exist
   * @throws KnowledgeBaseArticleVersionConflictException on a title conflict, a status transition
   *     that is not allowed, or an optimistic-lock conflict
   */
  KnowledgeBaseArticle alterArticle(String name, KnowledgeBaseArticleChange... changes)
      throws NoSuchKnowledgeBaseArticleException, KnowledgeBaseArticleVersionConflictException;

  /**
   * Deletes an article.
   *
   * @param name the article name
   * @return true if the article was deleted, false if it did not exist
   */
  boolean deleteArticle(String name);

  /**
   * Lists the metadata objects bound to the live version.
   *
   * @param name the article name
   * @return the bound objects visible to the caller
   * @throws NoSuchKnowledgeBaseArticleException if the article does not exist
   */
  ArticleObjectIdentifier[] listAssetBindings(String name)
      throws NoSuchKnowledgeBaseArticleException;

  /**
   * Adds and removes asset bindings in one transaction. A request is atomic: if any target is
   * missing or invisible, no binding changes.
   *
   * @param name the article name
   * @param objectsToAdd the objects to bind
   * @param objectsToRemove the objects to unbind
   * @return the resulting live bindings
   * @throws NoSuchKnowledgeBaseArticleException if the article does not exist
   * @throws KnowledgeBaseArticleVersionConflictException on an optimistic-lock conflict
   */
  ArticleObjectIdentifier[] alterAssetBindings(
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
  ArticleSummary[] listArticlesForObject(ArticleObjectIdentifier object, boolean includeDeprecated);
}
