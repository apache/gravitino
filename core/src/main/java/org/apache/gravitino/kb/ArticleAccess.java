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

import java.security.Principal;
import java.util.EnumMap;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.GravitinoAuthorizer.ArticleOwnerIds;
import org.apache.gravitino.authorization.Privilege.Name;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchKnowledgeBaseArticleException;
import org.apache.gravitino.storage.relational.service.KnowledgeBaseArticleMetaService;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.apache.gravitino.utils.PrincipalUtils;

/** Checks metalake definition privileges and ownership of the observed Article ID. */
public final class ArticleAccess {
  private final GravitinoAuthorizer authorizer;
  private final Principal principal;
  private final String metalake;
  private final AuthorizationRequestContext context;
  private final MetadataObject lake;
  private final boolean lakeOwner;
  private final EnumMap<Name, Boolean> privileges = new EnumMap<>(Name.class);

  private ArticleAccess(
      GravitinoAuthorizer authorizer,
      Principal principal,
      String metalake,
      AuthorizationRequestContext context) {
    this.authorizer = authorizer;
    this.principal = principal;
    this.metalake = metalake;
    this.context = context;
    lake = MetadataObjects.of(null, metalake, MetadataObject.Type.METALAKE);
    lakeOwner = authorizer.isOwner(principal, metalake, lake, context);
  }

  static ArticleAccess forRequest(String metalake) {
    return new ArticleAccess(
        GravitinoEnv.getInstance().gravitinoAuthorizer(),
        PrincipalUtils.getCurrentPrincipal(),
        metalake,
        new AuthorizationRequestContext());
  }

  /** Applies ordinary Article visibility to a generic metadata read. */
  public static boolean canRead(
      GravitinoAuthorizer authorizer,
      Principal principal,
      String metalake,
      MetadataObject object,
      AuthorizationRequestContext context) {
    if (object == null || object.type() != MetadataObject.Type.KNOWLEDGE_BASE_ARTICLE) {
      return false;
    }
    ArticleAccess access = new ArticleAccess(authorizer, principal, metalake, context);
    if (!access.canView()) {
      return false;
    }
    NameIdentifier ident = NameIdentifier.of(metalake, object.name());
    return SqlSessions.hasActiveSession()
        ? access.canRead(ident)
        : SqlSessions.readSnapshot(() -> access.canRead(ident));
  }

  boolean canView() {
    return allowed(Name.VIEW_DEFINITION);
  }

  boolean canManage() {
    return allowed(Name.MANAGE_DEFINITIONS);
  }

  ArticleOwnerIds draftOwnerIds() {
    return allowed(Name.CREATE_DEFINITION)
        ? authorizer.articleOwnerIds(principal, metalake, context)
        : ArticleOwnerIds.NONE;
  }

  void requireCreate() {
    if (!canView() || !allowed(Name.CREATE_DEFINITION)) {
      throw new ForbiddenException(
          "Creating an Article requires VIEW_DEFINITION and CREATE_DEFINITION");
    }
  }

  boolean canRead(long id, ArticleStatus status) {
    return canView() && (status != ArticleStatus.DRAFT || canEdit(id));
  }

  void requireRead(NameIdentifier ident, long id, ArticleStatus status) {
    if (!canRead(id, status)) {
      throw new NoSuchKnowledgeBaseArticleException("Article %s does not exist", ident);
    }
  }

  void requireEdit(KnowledgeBaseArticleEntity article) {
    if (!authorizeMutation(article)) {
      throw new NoSuchKnowledgeBaseArticleException(
          "Article %s does not exist", article.nameIdentifier());
    }
  }

  /** Returns false for hidden Articles and rejects writes to visible read-only Articles. */
  boolean authorizeMutation(KnowledgeBaseArticleEntity article) {
    if (!canView()) {
      return false;
    }
    if (canEdit(article.id())) {
      return true;
    }
    if (article.status() == ArticleStatus.DRAFT) {
      return false;
    }
    throw new ForbiddenException(
        "Editing or deleting an Article requires MANAGE_DEFINITIONS or ownership with CREATE_DEFINITION");
  }

  private boolean canRead(NameIdentifier ident) {
    KnowledgeBaseArticleEntity article;
    try {
      article = KnowledgeBaseArticleMetaService.getInstance().get(ident);
    } catch (NoSuchEntityException absent) {
      return false;
    }
    return canRead(article.id(), article.status());
  }

  private boolean canEdit(long id) {
    return canManage()
        || (allowed(Name.CREATE_DEFINITION)
            && authorizer.isArticleOwner(principal, metalake, id, context));
  }

  private boolean allowed(Name privilege) {
    return privileges.computeIfAbsent(
        privilege,
        required ->
            (lakeOwner || authorizer.authorize(principal, metalake, lake, required, context))
                && !authorizer.deny(principal, metalake, lake, required, context));
  }
}
