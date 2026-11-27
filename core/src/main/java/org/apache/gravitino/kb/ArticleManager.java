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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.KnowledgeBaseArticleAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchKnowledgeBaseArticleException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.relational.service.KnowledgeBaseArticleMetaService;
import org.apache.gravitino.storage.relational.service.KnowledgeBaseArticleMetaService.ExpectedRevision;
import org.apache.gravitino.storage.relational.service.KnowledgeBaseArticleMetaService.NameConflictException;
import org.apache.gravitino.storage.relational.service.KnowledgeBaseArticleMetaService.ScopedArticle;
import org.apache.gravitino.storage.relational.service.KnowledgeBaseArticleMetaService.TransactionHook;
import org.apache.gravitino.storage.relational.service.MetalakeMetaService;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/** Manages current Knowledge Base Articles in a metalake. */
public final class ArticleManager {
  private static final int MAX_NAME_RETRIES = 5;
  private static final int NAME_LOOKUP_BATCH = 32;
  private static final int TOKEN_BYTES = 3 * Long.BYTES;

  private final IdGenerator ids;
  private final ArticleEdits edits;
  private final TransactionHook policy;
  private final KnowledgeBaseArticleMetaService articles =
      KnowledgeBaseArticleMetaService.getInstance();

  /** Creates a manager that applies changes directly. */
  public ArticleManager(IdGenerator ids, int maxBodyBytes) {
    this(ids, maxBodyBytes, (before, after) -> {});
  }

  /** Creates a manager whose write policy participates in each effective write transaction. */
  public ArticleManager(IdGenerator ids, int maxBodyBytes, TransactionHook policy) {
    this.ids = Preconditions.checkNotNull(ids, "ids");
    this.edits = new ArticleEdits(maxBodyBytes);
    this.policy = Preconditions.checkNotNull(policy, "policy");
  }

  /** The Article metadata and its opaque editing token from the same transaction. */
  public record Result(KnowledgeBaseArticleEntity article, String revisionToken) {
    private static Result from(ScopedArticle state) {
      KnowledgeBaseArticleEntity article = state.article();
      return new Result(article, encode(state.metalakeId(), article.id(), article.revision()));
    }
  }

  /**
   * Creates an Article, allocating an available name when none is supplied.
   *
   * @throws OptimisticLockException if a concurrent writer claims the generated name in a
   *     caller-owned transaction. The caller can then retry the transaction.
   */
  public Result createArticle(String metalake, ArticleCreate request) {
    String name = request.name();
    boolean generated = name == null;
    boolean allocateInCallerTransaction = generated && SqlSessions.hasActiveSession();
    Long expectedMetalakeId = null;
    if (allocateInCallerTransaction) {
      // Fence the metalake observed during name selection against replacement before the insert.
      SessionUtils.clearSessionCache();
      try {
        expectedMetalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(metalake);
      } catch (NoSuchEntityException missing) {
        throw new NoSuchMetalakeException(missing, "Metalake %s does not exist", metalake);
      }
      name = nextAvailableName(metalake, request.title(), 0);
    } else if (generated) {
      name = ArticleEdits.generatedName(request.title(), 0);
    }
    KnowledgeBaseArticleEntity article = edits.create(metalake, ids.nextId(), name, request);
    for (int attempt = 0; ; attempt++) {
      try {
        return Result.from(
            articles.createScoped(article, expectedMetalakeId, policy, TargetNameConflict::new));
      } catch (TargetNameConflict conflict) {
        if (allocateInCallerTransaction) {
          throw new OptimisticLockException(
              conflict.getCause(),
              "Article name %s was claimed in %s; retry the transaction",
              article.name(),
              metalake);
        }
        if (!generated || attempt == MAX_NAME_RETRIES) {
          throw new KnowledgeBaseArticleAlreadyExistsException(
              conflict.getCause(),
              "Article name %s already exists in %s",
              article.name(),
              metalake);
        }
        expectedMetalakeId = conflict.metalakeId;
        article =
            article.toBuilder()
                .withName(
                    SqlSessions.readSnapshot(() -> nextAvailableName(metalake, request.title(), 1)))
                .withId(ids.nextId())
                .build();
      } catch (IOException failure) {
        throw new UncheckedIOException(failure);
      }
    }
  }

  /** Reads one Article and its token from a single database snapshot. */
  public Result loadArticle(String metalake, String name) {
    NameIdentifier ident = ident(metalake, name);
    try {
      return Result.from(articles.getScoped(ident));
    } catch (NoSuchEntityException missing) {
      throw new NoSuchKnowledgeBaseArticleException(missing, "Article %s does not exist", ident);
    }
  }

  /** Reads the stored body and its token from the same row. */
  public ArticleContent getArticleContent(String metalake, String name) {
    NameIdentifier ident = ident(metalake, name);
    try {
      var content = articles.getContent(ident);
      var revision = content.revision();
      return new ArticleContent(
          content.body(), encode(revision.metalakeId(), revision.articleId(), revision.revision()));
    } catch (NoSuchEntityException missing) {
      throw new NoSuchKnowledgeBaseArticleException(missing, "Article %s does not exist", ident);
    }
  }

  /** Lists the current stored Articles in stable ID order. */
  public List<KnowledgeBaseArticleEntity> listArticles(String metalake) {
    try {
      return articles.list(Namespace.of(metalake));
    } catch (NoSuchEntityException missing) {
      throw new NoSuchMetalakeException(missing, "Metalake %s does not exist", metalake);
    }
  }

  /** Applies the changes atomically, optionally checking the editing token before any change. */
  public Result alterArticle(
      String metalake,
      String name,
      @Nullable String expectedRevision,
      KnowledgeBaseArticleChange... changes) {
    NameIdentifier ident = ident(metalake, name);
    ExpectedRevision expected = expectedRevision == null ? null : decode(expectedRevision);
    Preconditions.checkNotNull(changes, "changes");
    return Result.from(
        articles.updateScoped(
            ident,
            expected,
            before -> edits.apply(before, changes),
            policy,
            missing ->
                new NoSuchKnowledgeBaseArticleException(
                    missing, "Article %s does not exist", ident)));
  }

  /** Deletes the Article and its relations, returning false when the Article is absent. */
  public boolean deleteArticle(String metalake, String name) {
    return articles.deleteAndGet(ident(metalake, name), policy).isPresent();
  }

  private static final class TargetNameConflict extends RuntimeException {
    private final long metalakeId;

    private TargetNameConflict(long metalakeId, NameConflictException cause) {
      super(cause);
      this.metalakeId = metalakeId;
    }
  }

  private static NameIdentifier ident(String metalake, String name) {
    return NameIdentifier.of(metalake, KnowledgeBaseArticle.normalizeName(name));
  }

  private String nextAvailableName(String metalake, String title, long start) {
    for (long offset = start; ; offset += NAME_LOOKUP_BATCH) {
      List<String> candidates = new ArrayList<>(NAME_LOOKUP_BATCH);
      for (int i = 0; i < NAME_LOOKUP_BATCH; i++) {
        candidates.add(ArticleEdits.generatedName(title, offset + i));
      }
      Set<String> occupied = articles.occupiedNames(metalake, candidates);
      for (String candidate : candidates) {
        if (!occupied.contains(candidate)) {
          return candidate;
        }
      }
    }
  }

  private static String encode(long metalakeId, long articleId, long revision) {
    byte[] token =
        ByteBuffer.allocate(TOKEN_BYTES)
            .putLong(metalakeId)
            .putLong(articleId)
            .putLong(revision)
            .array();
    return Base64.getUrlEncoder().withoutPadding().encodeToString(token);
  }

  private static ExpectedRevision decode(String token) {
    Preconditions.checkArgument(
        token.length() == TOKEN_BYTES * 4 / 3, "Invalid Article revision token");
    ByteBuffer bytes = ByteBuffer.wrap(Base64.getUrlDecoder().decode(token));
    Preconditions.checkArgument(bytes.remaining() == TOKEN_BYTES, "Invalid Article revision token");
    long lake = bytes.getLong();
    long article = bytes.getLong();
    long revision = bytes.getLong();
    Preconditions.checkArgument(revision > 0, "Invalid Article revision token");
    return new ExpectedRevision(lake, article, revision);
  }
}
