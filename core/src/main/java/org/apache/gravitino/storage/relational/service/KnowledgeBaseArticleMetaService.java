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
package org.apache.gravitino.storage.relational.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.base.Utf8;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.ArticleRevisionConflictException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.kb.ArticleBody;
import org.apache.gravitino.kb.ArticleStatus;
import org.apache.gravitino.kb.KnowledgeBaseArticle;
import org.apache.gravitino.kb.KnowledgeBaseArticleEntity;
import org.apache.gravitino.kb.KnowledgeBaseArticleEntity.Binding;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.storage.relational.converters.PostgreSQLExceptionConverter;
import org.apache.gravitino.storage.relational.converters.SQLExceptionConverterFactory;
import org.apache.gravitino.storage.relational.mapper.DomainMemberMapper;
import org.apache.gravitino.storage.relational.mapper.DomainMemberMapper.MemberType;
import org.apache.gravitino.storage.relational.mapper.DomainMemberMapper.Membership;
import org.apache.gravitino.storage.relational.mapper.DomainMetaMapper;
import org.apache.gravitino.storage.relational.mapper.KnowledgeBaseArticleMapper;
import org.apache.gravitino.storage.relational.po.DomainPO;
import org.apache.gravitino.storage.relational.po.KnowledgeBaseArticlePO;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.PrincipalUtils;

/** Persists current Article content and stable binding identities in one transaction. */
public final class KnowledgeBaseArticleMetaService {
  private static final KnowledgeBaseArticleMetaService INSTANCE =
      new KnowledgeBaseArticleMetaService();
  private static final int LOOKUP_BATCH_SIZE = 500;
  private static final MemberType ARTICLE = MemberType.KNOWLEDGE_BASE_ARTICLE;
  private static final TransactionHook NO_HOOK = (before, after) -> {};
  private static final Pattern H2_NAME_CONSTRAINT =
      Pattern.compile(
          "(?i)^Unique index or primary key violation: \"(?:[\\w]+\\.)?uk_kba_name_index_[0-9a-f]+ on ");
  private static final Pattern H2_ID_CONSTRAINT =
      Pattern.compile(
          "(?i)^Unique index or primary key violation: \"PRIMARY KEY ON (?:[\\w]+\\.)?kb_article_meta\\(article_id\\)");

  /** A conflict on the live Article name key during the Article row insert. */
  public static final class NameConflictException extends EntityAlreadyExistsException {
    private NameConflictException(Throwable cause, String name) {
      super(cause, "Article name %s already exists", name);
    }
  }

  /** Current Article and its metalake identity observed in the same transaction. */
  public record ScopedArticle(long metalakeId, KnowledgeBaseArticleEntity article) {}

  /** The addressed Article or its parent is missing, independently of any extension failure. */
  public static final class MissingArticleScopeException extends NoSuchEntityException {
    private MissingArticleScopeException(NameIdentifier ident) {
      super("Knowledge Base Article %s does not exist", ident);
    }

    private MissingArticleScopeException(NoSuchEntityException parent) {
      super(parent, "%s", parent.getMessage());
    }
  }

  /** Storage extension invoked before commit, with the complete old and new binding sets. */
  @FunctionalInterface
  public interface TransactionHook {
    /**
     * Runs under the Article lock before an effective update or deletion changes the row or its
     * relations. To read the old body and revision, call {@link
     * KnowledgeBaseArticleMetaService#getContent(NameIdentifier)} in this callback.
     *
     * @param before current Article metadata and relations
     * @param after proposed Article metadata and relations, or null for deletion
     */
    default void beforeChange(
        KnowledgeBaseArticleEntity before, @Nullable KnowledgeBaseArticleEntity after) {}

    /**
     * Runs after the changes have been applied and before commit. Throwing rolls back the write.
     */
    void onChange(
        @Nullable KnowledgeBaseArticleEntity before, @Nullable KnowledgeBaseArticleEntity after);
  }

  /** Stable identity and revision decoded from an editing token by the caller. */
  public record ExpectedRevision(long metalakeId, long articleId, long revision) {
    private boolean matches(long lake, long article, long version) {
      return metalakeId == lake && articleId == article && revision == version;
    }
  }

  /** Body and stable revision identity read from the same live row. */
  public record StoredContent(ArticleBody body, ExpectedRevision revision) {}

  private KnowledgeBaseArticleMetaService() {}

  /** Returns the shared Article service. */
  public static KnowledgeBaseArticleMetaService getInstance() {
    return INSTANCE;
  }

  /** Returns Article metadata and binding identities from one snapshot. */
  public KnowledgeBaseArticleEntity get(NameIdentifier ident) {
    return getScoped(ident).article();
  }

  /** Lists stored metadata and binding identities. */
  public List<KnowledgeBaseArticleEntity> list(Namespace namespace) {
    return readAll(namespace);
  }

  /** Reads the Article and the identity used for its editing token from one snapshot. */
  public ScopedArticle getScoped(NameIdentifier ident) {
    String name = KnowledgeBaseArticle.normalizeName(ident.name());
    if (SqlSessions.hasActiveSession()) {
      ScopedArticle result = joinedArticle(ident.namespace(), name);
      if (result == null) {
        throw missing(ident);
      }
      return result;
    }
    return SqlSessions.readSnapshot(
        () -> {
          long lake =
              MetalakeMetaService.getInstance()
                  .getMetalakeIdByName(metalakeName(ident.namespace()));
          return SessionUtils.getWithoutCommit(
              KnowledgeBaseArticleMapper.class,
              mapper -> {
                KnowledgeBaseArticlePO row = mapper.get(lake, name);
                if (row == null) {
                  throw missing(ident);
                }
                return new ScopedArticle(lake, materialize(ident.namespace(), row, mapper));
              });
        });
  }

  /** Returns the live names among the proposed allocation candidates. */
  public Set<String> occupiedNames(String metalake, List<String> candidates) {
    if (candidates.isEmpty()) {
      return Set.of();
    }
    return SessionUtils.getWithoutCommit(
        KnowledgeBaseArticleMapper.class, mapper -> mapper.occupiedNames(metalake, candidates));
  }

  /** Reads only the current content and its editing identity. */
  public StoredContent getContent(NameIdentifier ident) {
    String metalake = metalakeName(ident.namespace());
    String name = KnowledgeBaseArticle.normalizeName(ident.name());
    KnowledgeBaseArticlePO row =
        SessionUtils.getWithoutCommitFresh(
            KnowledgeBaseArticleMapper.class, mapper -> mapper.content(metalake, name));
    if (row == null) {
      throw missing(ident);
    }
    return new StoredContent(
        ArticleBody.of(ArticleBody.Format.valueOf(row.getBodyFormat()), row.getBodyContent()),
        new ExpectedRevision(row.getMetalakeId(), row.getArticleId(), row.getRevision()));
  }

  /** Resolves the stable ID without fetching the Article body. */
  public NamespacedEntityId getIds(NameIdentifier ident) {
    String metalake = metalakeName(ident.namespace());
    String name = KnowledgeBaseArticle.normalizeName(ident.name());
    KnowledgeBaseArticlePO ids =
        SessionUtils.getWithoutCommitFresh(
            KnowledgeBaseArticleMapper.class, m -> m.ids(metalake, name));
    if (ids == null) {
      throw missing(ident);
    }
    return new NamespacedEntityId(ids.getArticleId(), ids.getMetalakeId());
  }

  /**
   * Returns the requested live entities in input order, omitting missing entries.
   *
   * <p>Without an active session, all batches for one metalake share a read snapshot. With an
   * active session, visibility across batches follows the caller's transaction isolation level.
   */
  public List<KnowledgeBaseArticleEntity> batchGet(List<NameIdentifier> idents) {
    Map<Namespace, Set<String>> groups = new LinkedHashMap<>();
    for (NameIdentifier ident : idents) {
      groups
          .computeIfAbsent(ident.namespace(), unused -> new LinkedHashSet<>())
          .add(KnowledgeBaseArticle.normalizeName(ident.name()));
    }
    Map<NameIdentifier, KnowledgeBaseArticleEntity> found = new HashMap<>();
    groups.forEach(
        (ns, names) -> readBatch(ns, names).forEach(e -> found.put(e.nameIdentifier(), e)));
    List<KnowledgeBaseArticleEntity> result = new ArrayList<>();
    for (NameIdentifier ident : idents) {
      KnowledgeBaseArticleEntity entity =
          found.get(
              NameIdentifier.of(
                  ident.namespace(), KnowledgeBaseArticle.normalizeName(ident.name())));
      if (entity != null) {
        result.add(entity);
      }
    }
    return result;
  }

  /** Inserts a new Article, or overwrites the same live identity. */
  public void insert(KnowledgeBaseArticleEntity entity, boolean overwrite) throws IOException {
    insert(entity, overwrite, NO_HOOK);
  }

  /**
   * Commits the initial Owner or extension data along with the Article and initial bindings. If a
   * peer inserts the same identity during a standalone overwrite, retries in a fresh transaction.
   * Inside a caller-owned transaction the insert conflict is propagated; the owner can retry the
   * overwrite after ending its transaction.
   */
  public void insert(KnowledgeBaseArticleEntity entity, boolean overwrite, TransactionHook hook)
      throws IOException {
    insertScoped(entity, overwrite, hook);
  }

  /** Returns the stored state with the metalake ID fenced by the write. */
  public ScopedArticle insertScoped(
      KnowledgeBaseArticleEntity entity, boolean overwrite, TransactionHook hook)
      throws IOException {
    return insertScoped(
        entity,
        overwrite,
        hook,
        () -> lockMetalake(entity.namespace()),
        (id, conflict) -> conflict);
  }

  /**
   * Creates an Article under the original metalake. Only a conflict from this Article's row insert
   * is passed to the caller for name allocation.
   *
   * @param entity the Article to create
   * @param expectedMetalakeId the locked parent from a previous attempt, or null on the first
   * @param hook the extension participating in the transaction
   * @param onNameConflict maps a target name conflict and its locked metalake ID
   * @return the created Article and its metalake ID
   * @throws IOException if the storage write fails
   */
  public ScopedArticle createScoped(
      KnowledgeBaseArticleEntity entity,
      @Nullable Long expectedMetalakeId,
      TransactionHook hook,
      BiFunction<Long, NameConflictException, RuntimeException> onNameConflict)
      throws IOException {
    return insertScoped(
        entity,
        false,
        hook,
        () -> {
          String name = metalakeName(entity.namespace());
          long id;
          try {
            id = lockMetalake(entity.namespace());
          } catch (NoSuchEntityException missing) {
            throw new NoSuchMetalakeException(missing, "Metalake %s does not exist", name);
          }
          if (expectedMetalakeId != null && expectedMetalakeId != id) {
            throw new NoSuchMetalakeException("Metalake %s was replaced", name);
          }
          return id;
        },
        onNameConflict);
  }

  private ScopedArticle insertScoped(
      KnowledgeBaseArticleEntity entity,
      boolean overwrite,
      TransactionHook hook,
      LongSupplier lockParent,
      BiFunction<Long, NameConflictException, RuntimeException> onNameConflict)
      throws IOException {
    record InsertConflict(long metalakeId, EntityAlreadyExistsException failure) {}
    AtomicReference<InsertConflict> insertConflict = new AtomicReference<>();
    boolean retryInsert = overwrite && !SqlSessions.hasActiveSession();
    try {
      return write(
          mapper -> {
            long metalakeId = lockParent.getAsLong();
            KnowledgeBaseArticleEntity before = overwrite ? observe(entity.nameIdentifier()) : null;
            InsertConflict conflict = insertConflict.get();
            if (conflict != null
                && (conflict.metalakeId() != metalakeId
                    || before == null
                    || !before.id().equals(entity.id()))) {
              throw conflict.failure();
            }
            if (before != null) {
              KnowledgeBaseArticlePO row =
                  lockForChange(mapper, metalakeId, before, entity.domainIds(), null);
              return new ScopedArticle(metalakeId, replace(mapper, row, before, entity, hook));
            }
            entity.validateTextForWrite(null);
            requireDomainsLive(lockLiveDomains(metalakeId, entity.domainIds()));
            KnowledgeBaseArticleEntity initial =
                entity.toBuilder().withBodyUpdate(null).withRevision(1).build();
            KnowledgeBaseArticlePO row = encode(initial, metalakeId);
            ArticleBody body =
                Preconditions.checkNotNull(
                    entity.bodyUpdate(), "An initial Article body is required");
            row.setBodyFormat(body.format().name());
            row.setBodyContent(body.content());
            try {
              insertArticleRow(mapper, row);
            } catch (EntityAlreadyExistsException e) {
              if (!retryInsert) {
                if (e instanceof NameConflictException nameConflict) {
                  throw onNameConflict.apply(metalakeId, nameConflict);
                }
                throw e;
              }
              // Leave the failed insert transaction before observing the row that won.
              insertConflict.set(new InsertConflict(metalakeId, e));
              throw new StaleArticlePlan(entity.nameIdentifier());
            }
            addBindings(mapper, row, initial.bindings());
            changeMemberships(row, Set.of(), initial.domainIds());
            hook.onChange(null, initial);
            return new ScopedArticle(metalakeId, initial);
          });
    } catch (RuntimeException e) {
      // A duplicate from a binding or extension belongs to that write.
      if (!ExceptionUtils.isDuplicateKey(e)) {
        ExceptionUtils.checkSQLException(e, entity.type(), entity.nameIdentifier().toString());
      }
      throw e;
    }
  }

  /** Applies an EntityStore transformation and revalidates its input under the Article lock. */
  public <E extends Entity & HasIdentifier> KnowledgeBaseArticleEntity update(
      NameIdentifier ident, Function<E, E> updater) {
    return update(
        ident, null, current -> (KnowledgeBaseArticleEntity) updater.apply((E) current), NO_HOOK);
  }

  /**
   * Plans the affected Domain locks from a consistent Article snapshot and commits one revision per
   * effective edit. The updater is a transformation and may be re-evaluated if a peer edits the
   * Article during planning. Put transactional side effects in the hook, which runs after the
   * identities and revision have been checked under lock.
   */
  public KnowledgeBaseArticleEntity update(
      NameIdentifier ident,
      @Nullable ExpectedRevision expected,
      Function<KnowledgeBaseArticleEntity, KnowledgeBaseArticleEntity> updater,
      TransactionHook hook) {
    return updateScoped(ident, expected, updater, hook).article();
  }

  /** Returns the effective state and its token identity while the write locks are held. */
  public ScopedArticle updateScoped(
      NameIdentifier ident,
      @Nullable ExpectedRevision expected,
      Function<KnowledgeBaseArticleEntity, KnowledgeBaseArticleEntity> updater,
      TransactionHook hook) {
    return updateScoped(ident, expected, updater, hook, missing -> missing);
  }

  /**
   * Updates an Article, mapping absence only while locating the target of this write.
   *
   * @param ident the Article name identifier
   * @param expected the revision to match, or null for an unconditional edit
   * @param updater the transformation to apply to the current Article
   * @param hook the extension participating in the transaction
   * @param onMissingTarget maps a missing Article or metalake to the caller's exception
   * @return the effective Article and its metalake ID
   */
  public ScopedArticle updateScoped(
      NameIdentifier ident,
      @Nullable ExpectedRevision expected,
      Function<KnowledgeBaseArticleEntity, KnowledgeBaseArticleEntity> updater,
      TransactionHook hook,
      Function<NoSuchEntityException, RuntimeException> onMissingTarget) {
    record ArticleIdentity(long metalakeId, long articleId) {}
    AtomicReference<ArticleIdentity> firstObserved = new AtomicReference<>();
    return write(
        mapper -> {
          long metalakeId;
          try {
            metalakeId = lockMetalake(ident.namespace());
          } catch (NoSuchEntityException missing) {
            throw onMissingTarget.apply(missing);
          }
          KnowledgeBaseArticleEntity before = observe(ident);
          if (before == null) {
            throw onMissingTarget.apply(missing(ident));
          }
          ArticleIdentity current = new ArticleIdentity(metalakeId, before.id());
          ArticleIdentity first = firstObserved.compareAndExchange(null, current);
          if (first != null && !first.equals(current)) {
            throw new ArticleRevisionConflictException("Article %s was replaced", ident);
          }
          if (expected != null && !expected.matches(metalakeId, before.id(), before.revision())) {
            // Reject a stale token under lock before evaluating the transformation.
            lockForChange(mapper, metalakeId, before, before.domainIds(), expected);
          }
          KnowledgeBaseArticleEntity proposed = updater.apply(before);
          KnowledgeBaseArticlePO row =
              lockForChange(mapper, metalakeId, before, proposed.domainIds(), expected);
          return new ScopedArticle(metalakeId, replace(mapper, row, before, proposed, hook));
        });
  }

  /** Retires a live Article and its bindings atomically. */
  public boolean delete(NameIdentifier ident) {
    return delete(ident, NO_HOOK);
  }

  /** Retires an Article and invokes its storage extension with the former state. */
  public boolean delete(NameIdentifier ident, TransactionHook hook) {
    return deleteSnapshot(ident, hook, false).isPresent();
  }

  /**
   * Retires an Article and returns the metadata snapshot selected under its row lock.
   *
   * @param ident the Article name identifier
   * @param hook the extension participating in the deletion transaction
   * @return the deleted snapshot, or empty if the Article or its metalake is absent or the selected
   *     Article was replaced
   */
  public Optional<KnowledgeBaseArticleEntity> deleteAndGet(
      NameIdentifier ident, TransactionHook hook) {
    return deleteSnapshot(ident, hook, true);
  }

  /** Retires all Article rows in the enclosing metalake deletion transaction. */
  public void dropMetalake(long metalakeId) {
    long now = System.currentTimeMillis();
    SessionUtils.doWithoutCommit(
        KnowledgeBaseArticleMapper.class,
        m -> {
          m.dropMetalake(metalakeId, now);
          m.dropMetalakeBindings(metalakeId, now);
        });
  }

  /** Removes at most limit expired rows, including bindings detached from live Articles. */
  public int collect(long cutoff, int limit) {
    return SessionUtils.doWithCommitAndFetchResult(
        KnowledgeBaseArticleMapper.class,
        m -> {
          int bindings = m.purgeBindings(cutoff, limit);
          return bindings == limit ? bindings : bindings + m.purge(cutoff, limit - bindings);
        });
  }

  private Optional<KnowledgeBaseArticleEntity> deleteSnapshot(
      NameIdentifier ident, TransactionHook hook, boolean allowMissingMetalake) {
    // A revision change can retry the plan. Keep the first identity across those attempts.
    AtomicReference<Long> targetId = new AtomicReference<>();
    return write(
        mapper -> {
          long metalakeId;
          try {
            metalakeId = lockMetalake(ident.namespace());
          } catch (NoSuchEntityException e) {
            if (allowMissingMetalake) {
              return Optional.empty();
            }
            throw e;
          }
          KnowledgeBaseArticleEntity old = observe(ident);
          if (old == null) {
            return Optional.empty();
          }
          Long firstId = targetId.compareAndExchange(null, old.id());
          if (firstId != null && !firstId.equals(old.id())) {
            return Optional.empty();
          }
          KnowledgeBaseArticlePO row = lockObservedArticle(mapper, metalakeId, old, Set.of(), null);
          if (row == null) {
            return Optional.empty();
          }
          hook.beforeChange(old, null);
          long now = System.currentTimeMillis();
          if (mapper.delete(row.getArticleId(), old.revision(), now) != 1) {
            throw new ArticleRevisionConflictException("Article %s has changed", ident);
          }
          mapper.deleteBindings(row.getArticleId(), now);
          changeMemberships(row, old.domainIds(), Set.of());
          hook.onChange(old, null);
          return Optional.of(old);
        });
  }

  private static void insertArticleRow(
      KnowledgeBaseArticleMapper mapper, KnowledgeBaseArticlePO row) {
    try {
      mapper.insert(row);
    } catch (RuntimeException failure) {
      for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
        if (cause instanceof SQLException sql && ExceptionUtils.isDuplicateKey(sql)) {
          if (matchesConstraint(sql, "uk_kba_name", "uk_kba_name", H2_NAME_CONSTRAINT)) {
            throw new NameConflictException(failure, row.getArticleName());
          }
          if (matchesConstraint(sql, "kb_article_meta_pkey", "PRIMARY", H2_ID_CONSTRAINT)) {
            throw new EntityAlreadyExistsException(
                failure, "Article ID %s already exists", row.getArticleId());
          }
        }
      }
      throw failure;
    }
  }

  private static boolean matchesConstraint(
      SQLException sql, String pgKey, String mysqlKey, Pattern h2Constraint) {
    if (SQLExceptionConverterFactory.getConverter()
        instanceof PostgreSQLExceptionConverter postgres) {
      return postgres.violatesUniqueConstraint(sql, pgKey);
    }
    String message = sql.getMessage();
    if (message == null) {
      return false;
    }
    if (sql.getErrorCode() == 1062) {
      // The final quoted index name is independent of the session's message language.
      return message.endsWith("'" + mysqlKey + "'")
          || message.endsWith("'kb_article_meta." + mysqlKey + "'");
    }
    return h2Constraint.matcher(message).find();
  }

  private static String metalakeName(Namespace ns) {
    Preconditions.checkArgument(
        ns != null && ns.length() == 1, "Expected a metalake namespace: %s", ns);
    return ns.level(0);
  }

  private static NoSuchEntityException missing(NameIdentifier ident) {
    return new MissingArticleScopeException(ident);
  }

  private static final class StaleArticlePlan extends OptimisticLockException {
    private StaleArticlePlan(NameIdentifier ident) {
      super("Article %s changed during Domain lock planning", ident);
    }
  }

  private <T> T write(Function<KnowledgeBaseArticleMapper, T> operation) {
    if (SqlSessions.hasActiveSession()) {
      // Only the owner can release its locks and retry in a fresh transaction.
      return SessionUtils.doWithCommitAndFetchResult(
          KnowledgeBaseArticleMapper.class,
          mapper -> SqlSessions.withSavepoint(() -> operation.apply(mapper)));
    }
    for (int attempt = 1; ; attempt++) {
      try {
        return SessionUtils.doWithCommitAndFetchResult(KnowledgeBaseArticleMapper.class, operation);
      } catch (StaleArticlePlan stale) {
        if (attempt >= 5) {
          throw stale;
        }
      }
    }
  }

  private long lockMetalake(Namespace ns) {
    String name = metalakeName(ns);
    SessionUtils.clearSessionCache();
    MetalakeMetaService parents = MetalakeMetaService.getInstance();
    long id;
    try {
      id = parents.requireMetalakeByName(name).getMetalakeId();
      parents.lockMetalakeForChildWrite(name, id);
    } catch (NoSuchEntityException missing) {
      throw new MissingArticleScopeException(missing);
    }
    return id;
  }

  private List<KnowledgeBaseArticleEntity> readAll(Namespace ns) {
    String metalake = metalakeName(ns);
    if (SqlSessions.hasActiveSession()) {
      // A caller's READ_COMMITTED transaction may contain uncommitted writes. A single
      // statement observes them without splitting the row and its bindings across snapshots.
      return SessionUtils.getWithoutCommitFresh(
          KnowledgeBaseArticleMapper.class,
          m -> {
            List<KnowledgeBaseArticlePO> rows = m.joined(metalake, null);
            if (rows.isEmpty()) {
              throw new NoSuchEntityException(
                  NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, "metalake", metalake);
            }
            return aggregate(ns, rows);
          });
    }
    return SqlSessions.readSnapshot(
        () ->
            SessionUtils.getWithoutCommit(
                KnowledgeBaseArticleMapper.class,
                m -> {
                  long metalakeId =
                      MetalakeMetaService.getInstance()
                          .requireMetalakeByName(metalake)
                          .getMetalakeId();
                  Map<Long, Set<Binding>> bindings = new HashMap<>();
                  for (KnowledgeBaseArticlePO target : m.listBindings(metalakeId)) {
                    bindings
                        .computeIfAbsent(target.getArticleId(), unused -> new HashSet<>())
                        .add(new Binding(target.getTargetType(), target.getTargetId()));
                  }
                  Map<Long, Set<Long>> domains = new HashMap<>();
                  for (Membership member :
                      SessionUtils.getWithoutCommit(
                          DomainMemberMapper.class, members -> members.list(metalakeId, ARTICLE))) {
                    domains
                        .computeIfAbsent(member.memberId(), unused -> new HashSet<>())
                        .add(member.domainId());
                  }
                  return m.list(metalakeId).stream()
                      .map(
                          row ->
                              decode(
                                  ns,
                                  row,
                                  bindings.getOrDefault(row.getArticleId(), Set.of()),
                                  domains.getOrDefault(row.getArticleId(), Set.of())))
                      .toList();
                }));
  }

  private List<KnowledgeBaseArticleEntity> readBatch(Namespace ns, Set<String> names) {
    String metalake = metalakeName(ns);
    Supplier<List<KnowledgeBaseArticleEntity>> read =
        () ->
            SessionUtils.getWithoutCommitFresh(
                KnowledgeBaseArticleMapper.class,
                mapper -> {
                  List<KnowledgeBaseArticleEntity> result = new ArrayList<>();
                  for (List<String> batch :
                      Lists.partition(List.copyOf(names), LOOKUP_BATCH_SIZE)) {
                    result.addAll(aggregate(ns, mapper.joinedBatch(metalake, batch)));
                  }
                  return result;
                });
    return SqlSessions.hasActiveSession() ? read.get() : SqlSessions.readSnapshot(read);
  }

  /** Folds one joined row per Article, binding and membership into complete entities. */
  private List<KnowledgeBaseArticleEntity> aggregate(
      Namespace ns, List<KnowledgeBaseArticlePO> rows) {
    Map<Long, Set<Binding>> bindings = new HashMap<>();
    Map<Long, Set<Long>> domains = new HashMap<>();
    Map<Long, KnowledgeBaseArticlePO> articles = new LinkedHashMap<>();
    for (KnowledgeBaseArticlePO row : rows) {
      if (row.getArticleName() != null) {
        articles.put(row.getArticleId(), row);
      } else if (row.getDomainId() != null) {
        domains
            .computeIfAbsent(row.getArticleId(), unused -> new HashSet<>())
            .add(row.getDomainId());
      } else if (row.getTargetId() != null) {
        bindings
            .computeIfAbsent(row.getArticleId(), unused -> new HashSet<>())
            .add(new Binding(row.getTargetType(), row.getTargetId()));
      }
    }
    return articles.values().stream()
        .map(
            row ->
                decode(
                    ns,
                    row,
                    bindings.getOrDefault(row.getArticleId(), Set.of()),
                    domains.getOrDefault(row.getArticleId(), Set.of())))
        .toList();
  }

  @Nullable
  private KnowledgeBaseArticleEntity observe(NameIdentifier ident) {
    ScopedArticle current =
        joinedArticle(ident.namespace(), KnowledgeBaseArticle.normalizeName(ident.name()));
    return current == null ? null : current.article();
  }

  @Nullable
  private ScopedArticle joinedArticle(Namespace namespace, String name) {
    List<KnowledgeBaseArticlePO> rows =
        SessionUtils.getWithoutCommitFresh(
            KnowledgeBaseArticleMapper.class,
            mapper -> mapper.joined(metalakeName(namespace), name));
    return rows.isEmpty()
        ? null
        : new ScopedArticle(rows.get(0).getMetalakeId(), aggregate(namespace, rows).get(0));
  }

  private KnowledgeBaseArticlePO lockForChange(
      KnowledgeBaseArticleMapper mapper,
      long metalakeId,
      KnowledgeBaseArticleEntity observed,
      Set<Long> desired,
      @Nullable ExpectedRevision expected) {
    KnowledgeBaseArticlePO locked =
        lockObservedArticle(mapper, metalakeId, observed, desired, expected);
    if (locked == null) {
      throw new ArticleRevisionConflictException(
          "Article %s was removed or replaced", observed.nameIdentifier());
    }
    return locked;
  }

  @Nullable
  private KnowledgeBaseArticlePO lockObservedArticle(
      KnowledgeBaseArticleMapper mapper,
      long metalakeId,
      KnowledgeBaseArticleEntity observed,
      Set<Long> desired,
      @Nullable ExpectedRevision expected) {
    // A transformation can change the set of Domain locks. Plan from one statement snapshot,
    // then fence the endpoints in ID order before taking the Article lock.
    Set<Long> affected = Sets.symmetricDifference(observed.domainIds(), desired);
    Long missingDomain = lockLiveDomains(metalakeId, affected);
    KnowledgeBaseArticlePO locked = mapper.lock(metalakeId, observed.name());
    if (locked == null || locked.getArticleId() != observed.id()) {
      return null;
    }
    if (expected != null
        && !expected.matches(metalakeId, locked.getArticleId(), locked.getRevision())) {
      throw new ArticleRevisionConflictException(
          "Article %s has changed", observed.nameIdentifier());
    }
    if (locked.getRevision() != observed.revision()) {
      throw new StaleArticlePlan(observed.nameIdentifier());
    }
    requireDomainsLive(missingDomain);
    return locked;
  }

  @Nullable
  private Long lockLiveDomains(long metalakeId, Set<Long> ids) {
    return SessionUtils.getWithoutCommit(
        DomainMetaMapper.class,
        mapper -> {
          for (long id : new TreeSet<>(ids)) {
            DomainPO domain = mapper.selectDomainMetaByIdForShare(id);
            if (domain == null || domain.metalakeId() != metalakeId) {
              return id;
            }
          }
          return null;
        });
  }

  private void requireDomainsLive(@Nullable Long missingId) {
    if (missingId != null) {
      throw new NoSuchEntityException(
          "Domain %s does not exist in the Article's metalake", missingId);
    }
  }

  private void addBindings(
      KnowledgeBaseArticleMapper mapper, KnowledgeBaseArticlePO row, Set<Binding> added) {
    if (!added.isEmpty()) {
      String audit = newRelationAudit();
      for (Binding binding : added) {
        mapper.bind(row, binding, audit);
      }
    }
  }

  private String newRelationAudit() {
    AuditInfo audit =
        AuditInfo.builder()
            .withCreator(PrincipalUtils.getCurrentUserName())
            .withCreateTime(Instant.now())
            .build();
    try {
      return JsonUtils.anyFieldMapper().writeValueAsString(audit);
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot encode Article relation audit", e);
    }
  }

  private void changeMemberships(KnowledgeBaseArticlePO row, Set<Long> before, Set<Long> after) {
    SessionUtils.doWithoutCommit(
        DomainMemberMapper.class,
        members -> {
          Set<Long> removed = Sets.difference(before, after);
          if (!removed.isEmpty()) {
            long last = members.lastRetirement(row.getMetalakeId(), ARTICLE, row.getArticleId());
            long now = Math.max(System.currentTimeMillis(), Math.incrementExact(last));
            for (long id : removed) {
              members.retire(row.getMetalakeId(), ARTICLE, row.getArticleId(), id, now);
            }
          }
          Set<Long> added = Sets.difference(after, before);
          if (!added.isEmpty()) {
            String audit = newRelationAudit();
            for (long id : added) {
              members.add(row.getMetalakeId(), ARTICLE, row.getArticleId(), id, audit);
            }
          }
        });
  }

  private KnowledgeBaseArticleEntity replace(
      KnowledgeBaseArticleMapper mapper,
      KnowledgeBaseArticlePO row,
      KnowledgeBaseArticleEntity before,
      KnowledgeBaseArticleEntity proposed,
      TransactionHook hook) {
    Preconditions.checkArgument(
        before.id().equals(proposed.id())
            && before.nameIdentifier().equals(proposed.nameIdentifier()),
        "An Article edit must retain its ID, name and metalake");
    proposed.validateTextForWrite(before);
    ArticleBody body = proposed.bodyUpdate();
    boolean bodyChanged =
        body != null
            && !mapper.matchesBody(
                before.id(),
                body.format().name(),
                body.content(),
                Utf8.encodedLength(body.content()));
    if (!bodyChanged
        && before.equals(proposed.toBuilder().withRevision(before.revision()).build())) {
      return before;
    }
    KnowledgeBaseArticleEntity after =
        proposed.toBuilder()
            .withBodyUpdate(null)
            .withRevision(Math.incrementExact(before.revision()))
            .build();
    KnowledgeBaseArticlePO next = encode(after, row.getMetalakeId());
    if (bodyChanged) {
      next.setBodyFormat(body.format().name());
      next.setBodyContent(body.content());
    }
    hook.beforeChange(before, after);
    if (mapper.update(encode(before, row.getMetalakeId()), next) != 1) {
      throw new ArticleRevisionConflictException("Article %s has changed", before.nameIdentifier());
    }
    long now = System.currentTimeMillis();
    for (Binding removed : Sets.difference(before.bindings(), after.bindings())) {
      mapper.unbind(before.id(), removed, now);
    }
    addBindings(mapper, next, Sets.difference(after.bindings(), before.bindings()));
    changeMemberships(next, before.domainIds(), after.domainIds());
    hook.onChange(before, after);
    return after;
  }

  private KnowledgeBaseArticleEntity materialize(
      Namespace ns, KnowledgeBaseArticlePO row, KnowledgeBaseArticleMapper mapper) {
    Set<Long> domains =
        SessionUtils.getWithoutCommit(
            DomainMemberMapper.class,
            members ->
                Set.copyOf(members.domains(row.getMetalakeId(), ARTICLE, row.getArticleId())));
    return decode(ns, row, Set.copyOf(mapper.bindings(row.getArticleId())), domains);
  }

  private KnowledgeBaseArticleEntity decode(
      Namespace ns, KnowledgeBaseArticlePO row, Set<Binding> bindings, Set<Long> domains) {
    try {
      return KnowledgeBaseArticleEntity.builder()
          .withId(row.getArticleId())
          .withNamespace(ns)
          .withName(row.getArticleName())
          .withTitle(row.getTitle())
          .withStatus(ArticleStatus.valueOf(row.getStatus()))
          .withComment(row.getArticleComment())
          .withSynonyms(
              JsonUtils.anyFieldMapper()
                  .readValue(row.getSynonyms(), new TypeReference<List<String>>() {}))
          .withSourceReference(row.getSourceReference())
          .withProperties(
              JsonUtils.anyFieldMapper()
                  .readValue(row.getProperties(), new TypeReference<Map<String, String>>() {}))
          .withAuditInfo(JsonUtils.anyFieldMapper().readValue(row.getAuditInfo(), AuditInfo.class))
          .withRevision(row.getRevision())
          .withDomainIds(domains)
          .withBindings(bindings)
          .build();
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot decode Article " + row.getArticleId(), e);
    }
  }

  private KnowledgeBaseArticlePO encode(KnowledgeBaseArticleEntity entity, long metalakeId) {
    KnowledgeBaseArticlePO row = new KnowledgeBaseArticlePO();
    row.setArticleId(entity.id());
    row.setMetalakeId(metalakeId);
    row.setArticleName(entity.name());
    row.setTitle(entity.title());
    row.setStatus(entity.status().name());
    row.setArticleComment(entity.comment());
    row.setSourceReference(entity.sourceReference());
    row.setRevision(entity.revision());
    try {
      row.setSynonyms(JsonUtils.anyFieldMapper().writeValueAsString(entity.synonyms()));
      row.setProperties(JsonUtils.anyFieldMapper().writeValueAsString(entity.properties()));
      row.setAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(entity.auditInfo()));
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot encode Article " + entity.id(), e);
    }
    return row;
  }
}
