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
import com.google.common.collect.Sets;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.ArticleRevisionConflictException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.kb.ArticleBody;
import org.apache.gravitino.kb.ArticleStatus;
import org.apache.gravitino.kb.KnowledgeBaseArticle;
import org.apache.gravitino.kb.KnowledgeBaseArticleEntity;
import org.apache.gravitino.kb.KnowledgeBaseArticleEntity.Binding;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.NamespacedEntityId;
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
  private static final MemberType ARTICLE = MemberType.KNOWLEDGE_BASE_ARTICLE;
  private static final TransactionHook NO_HOOK = (before, after) -> {};

  /** Storage extension invoked before commit, with the complete old and new binding sets. */
  @FunctionalInterface
  public interface TransactionHook {
    /** Participates in the caller's transaction. Throwing rolls back the write. */
    void onChange(
        @Nullable KnowledgeBaseArticleEntity before, @Nullable KnowledgeBaseArticleEntity after);
  }

  /** Stable identity and revision decoded from an editing token by the caller. */
  public record ExpectedRevision(long metalakeId, long articleId, long revision) {}

  /** Body and stable revision identity read from the same live row. */
  public record StoredContent(ArticleBody body, ExpectedRevision revision) {}

  private KnowledgeBaseArticleMetaService() {}

  /** Returns the shared Article service. */
  public static KnowledgeBaseArticleMetaService getInstance() {
    return INSTANCE;
  }

  /** Returns Article metadata and binding identities from one snapshot. */
  public KnowledgeBaseArticleEntity get(NameIdentifier ident) {
    String name = KnowledgeBaseArticle.normalizeName(ident.name());
    List<KnowledgeBaseArticleEntity> result = read(ident.namespace(), name);
    if (result.isEmpty()) {
      throw missing(ident);
    }
    return result.get(0);
  }

  /** Lists stored metadata and binding identities. */
  public List<KnowledgeBaseArticleEntity> list(Namespace namespace) {
    return read(namespace, null);
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
        SessionUtils.getWithoutCommit(KnowledgeBaseArticleMapper.class, m -> m.ids(metalake, name));
    if (ids == null) {
      throw missing(ident);
    }
    return new NamespacedEntityId(ids.getArticleId(), ids.getMetalakeId());
  }

  /** Returns the requested live entities in input order, omitting missing entries. */
  public List<KnowledgeBaseArticleEntity> batchGet(List<NameIdentifier> idents) {
    Map<Namespace, List<String>> groups = new LinkedHashMap<>();
    for (NameIdentifier ident : idents) {
      groups
          .computeIfAbsent(ident.namespace(), unused -> new ArrayList<>())
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

  /** Commits the initial Owner or extension data along with the Article and initial bindings. */
  public void insert(KnowledgeBaseArticleEntity entity, boolean overwrite, TransactionHook hook)
      throws IOException {
    try {
      write(
          entity.namespace(),
          (mapper, metalakeId) -> {
            KnowledgeBaseArticleEntity before = overwrite ? observe(entity.nameIdentifier()) : null;
            if (before != null) {
              KnowledgeBaseArticlePO row =
                  lockForChange(mapper, metalakeId, before, entity.domainIds());
              replace(mapper, row, before, entity, hook);
              return null;
            }
            requireDomainsLive(lockLiveDomains(metalakeId, entity.domainIds()));
            KnowledgeBaseArticleEntity initial =
                entity.toBuilder().withBodyUpdate(null).withRevision(1).build();
            KnowledgeBaseArticlePO row = encode(initial, metalakeId);
            ArticleBody body =
                Preconditions.checkNotNull(
                    entity.bodyUpdate(), "An initial Article body is required");
            row.setBodyFormat(body.format().name());
            row.setBodyContent(body.content());
            mapper.insert(row);
            initial.bindings().forEach(binding -> mapper.bind(row, binding));
            changeMemberships(row, Set.of(), initial.domainIds());
            hook.onChange(null, initial);
            return null;
          });
    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(e, entity.type(), entity.nameIdentifier().toString());
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
    return write(
        ident.namespace(),
        (mapper, metalakeId) -> {
          KnowledgeBaseArticleEntity before = observe(ident);
          if (before == null) {
            throw missing(ident);
          }
          if (expected != null
              && (expected.metalakeId() != metalakeId
                  || expected.articleId() != before.id()
                  || expected.revision() != before.revision())) {
            throw new ArticleRevisionConflictException("Article %s has changed", ident);
          }
          KnowledgeBaseArticleEntity proposed = updater.apply(before);
          KnowledgeBaseArticlePO row =
              lockForChange(mapper, metalakeId, before, proposed.domainIds());
          return replace(mapper, row, before, proposed, hook);
        });
  }

  /** Retires a live Article and its bindings atomically. */
  public boolean delete(NameIdentifier ident) {
    return delete(ident, NO_HOOK);
  }

  /** Retires an Article and invokes its storage extension with the former state. */
  public boolean delete(NameIdentifier ident, TransactionHook hook) {
    return write(
        ident.namespace(),
        (mapper, metalakeId) -> {
          KnowledgeBaseArticleEntity old = observe(ident);
          if (old == null) {
            return false;
          }
          KnowledgeBaseArticlePO row = lockForChange(mapper, metalakeId, old, Set.of());
          long now = System.currentTimeMillis();
          mapper.deleteBindings(row.getArticleId(), now);
          changeMemberships(row, old.domainIds(), Set.of());
          mapper.delete(row.getArticleId(), now);
          hook.onChange(old, null);
          return true;
        });
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

  private static String metalakeName(Namespace ns) {
    Preconditions.checkArgument(
        ns != null && ns.length() == 1, "Expected a metalake namespace: %s", ns);
    return ns.level(0);
  }

  private static NoSuchEntityException missing(NameIdentifier ident) {
    return new NoSuchEntityException("Knowledge Base Article %s does not exist", ident);
  }

  private static final class StaleArticlePlan extends OptimisticLockException {
    private StaleArticlePlan(NameIdentifier ident) {
      super("Article %s changed during Domain lock planning", ident);
    }
  }

  private <T> T write(Namespace ns, BiFunction<KnowledgeBaseArticleMapper, Long, T> operation) {
    boolean nested = SqlSessions.hasActiveSession();
    for (int attempt = 1; ; attempt++) {
      try {
        return writeOnce(ns, operation);
      } catch (StaleArticlePlan stale) {
        // A caller-owned transaction must release its own locks before the plan can be retried.
        if (nested || attempt >= 5) {
          throw stale;
        }
      }
    }
  }

  private <T> T writeOnce(Namespace ns, BiFunction<KnowledgeBaseArticleMapper, Long, T> operation) {
    String name = metalakeName(ns);
    boolean nested = SqlSessions.hasActiveSession();
    return SessionUtils.doWithCommitAndFetchResult(
        KnowledgeBaseArticleMapper.class,
        mapper -> {
          MetalakeMetaService parents = MetalakeMetaService.getInstance();
          long id = parents.requireMetalakeByName(name).getMetalakeId();
          parents.lockMetalakeForChildWrite(name, id);
          SessionUtils.clearSessionCache();
          if (!nested) {
            return operation.apply(mapper, id);
          }
          // The owner may catch a failed extension and continue using its transaction.
          // Roll back this Article operation without discarding earlier work by that owner.
          try {
            Connection connection = SqlSessions.currentConnection();
            Savepoint savepoint = connection.setSavepoint();
            try {
              T result = operation.apply(mapper, id);
              connection.releaseSavepoint(savepoint);
              return result;
            } catch (RuntimeException | Error failure) {
              try {
                connection.rollback(savepoint);
                connection.releaseSavepoint(savepoint);
              } catch (SQLException rollbackFailure) {
                failure.addSuppressed(rollbackFailure);
              } finally {
                SessionUtils.clearSessionCache();
              }
              throw failure;
            }
          } catch (SQLException e) {
            throw new IllegalStateException("Cannot manage nested Article write", e);
          }
        });
  }

  private List<KnowledgeBaseArticleEntity> read(Namespace ns, @Nullable String name) {
    String metalake = metalakeName(ns);
    if (SqlSessions.hasActiveSession()) {
      // A caller's READ_COMMITTED transaction may contain uncommitted writes. A single
      // statement observes them without splitting the row and its bindings across snapshots.
      return SessionUtils.getWithoutCommitFresh(
          KnowledgeBaseArticleMapper.class, m -> aggregate(ns, m.joined(metalake, name)));
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
                  if (name != null) {
                    KnowledgeBaseArticlePO row = m.get(metalakeId, name);
                    return row == null ? List.of() : List.of(materialize(ns, row, m));
                  }
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

  private List<KnowledgeBaseArticleEntity> readBatch(Namespace ns, List<String> names) {
    String metalake = metalakeName(ns);
    if (names.isEmpty()) {
      return List.of();
    }
    if (SqlSessions.hasActiveSession()) {
      return SessionUtils.getWithoutCommitFresh(
          KnowledgeBaseArticleMapper.class, m -> aggregate(ns, m.joinedBatch(metalake, names)));
    }
    return SqlSessions.readSnapshot(
        () ->
            SessionUtils.getWithoutCommit(
                KnowledgeBaseArticleMapper.class,
                m -> aggregate(ns, m.joinedBatch(metalake, names))));
  }

  /** Folds one joined row per Article, binding and membership into complete entities. */
  private List<KnowledgeBaseArticleEntity> aggregate(
      Namespace ns, List<KnowledgeBaseArticlePO> rows) {
    Map<Long, Set<Binding>> bindings = new LinkedHashMap<>();
    Map<Long, Set<Long>> domains = new HashMap<>();
    Map<Long, KnowledgeBaseArticlePO> articles = new LinkedHashMap<>();
    for (KnowledgeBaseArticlePO row : rows) {
      articles.putIfAbsent(row.getArticleId(), row);
      Set<Binding> targets =
          bindings.computeIfAbsent(row.getArticleId(), unused -> new HashSet<>());
      if (row.getDomainId() != null) {
        domains
            .computeIfAbsent(row.getArticleId(), unused -> new HashSet<>())
            .add(row.getDomainId());
      }
      if (row.getTargetId() != null) {
        targets.add(new Binding(row.getTargetType(), row.getTargetId()));
      }
    }
    return articles.values().stream()
        .map(
            row ->
                decode(
                    ns,
                    row,
                    bindings.get(row.getArticleId()),
                    domains.getOrDefault(row.getArticleId(), Set.of())))
        .toList();
  }

  @Nullable
  private KnowledgeBaseArticleEntity observe(NameIdentifier ident) {
    List<KnowledgeBaseArticleEntity> articles =
        read(ident.namespace(), KnowledgeBaseArticle.normalizeName(ident.name()));
    return articles.isEmpty() ? null : articles.get(0);
  }

  private KnowledgeBaseArticlePO lockForChange(
      KnowledgeBaseArticleMapper mapper,
      long metalakeId,
      KnowledgeBaseArticleEntity observed,
      Set<Long> desired) {
    // A transformation can change the set of Domain locks. Plan from one statement snapshot,
    // then fence the endpoints in ID order before taking the Article lock.
    Set<Long> affected = Sets.symmetricDifference(observed.domainIds(), desired);
    Long missingDomain = lockLiveDomains(metalakeId, affected);
    KnowledgeBaseArticlePO locked = mapper.lock(metalakeId, observed.name());
    if (locked == null || locked.getArticleId() != observed.id()) {
      throw new ArticleRevisionConflictException(
          "Article %s was removed or replaced", observed.nameIdentifier());
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
            AuditInfo audit =
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentUserName())
                    .withCreateTime(Instant.now())
                    .build();
            try {
              String auditJson = JsonUtils.anyFieldMapper().writeValueAsString(audit);
              for (long id : added) {
                members.add(row.getMetalakeId(), ARTICLE, row.getArticleId(), id, auditJson);
              }
            } catch (IOException e) {
              throw new UncheckedIOException("Cannot encode Article membership audit", e);
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
    mapper.update(encode(before, row.getMetalakeId()), next);
    long now = System.currentTimeMillis();
    for (Binding removed : Sets.difference(before.bindings(), after.bindings())) {
      mapper.unbind(before.id(), removed, now);
    }
    for (Binding added : Sets.difference(after.bindings(), before.bindings())) {
      mapper.bind(next, added);
    }
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
