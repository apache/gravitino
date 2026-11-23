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
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.glossary.GlossaryTerm.Status;
import org.apache.gravitino.glossary.TermLink;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.GlossaryTermEntity;
import org.apache.gravitino.meta.GlossaryTermNames;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermMetaMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.po.GlossaryTermPO;
import org.apache.gravitino.storage.relational.po.GlossaryTermSnapshotPO;
import org.apache.gravitino.storage.relational.po.MetalakePO;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/**
 * Transactional persistence of glossary definitions and their synonyms.
 *
 * <p>Operations join an existing SessionUtils transaction or commit their own. Missing definitions
 * throw NoSuchEntityException, except delete (false) and batchGet (omitted). Duplicate names,
 * source references and reserved IDs are translated to EntityAlreadyExistsException. Other database
 * failures propagate through the existing store exception converter.
 *
 * <p>Namespace-based operations reject namespaces other than {@code
 * <metalake>.system.glossary_term} with IllegalArgumentException before accessing storage.
 *
 * <p>Insert starts at revision 1; update and overwrite increment the stored revision once. Callers
 * must detect semantic no-ops before invoking a write. Relation validation, authorization and the
 * name-versus-synonym checks belong to the glossary manager, not this persistence service. Shared
 * synonyms across terms are allowed. Name/synonym conflicts are not rejected here, even for
 * sequential writes; the parent lock and database unique keys do not enforce that cross-table rule.
 * Before exposing manager/API writes, the manager must take the per-metalake hierarchy lock, read
 * fresh state, and check conflicts in both directions. It must keep that lock through commit in the
 * same transaction for create, rename, overwrite and synonym changes. Reads use a single statement
 * snapshot for definitions, synonyms and the live parent name without taking row locks.
 */
public final class GlossaryTermMetaService {
  private static final int LOOKUP_BATCH_SIZE = 500;
  private static final GlossaryTermMetaService INSTANCE = new GlossaryTermMetaService();

  private GlossaryTermMetaService() {}
  /** Returns the shared service. */
  public static GlossaryTermMetaService getInstance() {
    return INSTANCE;
  }

  /**
   * Inserts a definition, or overwrites the same identity and increments its revision.
   *
   * @throws IllegalArgumentException if overwrite finds the name with a different entity ID; load
   *     the existing entity and use its ID before overwriting
   */
  public void insert(GlossaryTermEntity entity, boolean overwritten) throws IOException {
    try {
      transaction(
          entity.namespace(),
          (mapper, metalakeId) -> {
            GlossaryTermPO old =
                mapper.getForUpdate(metalakeId, GlossaryTermNames.key(entity.name()));
            GlossaryTermPO row = toPO(entity, metalakeId);
            if (overwritten && old != null) {
              Preconditions.checkArgument(
                  Objects.equals(old.getTermId(), entity.id()),
                  "Cannot overwrite glossary term %s with ID %s; load and retain existing ID %s",
                  entity.nameIdentifier(),
                  entity.id(),
                  old.getTermId());
              row.setCurrentVersion(Math.addExact(old.getCurrentVersion(), 1));
              requireUpdated(mapper.update(row, old.getCurrentVersion()), entity.nameIdentifier());
              mapper.clearSynonyms(row.getTermId());
            } else {
              row.setCurrentVersion(1L);
              mapper.insert(row);
            }
            writeSynonyms(mapper, row, entity.synonyms());
            return null;
          });
    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(
          e, Entity.EntityType.GLOSSARY_TERM, entity.nameIdentifier().toString());
      throw e;
    }
  }

  /** Loads a definition and its synonyms from one statement snapshot without row locks. */
  public GlossaryTermEntity get(NameIdentifier ident) {
    return getWithMetalakeId(ident).term();
  }

  /** A live term and its parent identity from the same definition snapshot. */
  public record ScopedTerm(long metalakeId, GlossaryTermEntity term) {}

  /**
   * Loads the term and metalake identity used to scope subsequent relation queries.
   *
   * @throws NoSuchEntityException if the term or its parent metalake does not exist
   */
  public ScopedTerm getWithMetalakeId(NameIdentifier ident) {
    return readTerm(ident, null);
  }

  /**
   * Loads a term from the metalake identity resolved in the caller's snapshot.
   *
   * @throws NoSuchEntityException if the term or its parent metalake does not exist
   */
  public GlossaryTermEntity get(NameIdentifier ident, long metalakeId) {
    return readTerm(ident, metalakeId).term();
  }

  /** Resolves term and metalake identities without row locks. */
  public NamespacedEntityId getIds(NameIdentifier ident) {
    validateNamespace(ident.namespace());
    return SessionUtils.getWithoutCommit(
        GlossaryTermMetaMapper.class,
        mapper -> {
          long metalakeId = metalake(ident.namespace()).getMetalakeId();
          GlossaryTermPO row =
              mapper.getIds(
                  metalakeId, ident.namespace().level(0), GlossaryTermNames.key(ident.name()));
          if (row == null) {
            throw new NoSuchEntityException("Glossary term %s does not exist", ident);
          }
          return new NamespacedEntityId(row.getTermId(), row.getMetalakeId());
        });
  }

  /** Lists definitions and synonyms from one statement snapshot without row locks. */
  public List<GlossaryTermEntity> list(Namespace ns) {
    return definitions(ns, readRows(ns, null, null));
  }

  /** Reads all definitions for the metalake identity resolved in the caller's snapshot. */
  public List<GlossaryTermEntity> list(Namespace ns, long metalakeId) {
    return definitions(ns, readRows(ns, metalakeId, null));
  }

  /**
   * Loads requested terms, omitting missing names.
   *
   * <p>Like the existing user/role fallback, this accepts identifiers from different metalakes and
   * performs a snapshot read per identifier. Use list for a complete metalake: it reads definitions
   * and synonyms together without issuing a query per returned term.
   */
  public List<GlossaryTermEntity> batchGet(List<NameIdentifier> identifiers) {
    List<GlossaryTermEntity> result = new ArrayList<>();
    for (NameIdentifier ident : identifiers) {
      try {
        result.add(get(ident));
      } catch (NoSuchEntityException ignored) {
        /* Missing entries are omitted. */
      }
    }
    return result;
  }

  /** Updates one definition atomically with its synonyms, preserving its identity and namespace. */
  public <E extends Entity & HasIdentifier> GlossaryTermEntity update(
      NameIdentifier ident, Function<E, E> updater) throws IOException {
    try {
      return transaction(
          ident.namespace(),
          (mapper, metalakeId) -> {
            GlossaryTermPO old = require(mapper, ident, metalakeId);
            GlossaryTermEntity updated =
                (GlossaryTermEntity) updater.apply((E) fromPO(old, mapper, ident.namespace()));
            Preconditions.checkArgument(
                Objects.equals(old.getTermId(), updated.id())
                    && ident.namespace().equals(updated.namespace()),
                "A glossary update must retain identity and namespace");
            GlossaryTermPO row = toPO(updated, old.getMetalakeId());
            row.setCurrentVersion(Math.addExact(old.getCurrentVersion(), 1));
            requireUpdated(mapper.update(row, old.getCurrentVersion()), ident);
            mapper.clearSynonyms(row.getTermId());
            writeSynonyms(mapper, row, updated.synonyms());
            return fromPO(row, mapper, ident.namespace());
          });
    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(e, Entity.EntityType.GLOSSARY_TERM, ident.toString());
      throw e;
    }
  }

  /**
   * Soft-deletes the definition, synonyms, incident edges and domain membership. Returns false if
   * the term is absent in a live metalake.
   *
   * @throws NoSuchEntityException if the parent metalake is absent or disappears before locking
   */
  public boolean delete(NameIdentifier ident) {
    return transaction(
        ident.namespace(),
        true,
        (mapper, metalakeId) -> {
          GlossaryTermPO row = mapper.getForUpdate(metalakeId, GlossaryTermNames.key(ident.name()));
          if (row == null) {
            return false;
          }
          long deletedAt = Math.max(1, System.currentTimeMillis());
          requireUpdated(mapper.delete(row.getTermId(), row.getCurrentVersion(), deletedAt), ident);
          mapper.deleteSynonyms(row.getTermId(), deletedAt);
          SessionUtils.doWithoutCommit(
              OwnerMetaMapper.class,
              owners ->
                  owners.softDeleteOwnerRelByMetadataObjectIdAndType(
                      row.getTermId(), Entity.EntityType.GLOSSARY_TERM.name()));
          GlossaryTermRelService.getInstance().removeTerm(metalakeId, row.getTermId());
          GlossaryTermDomainService.getInstance().removeTerm(row.getTermId());
          return true;
        });
  }

  /** Soft-deletes definitions, synonyms, edges and domain membership inside metalake deletion. */
  public void dropMetalake(long metalakeId) {
    long deletedAt = Math.max(1, System.currentTimeMillis());
    SessionUtils.doWithoutCommit(
        GlossaryTermMetaMapper.class,
        mapper -> {
          mapper.dropMetalake(metalakeId, deletedAt);
          mapper.dropMetalakeSynonyms(metalakeId, deletedAt);
          GlossaryTermRelService.getInstance().dropMetalake(metalakeId);
          GlossaryTermDomainService.getInstance().dropMetalake(metalakeId);
        });
  }

  /**
   * Purges at most limit expired relations, domain memberships and definitions, with the
   * definitions' synonyms, atomically.
   */
  public int collect(long cutoff, int limit) {
    return SessionUtils.doWithCommitAndFetchResult(
        GlossaryTermMetaMapper.class,
        mapper -> {
          int count = GlossaryTermRelService.getInstance().collect(cutoff, limit);
          if (count == limit) {
            return count;
          }
          count += GlossaryTermDomainService.getInstance().collect(cutoff, limit - count);
          if (count == limit) {
            return count;
          }
          for (Long id : mapper.expired(cutoff, limit - count)) {
            mapper.purgeSynonyms(id);
            count += mapper.purge(id);
          }
          return count;
        });
  }

  /** Runs a create request with the live parent and hierarchy locked through commit. */
  public <T> T withHierarchyLock(Namespace namespace, Supplier<T> work) {
    return withHierarchyLock(namespace, lake -> work.get());
  }

  /** Runs creation with the locked metalake ID and preserves the missing-metalake error. */
  public <T> T withHierarchyLock(Namespace namespace, Function<Long, T> work) {
    return transaction(
        namespace,
        true,
        missing -> {
          throw new NoSuchMetalakeException(
              missing, "Metalake %s does not exist", namespace.level(0));
        },
        (mapper, metalakeId) -> work.apply(metalakeId));
  }

  /**
   * Locks the parent, optional hierarchy mutex, and current term before invoking the operation.
   * Supplies an empty term if either the term or its parent is absent.
   */
  public <T> T withLockedTerm(
      NameIdentifier ident,
      boolean hierarchyLock,
      Function<Optional<GlossaryTermEntity>, T> operation) {
    return transaction(
        ident.namespace(),
        hierarchyLock,
        missing -> operation.apply(Optional.empty()),
        (mapper, metalakeId) -> {
          GlossaryTermPO row = mapper.getForUpdate(metalakeId, GlossaryTermNames.key(ident.name()));
          return operation.apply(
              Optional.ofNullable(row).map(term -> fromPO(term, mapper, ident.namespace())));
        });
  }

  /** Runs a graph operation under the live parent fence and hierarchy mutex. */
  public <T> T withHierarchyLock(
      Namespace ns, Function<Long, T> operation, Function<NoSuchEntityException, T> missingParent) {
    return transaction(ns, true, missingParent, (mapper, lake) -> operation.apply(lake));
  }

  /** Checks indexed name/synonym conflicts inside the caller's hierarchy transaction. */
  public void checkTermNames(GlossaryTermEntity term) {
    String name = GlossaryTermNames.key(term.name());
    long metalakeId = metalake(term.namespace()).getMetalakeId();
    SessionUtils.doWithoutCommit(
        GlossaryTermMetaMapper.class,
        mapper -> {
          Preconditions.checkArgument(
              mapper.conflictingSynonym(metalakeId, term.id(), name) == null,
              "Term name conflicts with a synonym");
          List<String> keys = new ArrayList<>();
          for (String synonym : term.synonyms()) {
            String key = GlossaryTermNames.key(synonym);
            Preconditions.checkArgument(
                !name.equals(key), "Term name conflicts with its own synonym");
            keys.add(key);
          }
          Preconditions.checkArgument(
              keys.isEmpty() || mapper.conflictingName(metalakeId, term.id(), keys) == null,
              "Synonym conflicts with a term name");
        });
  }

  private void validateNamespace(Namespace ns) {
    Preconditions.checkArgument(
        ns != null
            && ns.length() == 3
            && Entity.SYSTEM_CATALOG_RESERVED_NAME.equals(ns.level(1))
            && "glossary_term".equals(ns.level(2)),
        "Expected glossary term namespace: %s",
        ns);
  }

  private <T> T transaction(Namespace ns, BiFunction<GlossaryTermMetaMapper, Long, T> work) {
    return transaction(ns, false, work);
  }

  private <T> T transaction(
      Namespace ns, boolean hierarchyLock, BiFunction<GlossaryTermMetaMapper, Long, T> work) {
    return transaction(
        ns,
        hierarchyLock,
        missing -> {
          throw missing;
        },
        work);
  }

  private <T> T transaction(
      Namespace ns,
      boolean hierarchyLock,
      Function<NoSuchEntityException, T> missingParent,
      BiFunction<GlossaryTermMetaMapper, Long, T> work) {
    validateNamespace(ns);
    return SessionUtils.doWithCommitAndFetchResult(
        GlossaryTermMetaMapper.class,
        mapper -> {
          MetalakePO observed;
          try {
            observed = metalake(ns);
          } catch (NoSuchEntityException e) {
            return missingParent.apply(e);
          }
          // Fence metalake deletion; definition row locks keep the root and synonyms consistent.
          // MySQL/PostgreSQL share the parent lock across terms. H2 uses an exclusive parent
          // lock instead, serializing operations within a metalake. This is not the glossary
          // hierarchy mutex: a caller's name/synonym precheck before this lock can become stale,
          // even on H2. The manager must perform that check under the dedicated hierarchy mutex
          // in this same transaction; making only this parent lock exclusive is insufficient.
          MetalakePO locked =
              SessionUtils.getWithoutCommit(
                  MetalakeMetaMapper.class,
                  m -> m.selectMetalakeMetaByIdForShare(observed.getMetalakeId()));
          if (locked == null || !Objects.equals(locked.getMetalakeName(), ns.level(0))) {
            return missingParent.apply(
                new NoSuchEntityException("Metalake %s no longer exists", ns.level(0)));
          }
          if (hierarchyLock) {
            // Term deletion must exclude new edges before locking the term itself.
            GlossaryTermRelService.getInstance().lockHierarchy(locked.getMetalakeId());
          }
          SessionUtils.clearSessionCache();
          return work.apply(mapper, locked.getMetalakeId());
        });
  }

  private ScopedTerm readTerm(NameIdentifier ident, @Nullable Long metalakeId) {
    Namespace ns = ident.namespace();
    List<GlossaryTermSnapshotPO> rows =
        readRows(ns, metalakeId, GlossaryTermNames.key(ident.name()));
    List<GlossaryTermEntity> terms = definitions(ns, rows);
    if (terms.isEmpty()) {
      throw new NoSuchEntityException("Glossary term %s does not exist", ident);
    }
    return new ScopedTerm(rows.get(0).getMetalakeId(), terms.get(0));
  }

  private List<GlossaryTermSnapshotPO> readRows(
      Namespace ns, @Nullable Long metalakeId, @Nullable String key) {
    validateNamespace(ns);
    return SessionUtils.getWithoutCommit(
        GlossaryTermMetaMapper.class,
        mapper ->
            mapper.snapshot(
                metalakeId != null ? metalakeId : metalake(ns).getMetalakeId(), ns.level(0), key));
  }

  /**
   * Resolves normalized names in the fenced metalake before a relation writer locks term rows.
   * Missing names are omitted so the writer can check the root revision before reporting them.
   */
  public Map<String, Long> getIdsByKeys(long metalakeId, Set<String> keys) {
    Map<String, Long> result = new LinkedHashMap<>();
    for (List<String> batch : Lists.partition(List.copyOf(keys), LOOKUP_BATCH_SIZE)) {
      for (GlossaryTermPO row :
          SessionUtils.getWithoutCommit(
              GlossaryTermMetaMapper.class, mapper -> mapper.idsByKeys(metalakeId, batch))) {
        result.put(row.getNameKey(), row.getTermId());
      }
    }
    return result;
  }

  /** Loads live terms in bounded batches from the named metalake and its observed identity. */
  public List<GlossaryTermEntity> getByIds(Namespace ns, long lake, Set<Long> ids) {
    validateNamespace(ns);
    List<GlossaryTermEntity> result = new ArrayList<>();
    for (List<Long> batch : Lists.partition(List.copyOf(ids), LOOKUP_BATCH_SIZE)) {
      result.addAll(
          SessionUtils.getWithoutCommit(
              GlossaryTermMetaMapper.class,
              mapper -> definitions(ns, mapper.snapshotByIds(lake, ns.level(0), batch))));
    }
    return result;
  }

  private List<GlossaryTermEntity> definitions(Namespace ns, List<GlossaryTermSnapshotPO> rows) {
    if (rows.isEmpty()) {
      throw new NoSuchEntityException("Metalake %s no longer exists", ns.level(0));
    }
    // A live parent with no matching terms contributes one outer-join row.
    if (rows.get(0).getTermId() == null) {
      return new ArrayList<>();
    }
    Map<Long, GlossaryTermPO> definitions = new LinkedHashMap<>();
    Map<Long, List<String>> synonyms = new LinkedHashMap<>();
    for (GlossaryTermSnapshotPO row : rows) {
      definitions.putIfAbsent(row.getTermId(), row);
      List<String> values = synonyms.computeIfAbsent(row.getTermId(), ignored -> new ArrayList<>());
      if (row.getSynonym() != null) {
        values.add(row.getSynonym());
      }
    }
    // Read membership in the caller's session, so it comes from the same snapshot.
    Map<Long, List<String>> domains =
        GlossaryTermDomainService.getInstance()
            .domainNames(rows.get(0).getMetalakeId(), definitions.keySet());
    List<GlossaryTermEntity> result = new ArrayList<>();
    definitions.forEach(
        (id, row) -> result.add(fromPO(row, synonyms.get(id), domains.get(id), ns)));
    return result;
  }

  private MetalakePO metalake(Namespace ns) {
    return MetalakeMetaService.getInstance().requireMetalakeByName(ns.level(0));
  }

  private GlossaryTermPO require(
      GlossaryTermMetaMapper mapper, NameIdentifier ident, long metalakeId) {
    GlossaryTermPO row = mapper.getForUpdate(metalakeId, GlossaryTermNames.key(ident.name()));
    if (row == null) throw new NoSuchEntityException("Glossary term %s does not exist", ident);
    return row;
  }

  private void requireUpdated(int count, NameIdentifier ident) {
    if (count != 1)
      throw ExceptionUtils.concurrentModification(Entity.EntityType.GLOSSARY_TERM, ident);
  }

  private void writeSynonyms(GlossaryTermMetaMapper mapper, GlossaryTermPO row, String[] synonyms) {
    Map<String, String> unique = new LinkedHashMap<>();
    for (String synonym : synonyms) unique.putIfAbsent(GlossaryTermNames.key(synonym), synonym);
    unique.forEach(
        (key, value) -> mapper.addSynonym(row.getTermId(), row.getMetalakeId(), value, key));
  }

  private GlossaryTermPO toPO(GlossaryTermEntity entity, long metalakeId) {
    GlossaryTermPO row = new GlossaryTermPO();
    row.setTermId(entity.id());
    row.setMetalakeId(metalakeId);
    row.setName(entity.name());
    row.setNameKey(GlossaryTermNames.key(entity.name()));
    row.setDefinition(entity.definition());
    row.setStatus(entity.status().name());
    row.setSourceRef(entity.sourceRef());
    row.setCurrentVersion(entity.version());
    row.setDeletedAt(0L);
    row.setLinks(
        json(
            Arrays.stream(entity.links())
                .map(
                    link -> {
                      Map<String, String> fields = new LinkedHashMap<>();
                      fields.put("label", link.label());
                      fields.put("url", link.url());
                      return fields;
                    })
                .toList()));
    row.setProperties(json(entity.properties()));
    row.setAuditInfo(json(entity.auditInfo()));
    return row;
  }

  private GlossaryTermEntity fromPO(
      GlossaryTermPO row, GlossaryTermMetaMapper mapper, Namespace ns) {
    return fromPO(
        row,
        mapper.synonyms(row.getTermId()),
        GlossaryTermDomainService.getInstance()
            .domainNames(row.getMetalakeId(), List.of(row.getTermId()))
            .get(row.getTermId()),
        ns);
  }

  private GlossaryTermEntity fromPO(
      GlossaryTermPO row, List<String> synonyms, @Nullable List<String> domains, Namespace ns) {
    try {
      List<Map<String, String>> links =
          JsonUtils.anyFieldMapper()
              .readValue(row.getLinks(), new TypeReference<List<Map<String, String>>>() {});
      return GlossaryTermEntity.builder()
          .withId(row.getTermId())
          .withNamespace(ns)
          .withName(row.getName())
          .withDefinition(row.getDefinition())
          .withStatus(Status.valueOf(row.getStatus()))
          .withSourceRef(row.getSourceRef())
          .withVersion(row.getCurrentVersion())
          .withSynonyms(synonyms.toArray(new String[0]))
          .withDomains(domains == null ? new String[0] : domains.toArray(new String[0]))
          .withLinks(
              links.stream()
                  .map(link -> TermLink.of(link.get("label"), link.get("url")))
                  .toArray(TermLink[]::new))
          .withProperties(
              JsonUtils.anyFieldMapper()
                  .readValue(row.getProperties(), new TypeReference<Map<String, String>>() {}))
          .withAuditInfo(JsonUtils.anyFieldMapper().readValue(row.getAuditInfo(), AuditInfo.class))
          .build();
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot decode glossary term " + row.getTermId(), e);
    }
  }

  private String json(Object value) {
    try {
      return JsonUtils.anyFieldMapper().writeValueAsString(value);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
