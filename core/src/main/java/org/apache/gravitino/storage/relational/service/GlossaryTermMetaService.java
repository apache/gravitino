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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.glossary.GlossaryTerm.Status;
import org.apache.gravitino.glossary.TermLink;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.GlossaryTermEntity;
import org.apache.gravitino.meta.GlossaryTermNames;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermMetaMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.po.GlossaryTermPO;
import org.apache.gravitino.storage.relational.po.GlossaryTermSynonymPO;
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
 * name-versus-synonym checks belong to the glossary manager, not this persistence service.
 */
public final class GlossaryTermMetaService {
  private static final GlossaryTermMetaService INSTANCE = new GlossaryTermMetaService();

  private GlossaryTermMetaService() {}
  /** Returns the shared service. */
  public static GlossaryTermMetaService getInstance() {
    return INSTANCE;
  }

  /** Inserts a definition. Overwrite retains the stored identity and increments its revision. */
  public void insert(GlossaryTermEntity entity, boolean overwritten) throws IOException {
    try {
      transaction(
          entity.namespace(),
          mapper -> {
            long metalakeId = metalake(entity.namespace()).getMetalakeId();
            GlossaryTermPO old = mapper.get(metalakeId, GlossaryTermNames.key(entity.name()), true);
            GlossaryTermPO row = toPO(entity, metalakeId);
            if (overwritten && old != null) {
              row.setTermId(old.getTermId());
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

  /** Loads a definition and its synonyms in one protected transaction. */
  public GlossaryTermEntity get(NameIdentifier ident) {
    return transaction(
        ident.namespace(),
        mapper -> fromPO(require(mapper, ident, false), mapper, ident.namespace()));
  }

  /** Resolves term and metalake identities from the same protected row snapshot. */
  public NamespacedEntityId getIds(NameIdentifier ident) {
    return transaction(
        ident.namespace(),
        mapper -> {
          GlossaryTermPO row = require(mapper, ident, false);
          return new NamespacedEntityId(row.getTermId(), row.getMetalakeId());
        });
  }

  /** Lists definitions belonging to the requested metalake. */
  public List<GlossaryTermEntity> list(Namespace ns) {
    return transaction(
        ns,
        mapper -> {
          long metalakeId = metalake(ns).getMetalakeId();
          List<GlossaryTermPO> rows = mapper.list(metalakeId);
          Map<Long, List<String>> synonyms = new LinkedHashMap<>();
          for (GlossaryTermSynonymPO synonym : mapper.listSynonyms(metalakeId)) {
            synonyms
                .computeIfAbsent(synonym.getTermId(), ignored -> new ArrayList<>())
                .add(synonym.getSynonym());
          }
          List<GlossaryTermEntity> result = new ArrayList<>();
          for (GlossaryTermPO row : rows) {
            result.add(fromPO(row, synonyms.getOrDefault(row.getTermId(), List.of()), ns));
          }
          return result;
        });
  }

  /**
   * Loads requested terms, omitting missing names.
   *
   * <p>Like the existing user/role fallback, this accepts identifiers from different metalakes and
   * performs a protected read per identifier. Use list for a complete metalake: it batches synonyms
   * and does not issue a query per returned term.
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
          mapper -> {
            GlossaryTermPO old = require(mapper, ident, true);
            GlossaryTermEntity updated =
                (GlossaryTermEntity) updater.apply((E) fromPO(old, mapper, ident.namespace()));
            if (!Objects.equals(old.getTermId(), updated.id())
                || !ident.namespace().equals(updated.namespace()))
              throw new IllegalArgumentException(
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

  /** Soft-deletes a definition and all its synonyms; a missing definition returns false. */
  public boolean delete(NameIdentifier ident) {
    try {
      return transaction(
          ident.namespace(),
          mapper -> {
            GlossaryTermPO row = require(mapper, ident, true);
            long deletedAt = Math.max(1, System.currentTimeMillis());
            requireUpdated(
                mapper.delete(row.getTermId(), row.getCurrentVersion(), deletedAt), ident);
            mapper.deleteSynonyms(row.getTermId(), deletedAt);
            return true;
          });
    } catch (NoSuchEntityException e) {
      return false;
    }
  }

  /** Soft-deletes definitions and synonyms inside the metalake deletion transaction. */
  public void dropMetalake(long metalakeId) {
    long deletedAt = Math.max(1, System.currentTimeMillis());
    SessionUtils.doWithoutCommit(
        GlossaryTermMetaMapper.class,
        mapper -> {
          mapper.dropMetalake(metalakeId, deletedAt);
          mapper.dropMetalakeSynonyms(metalakeId, deletedAt);
        });
  }

  /** Purges at most limit expired definitions and their deleted synonyms atomically. */
  public int collect(long cutoff, int limit) {
    return SessionUtils.doWithCommitAndFetchResult(
        GlossaryTermMetaMapper.class,
        mapper -> {
          int count = 0;
          for (Long id : mapper.expired(cutoff, limit)) {
            mapper.purgeSynonyms(id);
            count += mapper.purge(id);
          }
          return count;
        });
  }

  private <T> T transaction(Namespace ns, Function<GlossaryTermMetaMapper, T> work) {
    if (ns.length() != 3
        || !Entity.SYSTEM_CATALOG_RESERVED_NAME.equals(ns.level(1))
        || !"glossary_term".equals(ns.level(2))) {
      throw new IllegalArgumentException("Expected glossary term namespace: " + ns);
    }
    return SessionUtils.doWithCommitAndFetchResult(
        GlossaryTermMetaMapper.class,
        mapper -> {
          MetalakePO observed = metalake(ns);
          // Fence metalake deletion; definition row locks keep the root and synonyms consistent.
          // MySQL/PostgreSQL share the parent lock across terms. H2 uses an exclusive parent
          // lock instead, serializing operations within a metalake.
          MetalakePO locked =
              SessionUtils.getWithoutCommit(
                  MetalakeMetaMapper.class,
                  m -> m.selectMetalakeMetaByIdForShare(observed.getMetalakeId()));
          if (locked == null || !Objects.equals(locked.getMetalakeName(), ns.level(0)))
            throw new NoSuchEntityException("Metalake %s no longer exists", ns.level(0));
          SessionUtils.clearSessionCache();
          return work.apply(mapper);
        });
  }

  private MetalakePO metalake(Namespace ns) {
    return MetalakeMetaService.getInstance().requireMetalakeByName(ns.level(0));
  }

  private GlossaryTermPO require(
      GlossaryTermMetaMapper mapper, NameIdentifier ident, boolean write) {
    GlossaryTermPO row =
        mapper.get(
            metalake(ident.namespace()).getMetalakeId(),
            GlossaryTermNames.key(ident.name()),
            write);
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
    return fromPO(row, mapper.synonyms(row.getTermId()), ns);
  }

  private GlossaryTermEntity fromPO(GlossaryTermPO row, List<String> synonyms, Namespace ns) {
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
