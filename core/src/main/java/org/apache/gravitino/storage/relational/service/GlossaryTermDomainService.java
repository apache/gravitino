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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.storage.relational.mapper.DomainMetaMapper;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermDomainRelMapper;
import org.apache.gravitino.storage.relational.po.DomainPO;
import org.apache.gravitino.storage.relational.po.GlossaryTermDomainRelPO;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/**
 * Explicit glossary term domain membership. Only the glossary term write path changes it. Every
 * method joins the caller's session. Writers hold the term row lock, and lock domain rows only
 * after every term row of the request, with {@link #lockDomain}.
 */
public final class GlossaryTermDomainService {
  private static final int BATCH_SIZE = 500;
  private static final GlossaryTermDomainService INSTANCE = new GlossaryTermDomainService();

  private GlossaryTermDomainService() {}

  /** Returns the shared service. */
  public static GlossaryTermDomainService getInstance() {
    return INSTANCE;
  }

  /**
   * Resolves a live domain by name and locks its row for share until the caller's transaction ends,
   * so a concurrent domain drop waits for this transaction or makes it fail.
   *
   * @param metalakeId The live metalake ID.
   * @param name The domain name.
   * @return The locked domain row.
   * @throws NoSuchEntityException If the domain is missing, deleted, in another metalake, or
   *     renamed after the name lookup.
   */
  public DomainPO lockDomain(long metalakeId, String name) {
    return SessionUtils.getWithoutCommit(
        DomainMetaMapper.class,
        mapper -> {
          DomainPO observed = mapper.selectDomainMetaByMetalakeIdAndName(metalakeId, name);
          DomainPO locked =
              observed == null ? null : mapper.selectDomainMetaByIdForShare(observed.domainId());
          if (locked == null
              || locked.deletedAt() != 0
              || !Objects.equals(locked.metalakeId(), metalakeId)
              || !Objects.equals(locked.domainName(), name)) {
            throw new NoSuchEntityException("Domain %s does not exist", name);
          }
          return locked;
        });
  }

  /** Returns a live domain by name without locking it, or null. */
  @Nullable
  public DomainPO findDomain(long metalakeId, String name) {
    return SessionUtils.getWithoutCommit(
        DomainMetaMapper.class,
        mapper -> mapper.selectDomainMetaByMetalakeIdAndName(metalakeId, name));
  }

  /**
   * Returns the names of the live domains of each term, in ascending name order. Terms without
   * domains are absent from the result.
   */
  public Map<Long, List<String>> domainNames(long metalakeId, Collection<Long> termIds) {
    Map<Long, List<String>> result = new LinkedHashMap<>();
    if (termIds.isEmpty()) {
      return result;
    }
    SessionUtils.doWithoutCommit(
        GlossaryTermDomainRelMapper.class,
        mapper -> {
          for (List<Long> batch : Lists.partition(List.copyOf(termIds), BATCH_SIZE)) {
            for (GlossaryTermDomainRelPO row : mapper.listDomainNames(metalakeId, batch)) {
              result
                  .computeIfAbsent(row.getTermId(), id -> new ArrayList<>())
                  .add(row.getDomainName());
            }
          }
        });
    return result;
  }

  /** Returns the live domains of one term as domain ID to name, in ascending name order. */
  public Map<Long, String> domainsOf(long metalakeId, long termId) {
    Map<Long, String> result = new LinkedHashMap<>();
    SessionUtils.doWithoutCommit(
        GlossaryTermDomainRelMapper.class,
        mapper -> {
          for (GlossaryTermDomainRelPO row : mapper.listDomainNames(metalakeId, List.of(termId))) {
            result.put(row.getDomainId(), row.getDomainName());
          }
        });
    return result;
  }

  /** Returns the live domain IDs of a term. */
  public Set<Long> domainIds(long termId) {
    return new HashSet<>(
        SessionUtils.getWithoutCommit(
            GlossaryTermDomainRelMapper.class, mapper -> mapper.listDomainIds(termId)));
  }

  /** Returns the live member terms of the given domains. */
  public Set<Long> termIds(long metalakeId, Set<Long> domainIds) {
    if (domainIds.isEmpty()) {
      return new HashSet<>();
    }
    return new HashSet<>(
        SessionUtils.getWithoutCommit(
            GlossaryTermDomainRelMapper.class,
            mapper -> mapper.listTermIds(metalakeId, domainIds)));
  }

  /** Returns the live direct sub-domains of a domain, in ascending name order. */
  public List<DomainPO> subdomains(long metalakeId, long domainId) {
    List<Long> ids =
        SessionUtils.getWithoutCommit(
            GlossaryTermDomainRelMapper.class,
            mapper -> mapper.listSubdomainIds(metalakeId, domainId));
    List<DomainPO> result = new ArrayList<>();
    SessionUtils.doWithoutCommit(
        DomainMetaMapper.class,
        mapper -> {
          for (Long id : ids) {
            DomainPO domain = mapper.selectDomainMetaById(id);
            if (domain != null) {
              result.add(domain);
            }
          }
        });
    return result;
  }

  /** Whether a domain still has live member terms. A plain read at read committed. */
  public boolean hasTerms(long domainId) {
    return SessionUtils.getWithoutCommit(
            GlossaryTermDomainRelMapper.class, mapper -> mapper.countTerms(domainId))
        > 0;
  }

  /** Adds a live membership row. The caller holds the term and domain locks. */
  public void add(long metalakeId, long termId, long domainId) {
    SessionUtils.doWithoutCommit(
        GlossaryTermDomainRelMapper.class,
        mapper ->
            Preconditions.checkState(
                mapper.insert(metalakeId, termId, domainId) == 1,
                "Failed to add term %s to domain %s",
                termId,
                domainId));
  }

  /** Retires a live membership row if present. The caller holds the term row lock. */
  public void remove(long termId, long domainId) {
    SessionUtils.doWithoutCommit(
        GlossaryTermDomainRelMapper.class,
        mapper -> mapper.remove(termId, domainId, nextDeletedAt(mapper.lastDeletedAt(termId))));
  }

  /** Retires every membership row of a term inside its deletion transaction. */
  public void removeTerm(long termId) {
    SessionUtils.doWithoutCommit(
        GlossaryTermDomainRelMapper.class,
        mapper -> mapper.removeTerm(termId, nextDeletedAt(mapper.lastDeletedAt(termId))));
  }

  /** Retires every membership row of a metalake inside its deletion transaction. */
  public void dropMetalake(long metalakeId) {
    SessionUtils.doWithoutCommit(
        GlossaryTermDomainRelMapper.class,
        mapper ->
            mapper.dropMetalake(
                metalakeId, nextDeletedAt(mapper.lastDeletedAtInMetalake(metalakeId))));
  }

  /** Purges at most limit retired rows older than cutoff, in the caller's transaction. */
  public int collect(long cutoff, int limit) {
    Preconditions.checkArgument(limit >= 0, "limit must be nonnegative");
    if (limit == 0) {
      return 0;
    }
    return SessionUtils.getWithoutCommit(
        GlossaryTermDomainRelMapper.class,
        mapper -> {
          int count = 0;
          for (GlossaryTermDomainRelPO row : mapper.expired(cutoff, limit)) {
            count += mapper.purge(row);
          }
          return count;
        });
  }

  // A repeated same-millisecond change must not collide with a retained generation of the row.
  private static long nextDeletedAt(long lastDeletedAt) {
    return Math.max(Math.max(1L, System.currentTimeMillis()), Math.addExact(lastDeletedAt, 1L));
  }
}
