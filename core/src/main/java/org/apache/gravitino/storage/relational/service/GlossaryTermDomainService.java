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

import static org.apache.gravitino.storage.relational.mapper.DomainMemberMapper.MemberType.GLOSSARY_TERM;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.gravitino.exceptions.NoSuchDomainException;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.storage.relational.mapper.DomainMemberMapper;
import org.apache.gravitino.storage.relational.mapper.DomainMetaMapper;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermDomainRelMapper;
import org.apache.gravitino.storage.relational.po.DomainPO;
import org.apache.gravitino.storage.relational.po.GlossaryTermDomainRelPO;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * Explicit glossary term domain membership. Only the glossary term write path changes it. Every
 * method joins the caller's session. Writers hold the term row lock, and lock domain rows only
 * after every term row of the request, with {@link #lockDomains}.
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
   * Locks existing member domains and domains named by an edit in ascending ID order. Existing
   * memberships follow a rename. A request naming a domain that disappears or changes name before
   * its row is locked must fail, even if another domain has taken that name. After locking, missing
   * names and authorization failures are reported in the same name order.
   *
   * @param metalakeId The live metalake ID.
   * @param members The current member domain IDs of the locked term.
   * @param requestedNames The names supplied by the caller.
   * @param checkRequested The authorization check for each requested live domain.
   * @return The locked domain rows by ID.
   */
  public Map<Long, DomainPO> lockDomains(
      long metalakeId,
      Set<Long> members,
      Set<String> requestedNames,
      Consumer<DomainPO> checkRequested) {
    return SessionUtils.getWithoutCommit(
        DomainMetaMapper.class,
        mapper -> {
          Map<String, Long> observedIds = new HashMap<>();
          Set<Long> ids = new TreeSet<>(members);
          for (String name : requestedNames) {
            DomainPO row = mapper.selectDomainMetaByMetalakeIdAndName(metalakeId, name);
            if (row != null && name.equals(row.domainName())) {
              observedIds.put(name, row.domainId());
              ids.add(row.domainId());
            }
          }
          Map<Long, DomainPO> locked = new LinkedHashMap<>();
          for (long id : ids) {
            DomainPO row = mapper.selectDomainMetaByIdForShare(id);
            if (row != null && row.deletedAt() == 0 && row.metalakeId() == metalakeId) {
              locked.put(id, row);
            } else {
              Preconditions.checkState(
                  !members.contains(id), "A glossary membership refers to a missing domain");
            }
          }
          for (String name : new TreeSet<>(requestedNames)) {
            DomainPO row = locked.get(observedIds.get(name));
            if (row == null || !name.equals(row.domainName())) {
              throw new NoSuchDomainException("Domain %s does not exist", name);
            }
            checkRequested.accept(row);
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

  /** Returns the live domain IDs of a term. */
  public Set<Long> domainIds(long metalakeId, long termId) {
    return new HashSet<>(
        SessionUtils.getWithoutCommit(
            DomainMemberMapper.class, mapper -> mapper.domains(metalakeId, GLOSSARY_TERM, termId)));
  }

  /** Returns the live member term IDs in ascending order for stable batched definition reads. */
  public Set<Long> termIds(long metalakeId, Set<Long> domainIds) {
    Set<Long> result = new TreeSet<>();
    if (!domainIds.isEmpty()) {
      SessionUtils.doWithoutCommit(
          GlossaryTermDomainRelMapper.class,
          mapper -> {
            for (List<Long> batch : Lists.partition(List.copyOf(domainIds), BATCH_SIZE)) {
              result.addAll(mapper.listTermIds(metalakeId, batch));
            }
          });
    }
    return result;
  }

  /** Adds a live membership row. The caller holds the term and domain locks. */
  public void add(long metalakeId, long termId, long domainId) {
    AuditInfo audit =
        AuditInfo.builder()
            .withCreator(PrincipalUtils.getCurrentUserName())
            .withCreateTime(Instant.now())
            .build();
    String auditJson;
    try {
      auditJson = JsonUtils.anyFieldMapper().writeValueAsString(audit);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException("Cannot serialize Domain membership audit information", e);
    }
    SessionUtils.doWithoutCommit(
        GlossaryTermDomainRelMapper.class,
        mapper ->
            Preconditions.checkState(
                mapper.insert(metalakeId, termId, domainId, auditJson) == 1,
                "Failed to add term %s to domain %s",
                termId,
                domainId));
  }

  /** Retires a live membership row if present. The caller holds the term row lock. */
  public void remove(long metalakeId, long termId, long domainId) {
    SessionUtils.doWithoutCommit(
        DomainMemberMapper.class,
        mapper ->
            mapper.retire(
                metalakeId,
                GLOSSARY_TERM,
                termId,
                domainId,
                nextDeletedAt(mapper.lastRetirement(metalakeId, GLOSSARY_TERM, termId))));
  }

  /** Retires every membership row of a term inside its deletion transaction. */
  public void removeTerm(long metalakeId, long termId) {
    SessionUtils.doWithoutCommit(
        DomainMemberMapper.class,
        mapper ->
            mapper.retireMember(
                metalakeId,
                GLOSSARY_TERM,
                termId,
                nextDeletedAt(mapper.lastRetirement(metalakeId, GLOSSARY_TERM, termId))));
  }

  /** Retires every membership row of a metalake inside its deletion transaction. */
  public void dropMetalake(long metalakeId) {
    SessionUtils.doWithoutCommit(
        DomainMemberMapper.class,
        mapper ->
            mapper.retireMetalake(
                metalakeId, nextDeletedAt(mapper.lastMetalakeRetirement(metalakeId))));
  }

  // A repeated same-millisecond change must not collide with a retained generation of the row.
  private static long nextDeletedAt(long lastDeletedAt) {
    return Math.max(Math.max(1L, System.currentTimeMillis()), Math.addExact(lastDeletedAt, 1L));
  }
}
