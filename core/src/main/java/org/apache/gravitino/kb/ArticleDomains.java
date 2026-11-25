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

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Nullable;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.DomainAccess;
import org.apache.gravitino.authorization.Privilege.Name;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchDomainException;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.storage.relational.mapper.DomainMetaMapper;
import org.apache.gravitino.storage.relational.po.DomainPO;
import org.apache.gravitino.storage.relational.service.DomainRelService;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.PrincipalUtils;

/** Resolves Article membership against the current Domain identity, parent and privileges. */
final class ArticleDomains {
  private final String metalake;
  private final boolean authorization;
  // Shared by Articles in one response snapshot only.
  private final Map<Long, Optional<String>> visibleNames = new HashMap<>();

  ArticleDomains(String metalake, boolean authorization) {
    this.metalake = metalake;
    this.authorization = authorization;
  }

  record Replacement(List<Set<Long>> steps, Map<Long, String> named) {}

  Replacement resolve(List<List<String>> replacements) {
    BiMap<String, Long> ids = HashBiMap.create();
    List<Set<Long>> steps = new ArrayList<>();
    for (List<String> replacement : replacements) {
      Set<Long> step = new HashSet<>();
      for (String name : replacement) {
        step.add(ids.computeIfAbsent(name, key -> find(key).domainId()));
      }
      steps.add(step);
    }
    return new Replacement(steps, ids.inverse());
  }

  Set<Long> check(Replacement plan, Set<Long> current, boolean lock) {
    Set<Long> targets = new TreeSet<>(current);
    targets.addAll(plan.named().keySet());
    Set<Long> result = new HashSet<>();
    for (long id : targets) {
      String name = plan.named().get(id);
      DomainEntity domain = currentDomain(id, name, lock);
      boolean member = current.contains(id);
      // An ordinary replacement edits the visible set. Hidden assignments require explicit detach.
      if (member && name == null && !readable(domain)) {
        result.add(id);
        continue;
      }
      boolean changed = false;
      for (Set<Long> step : plan.steps()) {
        boolean next = step.contains(id);
        changed |= member != next;
        member = next;
      }
      if (changed) {
        requireManage(domain, name);
      } else if (name != null) {
        requireReadable(domain);
      }
      if (member) {
        result.add(id);
      }
    }
    return result;
  }

  long manage(String name) {
    DomainPO row = find(name);
    requireManage(entity(row), name);
    return row.domainId();
  }

  void lockManaged(long lake, long id, String name) {
    DomainPO locked = lockDomain(id);
    if (locked == null || locked.metalakeId() != lake) {
      throw new NoSuchDomainException("Domain %s does not exist", name);
    }
    requireManage(currentDomain(id, name, false), name);
  }

  Set<Long> scope(String name) {
    DomainEntity root = entity(find(name));
    requireReadable(root);
    Set<Long> ids = new HashSet<>();
    ids.add(root.id());
    for (DomainEntity child :
        DomainRelService.getInstance()
            .listSubDomains(NameIdentifierUtil.ofDomain(metalake, name))) {
      if (readable(child)) {
        ids.add(child.id());
      }
    }
    return ids;
  }

  KnowledgeBaseArticleEntity visible(KnowledgeBaseArticleEntity article) {
    if (article.domainIds().isEmpty()) {
      return article;
    }
    Map<Long, String> visible = visibleNames(article.domainIds());
    return article.toBuilder()
        .withDomains(visible.values().stream().sorted().toList())
        .withDomainIds(visible.keySet())
        .build();
  }

  Map<Long, String> visibleNames(Iterable<Long> domainIds) {
    Map<Long, String> result = new HashMap<>();
    for (long id : domainIds) {
      Optional<String> visible = visibleNames.computeIfAbsent(id, this::visibleName);
      if (visible.isPresent()) {
        result.put(id, visible.get());
      }
    }
    return result;
  }

  private Optional<String> visibleName(long id) {
    DomainPO row = byId(id);
    return row != null && readable(entity(row)) ? Optional.of(row.domainName()) : Optional.empty();
  }

  private DomainEntity currentDomain(long id, @Nullable String name, boolean lock) {
    if (lock && lockDomain(id) == null) {
      throw new NoSuchDomainException("Requested Domain does not exist");
    }
    // The locking select only reads the Domain row. Read its current parent after taking the lock.
    DomainPO row = byId(id);
    if (row == null || (name != null && !name.equals(row.domainName()))) {
      throw new NoSuchDomainException("Requested Domain does not exist");
    }
    return entity(row);
  }

  private DomainPO find(String name) {
    DomainPO row =
        SessionUtils.getWithoutCommit(
            DomainMetaMapper.class,
            mapper -> mapper.selectDomainMetaByMetalakeAndName(metalake, name));
    if (row == null || !name.equals(row.domainName())) {
      throw new NoSuchDomainException("Domain %s does not exist", name);
    }
    return row;
  }

  @Nullable
  private DomainPO lockDomain(long id) {
    return SessionUtils.getWithoutCommit(
        DomainMetaMapper.class, mapper -> mapper.selectDomainMetaByIdForShare(id));
  }

  @Nullable
  private DomainPO byId(long id) {
    return SessionUtils.getWithoutCommit(
        DomainMetaMapper.class, mapper -> mapper.selectDomainMetaById(id));
  }

  private DomainEntity entity(DomainPO row) {
    return DomainPO.fromDomainPO(row, NamespaceUtil.ofDomain(metalake));
  }

  private void requireManage(DomainEntity domain, @Nullable String named) {
    if (!allowed(domain, Name.MANAGE_DOMAIN)) {
      if (named != null && domain.status() == Domain.Status.DRAFT) {
        throw new NoSuchDomainException("Domain %s does not exist", named);
      }
      // A removed membership may have been hidden from the caller.
      throw new ForbiddenException("Changing Article membership requires MANAGE_DOMAIN");
    }
  }

  private void requireReadable(DomainEntity domain) {
    if (!readable(domain)) {
      if (domain.status() == Domain.Status.DRAFT) {
        throw new NoSuchDomainException("Domain %s does not exist", domain.name());
      }
      throw new ForbiddenException("Cannot use Domain %s", domain.name());
    }
  }

  private boolean readable(DomainEntity domain) {
    return allowed(domain, DomainAccess.readPrivilege(domain));
  }

  private boolean allowed(DomainEntity domain, Name privilege) {
    return !authorization
        || DomainAccess.allowed(
            GravitinoEnv.getInstance().gravitinoAuthorizer(),
            PrincipalUtils.getCurrentPrincipal(),
            metalake,
            DomainAccess.scopes(metalake, domain),
            privilege,
            new AuthorizationRequestContext());
  }
}
