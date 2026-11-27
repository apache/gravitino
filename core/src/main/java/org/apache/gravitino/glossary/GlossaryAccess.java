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

package org.apache.gravitino.glossary;

import com.google.common.collect.Lists;
import java.security.Principal;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.DomainAccess;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privilege.Name;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.meta.GlossaryTermEntity;
import org.apache.gravitino.storage.relational.service.DomainMetaService;
import org.apache.gravitino.storage.relational.service.GlossaryTermMetaService;
import org.apache.gravitino.storage.relational.service.MetalakeMetaService;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;

/** Effective definition privileges across the metalake and the current term's member domains. */
public final class GlossaryAccess {
  private enum Decision {
    ALLOW,
    DENY,
    UNSET
  }

  private final GravitinoAuthorizer authorizer;
  private final Principal principal;
  private final String metalake;
  private final MetadataObject lake;
  private final AuthorizationRequestContext context;
  private final boolean unrestricted;
  private final Map<MetadataObject, Map<Name, Decision>> decisions = new HashMap<>();
  private final Map<String, DomainEntity> domains = new HashMap<>();
  private final Map<String, Boolean> visibleDomains = new HashMap<>();
  private final Map<Long, GlossaryTermEntity> publishedTerms = new HashMap<>();
  private final Map<Long, Boolean> ownedTerms = new HashMap<>();

  private GlossaryAccess(
      @Nullable GravitinoAuthorizer authorizer,
      Principal principal,
      String metalake,
      AuthorizationRequestContext context) {
    this.authorizer = authorizer;
    this.principal = principal;
    this.metalake = metalake;
    this.context = context;
    lake = MetadataObjects.of(null, metalake, MetadataObject.Type.METALAKE);
    unrestricted = authorizer == null || authorizer.isOwner(principal, metalake, lake, context);
  }

  static GlossaryAccess forRequest(String metalake, boolean enabled) {
    return new GlossaryAccess(
        enabled ? Objects.requireNonNull(GravitinoEnv.getInstance().gravitinoAuthorizer()) : null,
        PrincipalUtils.getCurrentPrincipal(),
        metalake,
        new AuthorizationRequestContext());
  }

  /** Checks the same term visibility used by glossary reads for generic metadata endpoints. */
  public static boolean canRead(
      GravitinoAuthorizer authorizer,
      Principal principal,
      String metalake,
      MetadataObject object,
      AuthorizationRequestContext context) {
    if (object == null || object.type() != MetadataObject.Type.GLOSSARY_TERM) {
      return false;
    }
    Supplier<Boolean> read =
        () ->
            new GlossaryAccess(authorizer, principal, metalake, context)
                .canReadNamedTerm(object.name());
    return SqlSessions.hasActiveSession() ? read.get() : SqlSessions.readSnapshot(read);
  }

  boolean lakeViewDenied() {
    return !unrestricted && decision(lake, Name.VIEW_DEFINITION) == Decision.DENY;
  }

  boolean needsDomainScopes() {
    return !unrestricted;
  }

  GlossaryTermEntity publishedTerm(long id, Supplier<GlossaryTermEntity> load) {
    return publishedTerms.computeIfAbsent(id, ignored -> load.get());
  }

  private boolean canReadNamedTerm(String name) {
    if (lakeViewDenied()) {
      return false;
    }
    GlossaryTermEntity term;
    try {
      long lakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(metalake);
      term =
          GlossaryTermMetaService.getInstance()
              .get(NameIdentifierUtil.ofGlossaryTerm(metalake, name), lakeId);
    } catch (NoSuchEntityException missing) {
      return false;
    }
    return canRead(term);
  }

  private Decision decision(MetadataObject scope, Name privilege) {
    return decisions
        .computeIfAbsent(scope, ignored -> new EnumMap<>(Name.class))
        .computeIfAbsent(
            privilege,
            key -> {
              if (authorizer.deny(principal, metalake, scope, key, context)) return Decision.DENY;
              return authorizer.authorize(principal, metalake, scope, key, context)
                  ? Decision.ALLOW
                  : Decision.UNSET;
            });
  }

  void preloadDomains(Collection<String> names) {
    if (unrestricted) return;
    List<String> missing =
        names.stream().distinct().filter(name -> !domains.containsKey(name)).toList();
    for (List<String> batch : Lists.partition(missing, 500)) {
      for (DomainEntity domain :
          DomainMetaService.getInstance()
              .batchGetDomainByIdentifier(
                  batch.stream()
                      .map(name -> NameIdentifierUtil.ofDomain(metalake, name))
                      .toList())) {
        rememberDomain(domain);
      }
    }
  }

  private DomainEntity rememberDomain(DomainEntity domain) {
    domains.put(domain.name(), domain);
    context.rememberTargetId(
        metalake, MetadataObjects.of(null, domain.name(), MetadataObject.Type.DOMAIN), domain.id());
    return domain;
  }

  @Nullable
  private DomainEntity domain(String name) {
    DomainEntity known = domains.get(name);
    if (known != null) return known;
    try {
      return rememberDomain(
          DomainMetaService.getInstance()
              .getDomainByIdentifier(NameIdentifierUtil.ofDomain(metalake, name)));
    } catch (NoSuchEntityException missing) {
      return null;
    }
  }

  private boolean effective(Name privilege, Collection<String> names) {
    if (unrestricted) return true;
    Decision global = decision(lake, privilege);
    if (global == Decision.DENY) return false;
    boolean allowed = global == Decision.ALLOW;
    for (String name : names) {
      DomainEntity domain = domain(name);
      if (domain == null) return false;
      for (MetadataObject scope : DomainAccess.scopes(metalake, domain)) {
        Decision local = decision(scope, privilege);
        if (local == Decision.DENY) return false;
        allowed |= local == Decision.ALLOW;
      }
    }
    return allowed;
  }

  boolean canRead(GlossaryTermEntity term) {
    return canRead(term.id(), term.status(), List.of(term.domains()));
  }

  boolean canRead(long id, GlossaryTerm.Status status, Collection<String> names) {
    return effective(Name.VIEW_DEFINITION, names)
        && (status != GlossaryTerm.Status.DRAFT || editable(id, names));
  }

  boolean canEdit(GlossaryTermEntity term) {
    List<String> names = List.of(term.domains());
    return effective(Name.VIEW_DEFINITION, names) && editable(term.id(), names);
  }

  private boolean editable(long id, Collection<String> names) {
    return effective(Name.MANAGE_DEFINITIONS, names)
        || (effective(Name.CREATE_DEFINITION, names)
            && ownedTerms.computeIfAbsent(
                id,
                termId -> authorizer.isGlossaryTermOwner(principal, metalake, termId, context)));
  }

  boolean canManage(GlossaryTermEntity term) {
    List<String> names = List.of(term.domains());
    return effective(Name.VIEW_DEFINITION, names) && effective(Name.MANAGE_DEFINITIONS, names);
  }

  boolean canCreate(Collection<String> names) {
    return effective(Name.VIEW_DEFINITION, names) && effective(Name.CREATE_DEFINITION, names);
  }

  void requireCreate(Collection<String> names) {
    if (!canCreate(names))
      throw new ForbiddenException(
          "Creating or assigning a glossary term requires effective VIEW_DEFINITION and CREATE_DEFINITION");
  }

  boolean canUseDomain(String name) {
    if (unrestricted) return true;
    DomainEntity domain = domain(name);
    return domain != null && canUseDomain(domain);
  }

  boolean canUseDomain(DomainEntity domain) {
    if (unrestricted) return true;
    rememberDomain(domain);
    return visibleDomains.computeIfAbsent(
        domain.name(),
        ignored ->
            DomainAccess.allowed(
                authorizer,
                principal,
                metalake,
                DomainAccess.scopes(metalake, domain),
                DomainAccess.readPrivilege(domain),
                context));
  }
}
