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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.LongPredicate;
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
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;

/** Metalake capabilities for one glossary operation. Term ownership follows the observed ID. */
public final class GlossaryAccess {
  private static final GlossaryAccess UNRESTRICTED =
      new GlossaryAccess(true, () -> true, () -> true, id -> true, (name, domain) -> true);
  private static final GlossaryAccess DENIED =
      new GlossaryAccess(false, () -> false, () -> false, id -> false, (name, domain) -> false);

  private final boolean view;
  private final Supplier<Boolean> create;
  private final Supplier<Boolean> manage;
  private final LongPredicate owns;
  private final BiPredicate<String, DomainEntity> domains;
  private final Map<String, Boolean> visibleDomains = new HashMap<>();

  private GlossaryAccess(
      boolean view,
      Supplier<Boolean> create,
      Supplier<Boolean> manage,
      LongPredicate owns,
      BiPredicate<String, DomainEntity> domains) {
    this.view = view;
    // A list or hierarchy walk can visit many drafts. Evaluate each required privilege once.
    this.create = Suppliers.memoize(create);
    this.manage = Suppliers.memoize(manage);
    this.owns = owns;
    this.domains = domains;
  }

  boolean canView() {
    return view;
  }

  boolean manage() {
    return manage.get();
  }

  /** Returns the current principal's Glossary access for a single operation. */
  public static GlossaryAccess forRequest(String metalake, boolean enabled) {
    if (!enabled) {
      return UNRESTRICTED;
    }
    GravitinoAuthorizer authorizer = GravitinoEnv.getInstance().gravitinoAuthorizer();
    Principal principal = PrincipalUtils.getCurrentPrincipal();
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    return forPrincipal(authorizer, principal, metalake, context);
  }

  /** Checks the same draft visibility used by glossary reads for generic metadata endpoints. */
  public static boolean canRead(
      GravitinoAuthorizer authorizer,
      Principal principal,
      String metalake,
      MetadataObject object,
      AuthorizationRequestContext context) {
    if (object == null || object.type() != MetadataObject.Type.GLOSSARY_TERM) {
      return false;
    }
    GlossaryAccess access = forPrincipal(authorizer, principal, metalake, context);
    if (!access.canView()) {
      return false;
    }
    GlossaryTermEntity term;
    try {
      term =
          GlossaryTermMetaService.getInstance()
              .get(NameIdentifierUtil.ofGlossaryTerm(metalake, object.name()));
    } catch (NoSuchEntityException missing) {
      return false;
    }
    return access.canRead(term);
  }

  private static GlossaryAccess forPrincipal(
      GravitinoAuthorizer authorizer,
      Principal principal,
      String metalake,
      AuthorizationRequestContext context) {
    MetadataObject lake = MetadataObjects.of(null, metalake, MetadataObject.Type.METALAKE);
    if (authorizer.isOwner(principal, metalake, lake, context)) {
      return UNRESTRICTED;
    }
    if (!effective(authorizer, principal, metalake, lake, Name.VIEW_DEFINITION, context)) {
      return DENIED;
    }
    return new GlossaryAccess(
        true,
        () -> effective(authorizer, principal, metalake, lake, Name.CREATE_DEFINITION, context),
        () -> effective(authorizer, principal, metalake, lake, Name.MANAGE_DEFINITIONS, context),
        id -> authorizer.isGlossaryTermOwner(principal, metalake, id, context),
        (name, snapshot) -> {
          DomainEntity domain = snapshot;
          if (domain == null) {
            try {
              domain =
                  DomainMetaService.getInstance()
                      .getDomainByIdentifier(NameIdentifierUtil.ofDomain(metalake, name));
            } catch (NoSuchEntityException absent) {
              return false;
            }
          }
          return DomainAccess.allowed(
              authorizer,
              principal,
              metalake,
              DomainAccess.scopes(metalake, domain),
              DomainAccess.readPrivilege(domain),
              context);
        });
  }

  private static boolean effective(
      GravitinoAuthorizer authorizer,
      Principal principal,
      String metalake,
      MetadataObject lake,
      Name privilege,
      AuthorizationRequestContext context) {
    return authorizer.authorize(principal, metalake, lake, privilege, context)
        && !authorizer.deny(principal, metalake, lake, privilege, context);
  }

  boolean canRead(GlossaryTermEntity term) {
    return canRead(term.id(), term.status());
  }

  /** Checks visibility of the supplied live identity and status in the caller's snapshot. */
  public boolean canRead(long id, GlossaryTerm.Status status) {
    return view && (status != GlossaryTerm.Status.DRAFT || canEdit(id));
  }

  boolean canEdit(GlossaryTermEntity term) {
    // Listing drafts as a creator requires a current owner lookup per draft to observe transfers.
    return canEdit(term.id());
  }

  private boolean canEdit(long id) {
    return manage() || (create.get() && owns.test(id));
  }

  /**
   * Whether the caller can see a domain through the ordinary domain-read check: USE_DOMAIN on a
   * published domain, MANAGE_DOMAIN on a draft. A missing domain is not visible. Each name is
   * checked once per request.
   */
  boolean canUseDomain(String domain) {
    return canUseDomain(domain, null);
  }

  /** Uses the already loaded domain and parent in a subdomain listing. */
  boolean canUseDomain(DomainEntity domain) {
    return canUseDomain(domain.name(), domain);
  }

  private boolean canUseDomain(String name, @Nullable DomainEntity domain) {
    if (this == UNRESTRICTED || this == DENIED) {
      return this == UNRESTRICTED;
    }
    return visibleDomains.computeIfAbsent(name, key -> domains.test(key, domain));
  }

  void requireCreate() {
    if (!create.get()) {
      throw new ForbiddenException(
          "Creating a glossary term requires effective VIEW_DEFINITION and CREATE_DEFINITION");
    }
  }
}
