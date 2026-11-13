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
package org.apache.gravitino.server.authorization;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.SupportsDomainOperations.LockedDomainState;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.domain.DomainDispatcher;
import org.apache.gravitino.domain.DomainMember;
import org.apache.gravitino.domain.DomainMemberFilter;
import org.apache.gravitino.domain.DomainMemberInfo;
import org.apache.gravitino.domain.DomainRelation;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchDomainException;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.utils.PrincipalUtils;

/** Server authorization, draft visibility and result filtering for domain REST operations. */
public final class DomainAuthzHelper {

  private DomainAuthzHelper() {}

  /**
   * Resolves the current domain parent without caching its mutable relationship.
   *
   * @param metalake The metalake name.
   * @param object The object whose parent is needed.
   * @return The current parent, or null for a root object.
   */
  @Nullable
  public static MetadataObject parent(String metalake, MetadataObject object) {
    if (object.type() != MetadataObject.Type.DOMAIN) {
      return MetadataObjects.parent(object);
    }
    try {
      return GravitinoEnv.getInstance()
          .internalDomainDispatcher()
          .getDomain(metalake, object.fullName())
          .parent()
          .map(name -> MetadataObjects.of(null, name, MetadataObject.Type.DOMAIN))
          .orElse(null);
    } catch (NoSuchDomainException e) {
      return null;
    }
  }

  /**
   * Builds the complete authorization scope from a domain snapshot with its current parent.
   *
   * @param metalake The metalake name.
   * @param domain The domain snapshot, or null for a metalake-only check.
   * @return The metalake, parent and domain scopes.
   */
  public static List<MetadataObject> scopes(String metalake, @Nullable DomainEntity domain) {
    List<MetadataObject> scopes = new ArrayList<>();
    scopes.add(MetadataObjects.of(null, metalake, MetadataObject.Type.METALAKE));
    if (domain != null) {
      domain
          .parent()
          .ifPresent(
              name -> scopes.add(MetadataObjects.of(null, name, MetadataObject.Type.DOMAIN)));
      scopes.add(MetadataObjects.of(null, domain.name(), MetadataObject.Type.DOMAIN));
    }
    return scopes;
  }

  /**
   * Evaluates ownership and allow/deny policies over the whole scope. Management implies usage; a
   * usage deny still overrides that implied usage. Owners bypass privilege denies.
   *
   * @param authorizer The authorizer.
   * @param principal The caller.
   * @param metalake The metalake name.
   * @param scopes The current authorization scopes.
   * @param privilege The requested privilege.
   * @param context The request context.
   * @return Whether the caller has the requested privilege.
   */
  public static boolean allowed(
      GravitinoAuthorizer authorizer,
      Principal principal,
      String metalake,
      List<MetadataObject> scopes,
      Privilege.Name privilege,
      AuthorizationRequestContext context) {
    if (scopes.stream()
        .anyMatch(scope -> authorizer.isOwner(principal, metalake, scope, context))) {
      return true;
    }
    if (scopes.stream()
        .anyMatch(scope -> authorizer.deny(principal, metalake, scope, privilege, context))) {
      return false;
    }
    boolean granted =
        scopes.stream()
            .anyMatch(
                scope -> authorizer.authorize(principal, metalake, scope, privilege, context));
    return granted
        || (privilege == Privilege.Name.USE_DOMAIN
            && allowed(
                authorizer, principal, metalake, scopes, Privilege.Name.MANAGE_DOMAIN, context));
  }

  /**
   * @return Whether server authorization is enabled.
   */
  public static boolean enabled() {
    return GravitinoEnv.getInstance().config() != null
        && GravitinoEnv.getInstance().config().get(Configs.ENABLE_AUTHORIZATION);
  }

  /**
   * Checks the current caller's privilege.
   *
   * @param metalake The metalake name.
   * @param domain The current domain, or null for creation on the metalake.
   * @param privilege The privilege to check.
   * @return Whether access is permitted.
   */
  public static boolean allowed(
      String metalake, @Nullable DomainEntity domain, Privilege.Name privilege) {
    return allowed(metalake, domain, privilege, new AuthorizationRequestContext());
  }

  /**
   * Applies ordinary metadata visibility without requiring any tag privilege.
   *
   * @param metalake The metalake name.
   * @param object The member object.
   * @return Whether the object is accessible.
   */
  public static boolean canAccessObject(String metalake, MetadataObject object) {
    if (!enabled()) {
      return true;
    }
    return accessibleObjects(metalake, new MetadataObject[] {object}).length > 0;
  }

  /**
   * Checks visibility without disclosing hidden drafts.
   *
   * @param metalake The metalake name.
   * @param domain The domain snapshot.
   * @return Whether the caller can see the domain.
   */
  public static boolean visible(String metalake, DomainEntity domain) {
    return visible(metalake, domain, new AuthorizationRequestContext());
  }

  /**
   * Requires domain visibility, returning 404 for a hidden draft and 403 for a published domain.
   *
   * @param metalake The metalake name.
   * @param domain The domain snapshot.
   */
  public static void requireVisible(String metalake, DomainEntity domain) {
    requireVisible(metalake, domain, new AuthorizationRequestContext());
  }

  /**
   * Requires management, while preserving hidden draft semantics.
   *
   * @param metalake The metalake name.
   * @param domain The current domain, or null for creation.
   */
  public static void requireManage(String metalake, @Nullable DomainEntity domain) {
    if (!allowed(metalake, domain, Privilege.Name.MANAGE_DOMAIN)) {
      if (domain != null && domain.status() == Domain.Status.DRAFT) {
        throw new NoSuchDomainException(
            "Domain %s under metalake %s does not exist", domain.name(), metalake);
      }
      throw new ForbiddenException("Cannot manage domains in metalake %s", metalake);
    }
  }

  /**
   * Checks management before storage resolves other objects named in a request.
   *
   * @param dispatcher The domain dispatcher.
   * @param metalake The metalake name.
   * @param name The requested domain name.
   */
  public static void requireManage(DomainDispatcher dispatcher, String metalake, String name) {
    if (enabled()) {
      requireManage(metalake, dispatcher.getDomain(metalake, name));
    }
  }

  /**
   * Requires ownership of a backing tag, or permission to create a new one.
   *
   * @param metalake The metalake name.
   * @param name The tag name.
   * @param create Whether a new tag will be created.
   */
  public static void requireBackingTag(String metalake, String name, boolean create) {
    if (!enabled()) {
      return;
    }
    GravitinoAuthorizer authorizer = GravitinoEnv.getInstance().gravitinoAuthorizer();
    Principal principal = PrincipalUtils.getCurrentPrincipal();
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    List<MetadataObject> scopes = scopes(metalake, null);
    boolean allowed =
        authorizer != null
            && (create
                ? allowed(
                    authorizer, principal, metalake, scopes, Privilege.Name.CREATE_TAG, context)
                : authorizer.isOwner(principal, metalake, scopes.get(0), context)
                    || authorizer.isOwner(
                        principal,
                        metalake,
                        MetadataObjects.of(null, name, MetadataObject.Type.TAG),
                        context));
    if (!allowed) {
      throw new ForbiddenException("Cannot bind backing tag %s", name);
    }
  }

  /**
   * Installs a server check to run against the state locked by a domain mutation.
   *
   * @param dispatcher The domain dispatcher.
   * @param check The server-supplied check.
   * @return A scoped dispatcher, or the original when authorization is disabled.
   */
  public static DomainDispatcher checked(
      DomainDispatcher dispatcher, Consumer<LockedDomainState> check) {
    return enabled() ? dispatcher.withMutationCheck(check) : dispatcher;
  }

  /**
   * Requires management against each mutation's current, locked domain.
   *
   * @param dispatcher The dispatcher.
   * @param metalake The metalake name.
   * @return A dispatcher with the server's management check.
   */
  public static DomainDispatcher managed(DomainDispatcher dispatcher, String metalake) {
    return checked(dispatcher, state -> requireManage(metalake, state.domain()));
  }

  /**
   * Removes domains the caller cannot see from a list.
   *
   * @param metalake The metalake name.
   * @param domains The unfiltered domain snapshots.
   * @return The visible domains.
   */
  public static DomainEntity[] filterDomains(String metalake, DomainEntity[] domains) {
    return filterDomains(metalake, domains, new AuthorizationRequestContext());
  }

  /**
   * Checks a requested domain before browsing its scope.
   *
   * @param dispatcher The dispatcher.
   * @param metalake The metalake name.
   * @param name The requested domain name.
   */
  public static void requireVisible(DomainDispatcher dispatcher, String metalake, String name) {
    if (enabled()) {
      requireVisible(metalake, dispatcher.getDomain(metalake, name));
    }
  }

  /**
   * Lists members with ordinary object access and only visible membership paths.
   *
   * @param dispatcher The domain dispatcher.
   * @param metalake The metalake name.
   * @param name The requested domain.
   * @param filter The member filter.
   * @return Accessible members with visible relations.
   */
  public static DomainMember[] listMembers(
      DomainDispatcher dispatcher, String metalake, String name, DomainMemberFilter filter) {
    if (!enabled()) {
      return dispatcher.listMembers(metalake, name, filter);
    }
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    DomainDispatcher internal = GravitinoEnv.getInstance().internalDomainDispatcher();
    DomainEntity domain = internal.getDomain(metalake, name);
    requireVisible(metalake, domain, context);
    Set<String> visibleNames = new HashSet<>();
    visibleNames.add(domain.name());
    DomainEntity[] subdomains =
        domain.parent().isPresent()
            ? new DomainEntity[0]
            : filterDomains(metalake, internal.listSubdomainInfos(metalake, name), context);
    Arrays.stream(subdomains).map(DomainEntity::name).forEach(visibleNames::add);
    DomainMemberFilter effectiveFilter = filter;
    if (!filter.domains().isEmpty()) {
      Set<String> selected = new HashSet<>(filter.domains());
      selected.retainAll(visibleNames);
      // Preserve an unmatched nonempty filter so the dispatcher still emits query events.
      // Replacing it with an empty filter would incorrectly select the whole scope.
      if (!selected.isEmpty()) {
        effectiveFilter =
            DomainMemberFilter.builder()
                .withDomains(selected.toArray(new String[0]))
                .withTypes(filter.types().toArray(new MetadataObject.Type[0]))
                .withNameContains(filter.nameContains().orElse(null))
                .withExpand(filter.expand())
                .build();
      }
    }
    DomainMember[] members = dispatcher.listMembers(metalake, domain, subdomains, effectiveFilter);
    Set<MetadataObject> accessible =
        new HashSet<>(
            Arrays.asList(
                accessibleObjects(
                    metalake,
                    Arrays.stream(members)
                        .map(DomainMember::object)
                        .toArray(MetadataObject[]::new))));
    return Arrays.stream(members)
        .filter(member -> accessible.contains(member.object()))
        .map(
            member ->
                new DomainMemberInfo(
                    member.object(), filterRelations(member.relations(), visibleNames)))
        .filter(member -> member.relations().length > 0)
        .toArray(DomainMember[]::new);
  }

  /**
   * Lists an object's visible domains without revealing hidden subdomain roll-ups.
   *
   * @param dispatcher The domain dispatcher.
   * @param metalake The metalake name.
   * @param object The leaf metadata object.
   * @return The visible domain relations.
   */
  public static DomainEntity[] listDomainsForMetadataObject(
      DomainDispatcher dispatcher, String metalake, MetadataObject object) {
    if (!enabled()) {
      return dispatcher.listDomainsForMetadataObject(metalake, object);
    }
    if (!canAccessObject(metalake, object)) {
      throw new ForbiddenException("Cannot access metadata object %s", object.fullName());
    }
    DomainEntity[] visible =
        filterDomains(metalake, dispatcher.listDomainsForMetadataObject(metalake, object));
    Set<String> visibleNames =
        Arrays.stream(visible).map(DomainEntity::name).collect(Collectors.toSet());
    return Arrays.stream(visible)
        .map(
            domain ->
                domain.toBuilder()
                    .withRelations(filterRelations(domain.relations(), visibleNames))
                    .build())
        .filter(domain -> domain.relations().length > 0)
        .toArray(DomainEntity[]::new);
  }

  private static boolean allowed(
      String metalake,
      @Nullable DomainEntity domain,
      Privilege.Name privilege,
      AuthorizationRequestContext context) {
    if (!enabled()) {
      return true;
    }
    GravitinoAuthorizer authorizer = GravitinoEnv.getInstance().gravitinoAuthorizer();
    return authorizer != null
        && allowed(
            authorizer,
            PrincipalUtils.getCurrentPrincipal(),
            metalake,
            scopes(metalake, domain),
            privilege,
            context);
  }

  private static boolean visible(
      String metalake, DomainEntity domain, AuthorizationRequestContext context) {
    return allowed(
        metalake,
        domain,
        domain.status() == Domain.Status.DRAFT
            ? Privilege.Name.MANAGE_DOMAIN
            : Privilege.Name.USE_DOMAIN,
        context);
  }

  private static void requireVisible(
      String metalake, DomainEntity domain, AuthorizationRequestContext context) {
    if (!visible(metalake, domain, context)) {
      if (domain.status() == Domain.Status.DRAFT) {
        throw new NoSuchDomainException(
            "Domain %s under metalake %s does not exist", domain.name(), metalake);
      }
      throw new ForbiddenException("Cannot use domain %s", domain.name());
    }
  }

  private static DomainEntity[] filterDomains(
      String metalake, DomainEntity[] domains, AuthorizationRequestContext context) {
    if (!enabled()) {
      return domains;
    }
    return Arrays.stream(domains)
        .filter(domain -> visible(metalake, domain, context))
        .toArray(DomainEntity[]::new);
  }

  private static MetadataObject[] accessibleObjects(String metalake, MetadataObject[] objects) {
    GravitinoAuthorizer authorizer = GravitinoEnv.getInstance().gravitinoAuthorizer();
    if (authorizer == null) {
      return new MetadataObject[0];
    }
    return MetadataAuthzHelper.filterMetadataObject(
        metalake, objects, PrincipalUtils.getCurrentPrincipal(), authorizer);
  }

  private static DomainRelation[] filterRelations(
      DomainRelation[] relations, Set<String> visibleNames) {
    return Arrays.stream(relations)
        .filter(
            relation ->
                relation.type() != DomainRelation.Type.VIA_SUBDOMAIN
                    || relation
                        .source()
                        .map(source -> visibleNames.contains(source.fullName()))
                        .orElse(false))
        .toArray(DomainRelation[]::new);
  }
}
