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

import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privilege.Name;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.storage.relational.RelationalEntityStoreIdResolver;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * Evaluates target object read and admin authorization for glossary bindings using {@link
 * GravitinoAuthorizer}. Matches the platform's {@code LOAD_*} and {@code MODIFY_*}/{@code OWNER}
 * authorization expressions across all supported asset types and semantic models.
 */
final class GlossaryBindingAccess {
  private static final GlossaryBindingAccess UNRESTRICTED =
      new GlossaryBindingAccess(null, null, null, null, null);

  private final GravitinoAuthorizer authorizer;
  private final Principal principal;
  private final String metalake;
  private final AuthorizationRequestContext context;
  @Nullable private final Map<MetadataObject, Optional<Long>> resolvedScopes;

  private GlossaryBindingAccess(
      GravitinoAuthorizer authorizer,
      Principal principal,
      String metalake,
      AuthorizationRequestContext context,
      @Nullable Map<MetadataObject, Optional<Long>> resolvedScopes) {
    this.authorizer = authorizer;
    this.principal = principal;
    this.metalake = metalake;
    this.context = context;
    this.resolvedScopes = resolvedScopes;
  }

  static GlossaryBindingAccess forRequest(String metalake, boolean enabled) {
    if (!enabled) {
      return UNRESTRICTED;
    }
    return new GlossaryBindingAccess(
        bindingAuthorizer(),
        PrincipalUtils.getCurrentPrincipal(),
        metalake,
        new AuthorizationRequestContext(),
        null);
  }

  static GlossaryBindingAccess forResolvedTarget(
      String metalake, boolean enabled, MetadataObject target, NamespacedEntityId ids) {
    if (!enabled) {
      return UNRESTRICTED;
    }
    Map<MetadataObject, Optional<Long>> scopes = new HashMap<>();
    cacheChain(scopes, metalake, target, ids);
    return forSnapshot(metalake, scopes);
  }

  /** Uses the binding read snapshot for canonical target and ancestor IDs. */
  static GlossaryBindingAccess forBindingList(String metalake, boolean enabled, long metalakeId) {
    if (!enabled) {
      return UNRESTRICTED;
    }
    Map<MetadataObject, Optional<Long>> scopes = new HashMap<>();
    scopes.put(
        MetadataObjects.of(null, metalake, MetadataObject.Type.METALAKE), Optional.of(metalakeId));
    return forSnapshot(metalake, scopes);
  }

  /** Seeds a known live identity from the current snapshot so target checks skip name lookups. */
  void registerTarget(MetadataObject target, long entityId) {
    if (resolvedScopes != null) {
      resolvedScopes.putIfAbsent(target, Optional.of(entityId));
    }
  }

  /** Returns whether the caller has ordinary metadata read access to the binding target. */
  boolean canReadTarget(MetadataObject target) {
    if (this == UNRESTRICTED) {
      return true;
    }
    MetadataObject effective = normalizeTarget(target);
    MetadataObject lake = MetadataObjects.of(null, metalake, MetadataObject.Type.METALAKE);
    NameIdentifier ident = MetadataObjectUtil.toEntityIdent(metalake, effective);
    return switch (effective.type()) {
      case CATALOG -> {
        MetadataObject catalog =
            MetadataObjects.of(null, ident.name(), MetadataObject.Type.CATALOG);
        yield isOwnerAny(lake, catalog) || anyEffective(Name.USE_CATALOG, lake, catalog);
      }
      case SCHEMA -> {
        MetadataObject catalog =
            MetadataObjects.of(null, ident.namespace().level(1), MetadataObject.Type.CATALOG);
        MetadataObject schema =
            MetadataObjects.of(
                ident.namespace().level(1), ident.name(), MetadataObject.Type.SCHEMA);
        yield isOwnerAny(lake, catalog)
            || (anyEffective(Name.USE_CATALOG, lake, catalog)
                && (isOwnerAny(schema) || anyEffective(Name.USE_SCHEMA, lake, catalog, schema)));
      }
      case TABLE -> canReadLeaf(lake, ident, effective, Name.SELECT_TABLE, Name.MODIFY_TABLE);
      case VIEW -> canReadLeaf(lake, ident, effective, Name.SELECT_VIEW);
      case FILESET -> canReadLeaf(lake, ident, effective, Name.READ_FILESET, Name.WRITE_FILESET);
      case TOPIC -> canReadLeaf(lake, ident, effective, Name.CONSUME_TOPIC, Name.PRODUCE_TOPIC);
      case MODEL -> canReadLeaf(lake, ident, effective, Name.USE_MODEL);
      case SEMANTIC_MODEL -> canReadLeaf(
          lake, ident, effective, Name.USE_SEMANTIC_MODEL, Name.MODIFY_SEMANTIC_MODEL);
      default -> false;
    };
  }

  /** Returns whether the caller has admin privilege on the binding target. */
  boolean canAdminTarget(MetadataObject target) {
    if (this == UNRESTRICTED) {
      return true;
    }
    MetadataObject effective = normalizeTarget(target);
    MetadataObject lake = MetadataObjects.of(null, metalake, MetadataObject.Type.METALAKE);
    NameIdentifier ident = MetadataObjectUtil.toEntityIdent(metalake, effective);
    return switch (effective.type()) {
      case CATALOG -> {
        MetadataObject catalog =
            MetadataObjects.of(null, ident.name(), MetadataObject.Type.CATALOG);
        yield isOwnerAny(lake, catalog);
      }
      case SCHEMA -> {
        MetadataObject catalog =
            MetadataObjects.of(null, ident.namespace().level(1), MetadataObject.Type.CATALOG);
        MetadataObject schema =
            MetadataObjects.of(
                ident.namespace().level(1), ident.name(), MetadataObject.Type.SCHEMA);
        yield isOwnerAny(lake, catalog)
            || (anyEffective(Name.USE_CATALOG, lake, catalog) && isOwnerAny(schema));
      }
      case TABLE -> canReadLeaf(lake, ident, effective, Name.MODIFY_TABLE);
      case VIEW, FILESET, TOPIC, MODEL -> canReadLeaf(lake, ident, effective);
      case SEMANTIC_MODEL -> canReadLeaf(lake, ident, effective, Name.MODIFY_SEMANTIC_MODEL);
      default -> false;
    };
  }

  static MetadataObject normalizeTarget(MetadataObject target) {
    if (target.type() == MetadataObject.Type.COLUMN) {
      return MetadataObjects.parse(target.parent(), MetadataObject.Type.TABLE);
    }
    return target;
  }

  private boolean canReadLeaf(
      MetadataObject lake, NameIdentifier ident, MetadataObject leaf, Name... privileges) {
    MetadataObject catalog =
        MetadataObjects.of(null, ident.namespace().level(1), MetadataObject.Type.CATALOG);
    MetadataObject schema =
        MetadataObjects.of(
            ident.namespace().level(1), ident.namespace().level(2), MetadataObject.Type.SCHEMA);
    if (isOwnerAny(lake, catalog)) {
      return true;
    }
    boolean useCatalog = anyEffective(Name.USE_CATALOG, lake, catalog);
    if (!useCatalog) {
      return false;
    }
    if (isOwnerAny(schema)) {
      return true;
    }
    if (!anyEffective(Name.USE_SCHEMA, lake, catalog, schema)) {
      return false;
    }
    if (isOwnerAny(leaf)) {
      return true;
    }
    for (Name privilege : privileges) {
      if (anyEffective(privilege, lake, catalog, schema, leaf)) {
        return true;
      }
    }
    return false;
  }

  private boolean isOwnerAny(MetadataObject... scopes) {
    for (MetadataObject scope : scopes) {
      if (authorizer.isOwner(principal, metalake, scope, context)) {
        return true;
      }
    }
    return false;
  }

  private boolean anyEffective(Name privilege, MetadataObject... scopes) {
    boolean allowed = false;
    for (MetadataObject scope : scopes) {
      if (authorizer.deny(principal, metalake, scope, privilege, context)) {
        return false;
      }
      if (!allowed && authorizer.authorize(principal, metalake, scope, privilege, context)) {
        allowed = true;
      }
    }
    return allowed;
  }

  private static GravitinoAuthorizer bindingAuthorizer() {
    GravitinoAuthorizer authorizer = GravitinoEnv.getInstance().gravitinoAuthorizer();
    if (!authorizer.supportsResolvedTargetAuthorization()) {
      throw new ForbiddenException(
          "The configured authorizer does not support glossary binding checks by target ID");
    }
    return authorizer;
  }

  private static void cacheChain(
      Map<MetadataObject, Optional<Long>> scopes,
      String metalake,
      MetadataObject target,
      NamespacedEntityId ids) {
    long[] chain = ids.fullIds();
    MetadataObject scope = target;
    for (int i = chain.length - 1; i > 0; i--) {
      scopes.put(scope, Optional.of(chain[i]));
      scope = MetadataObjects.parent(scope);
    }
    scopes.put(
        MetadataObjects.of(null, metalake, MetadataObject.Type.METALAKE), Optional.of(chain[0]));
  }

  private static GlossaryBindingAccess forSnapshot(
      String metalake, Map<MetadataObject, Optional<Long>> scopes) {
    RelationalEntityStoreIdResolver resolver = new RelationalEntityStoreIdResolver();
    AuthorizationRequestContext context =
        new AuthorizationRequestContext(
            metalake,
            scope -> {
              Optional<Long> existing = scopes.get(scope);
              if (existing != null) {
                return existing;
              }
              try {
                NamespacedEntityId ids =
                    resolver.getEntityIds(
                        MetadataObjectUtil.toEntityIdent(metalake, scope),
                        MetadataObjectUtil.toEntityType(scope));
                cacheChain(scopes, metalake, scope, ids);
                return Optional.of(ids.entityId());
              } catch (NoSuchEntityException missing) {
                scopes.put(scope, Optional.empty());
                return Optional.empty();
              }
            });
    return new GlossaryBindingAccess(
        bindingAuthorizer(), PrincipalUtils.getCurrentPrincipal(), metalake, context, scopes);
  }
}
