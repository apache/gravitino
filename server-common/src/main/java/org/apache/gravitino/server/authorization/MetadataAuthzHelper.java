/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.server.authorization;

import com.google.common.base.Preconditions;
import java.lang.reflect.Array;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Function;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.Metalake;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.dto.tag.MetadataObjectDTO;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.utils.EntityClassMapper;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MetadataFilterHelper performs permission checks on the list data returned by the REST API based
 * on expressions or metadata types, and calls {@link GravitinoAuthorizer} for authorization,
 * returning only the metadata that the user has permission to access.
 */
public class MetadataAuthzHelper {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataAuthzHelper.class);
  private static volatile Executor executor = null;

  /**
   * Entity types that support batch get operations for cache preloading. These types have
   * implemented the batchGetByIdentifier method in their respective MetaService classes.
   */
  private static final List<Entity.EntityType> SUPPORTED_PRELOAD_ENTITY_TYPES =
      Arrays.asList(
          Entity.EntityType.METALAKE,
          Entity.EntityType.CATALOG,
          Entity.EntityType.SCHEMA,
          Entity.EntityType.TABLE,
          Entity.EntityType.FILESET,
          Entity.EntityType.TOPIC,
          Entity.EntityType.MODEL,
          Entity.EntityType.TAG,
          Entity.EntityType.POLICY,
          Entity.EntityType.JOB,
          Entity.EntityType.JOB_TEMPLATE);

  /**
   * Topic and Table may be from the external system and the schema may not exist in Gravitino, so
   * we need to import the schema first
   */
  private static final List<Entity.EntityType> REQUIRE_SCHEMA_EXISTS =
      Arrays.asList(Entity.EntityType.TABLE, Entity.EntityType.TOPIC);

  private static final String TABLE_PARENT_SCOPES = "METALAKE, CATALOG, SCHEMA";
  private static final String SCHEMA_PARENT_SCOPES = "METALAKE, CATALOG";
  private static final String CATALOG_PARENT_SCOPES = "METALAKE";

  /**
   * Registry of list-authorization short-circuits keyed by the listed object's entity type. Each
   * entry pairs a per-object filter expression with the alternative parent-scope access paths that
   * can make every listed object visible. Each path tracks only the deny privileges that can
   * invalidate that path, so a deny on one path does not disable an independent path.
   */
  private static final Map<Entity.EntityType, Map<String, List<ParentScopeAccessPath>>>
      LIST_SHORT_CIRCUITS =
          Map.of(
              Entity.EntityType.TABLE,
              Map.of(
                  AuthorizationExpressionConstants.FILTER_TABLE_AUTHORIZATION_EXPRESSION,
                  List.of(
                      parentOwnerPath(TABLE_PARENT_SCOPES),
                      parentPrivilegePath(Privilege.Name.SELECT_TABLE, TABLE_PARENT_SCOPES),
                      parentPrivilegePath(Privilege.Name.MODIFY_TABLE, TABLE_PARENT_SCOPES)),
                  AuthorizationExpressionConstants.LIST_TABLE_LIKE_AUTHORIZATION_EXPRESSION,
                  List.of(
                      parentOwnerPath(TABLE_PARENT_SCOPES),
                      tableLikeParentPrivilegePath(Privilege.Name.PROBE_TABLE_LIKE),
                      tableLikeParentPrivilegePath(Privilege.Name.SELECT_TABLE),
                      tableLikeParentPrivilegePath(Privilege.Name.MODIFY_TABLE),
                      tableLikeParentPrivilegePath(Privilege.Name.CREATE_TABLE),
                      tableLikeParentPrivilegePath(Privilege.Name.CREATE_VIEW))),
              Entity.EntityType.SCHEMA,
              Map.of(
                  AuthorizationExpressionConstants.FILTER_SCHEMA_AUTHORIZATION_EXPRESSION,
                  List.of(
                      parentOwnerPath(SCHEMA_PARENT_SCOPES),
                      parentPrivilegePath(Privilege.Name.USE_SCHEMA, SCHEMA_PARENT_SCOPES))),
              Entity.EntityType.CATALOG,
              Map.of(
                  AuthorizationExpressionConstants.LOAD_CATALOG_AUTHORIZATION_EXPRESSION,
                  List.of(
                      parentOwnerPath(CATALOG_PARENT_SCOPES),
                      parentPrivilegePath(Privilege.Name.USE_CATALOG, CATALOG_PARENT_SCOPES))));

  /** A sufficient parent-scope access path and the deny privileges that can invalidate it. */
  private static final class ParentScopeAccessPath {
    private final String expression;
    private final Set<Privilege.Name> denyPrivileges;

    private ParentScopeAccessPath(String expression, Set<Privilege.Name> denyPrivileges) {
      this.expression = expression;
      this.denyPrivileges = denyPrivileges;
    }
  }

  private MetadataAuthzHelper() {}

  private static ParentScopeAccessPath parentOwnerPath(String parentScopes) {
    return new ParentScopeAccessPath("ANY(OWNER, " + parentScopes + ")", Set.of());
  }

  private static ParentScopeAccessPath parentPrivilegePath(
      Privilege.Name privilege, String parentScopes) {
    return new ParentScopeAccessPath(
        String.format("ANY(%s, %s)", privilege.name(), parentScopes), Set.of(privilege));
  }

  private static ParentScopeAccessPath tableLikeParentPrivilegePath(Privilege.Name privilege) {
    ParentScopeAccessPath privilegePath = parentPrivilegePath(privilege, TABLE_PARENT_SCOPES);
    return new ParentScopeAccessPath(
        "ANY_USE_CATALOG && ANY_USE_SCHEMA && (" + privilegePath.expression + ")",
        privilegePath.denyPrivileges);
  }

  public static Metalake[] filterMetalakes(Metalake[] metalakes, String expression) {
    AuthorizationRequestContext authorizationRequestContext = new AuthorizationRequestContext();
    return doFilter(
        expression,
        metalakes,
        PrincipalUtils.getCurrentPrincipal(),
        GravitinoAuthorizerProvider.getInstance().getGravitinoAuthorizer(),
        authorizationRequestContext,
        metalake -> {
          String metalakeName = metalake.name();
          return NameIdentifierUtil.splitNameIdentifier(
              metalakeName,
              Entity.EntityType.METALAKE,
              NameIdentifierUtil.ofMetalake(metalakeName));
        },
        (unused) -> null);
  }

  /**
   * Filters MetadataObjectDTO array based on access permissions.
   *
   * @param metalake The metalake name
   * @param metadataObjects The array of metadata object DTOs to filter
   * @return Filtered array of metadata object DTOs that the current user has access to
   */
  public static MetadataObjectDTO[] filterMetadataObject(
      String metalake, MetadataObjectDTO[] metadataObjects) {
    return doFilter(
        AuthorizationExpressionConstants.CAN_ACCESS_METADATA,
        metadataObjects,
        PrincipalUtils.getCurrentPrincipal(),
        GravitinoAuthorizerProvider.getInstance().getGravitinoAuthorizer(),
        new AuthorizationRequestContext(),
        metadataObject ->
            NameIdentifierUtil.splitNameIdentifier(
                metalake,
                MetadataObjectUtil.toEntityType(metadataObject.type()),
                MetadataObjectUtil.toEntityIdent(metalake, metadataObject)),
        metadataObject -> MetadataObjectUtil.toEntityType(metadataObject.type()));
  }

  /**
   * Filters MetadataObject array based on access permissions.
   *
   * @param metalake The metalake name
   * @param metadataObjects The array of metadata objects to filter
   * @return Filtered array of metadata objects that the current user has access to
   */
  public static MetadataObject[] filterMetadataObject(
      String metalake, MetadataObject[] metadataObjects) {
    return doFilter(
        AuthorizationExpressionConstants.CAN_ACCESS_METADATA,
        metadataObjects,
        PrincipalUtils.getCurrentPrincipal(),
        GravitinoAuthorizerProvider.getInstance().getGravitinoAuthorizer(),
        new AuthorizationRequestContext(),
        metadataObject ->
            NameIdentifierUtil.splitNameIdentifier(
                metalake,
                MetadataObjectUtil.toEntityType(metadataObject.type()),
                MetadataObjectUtil.toEntityIdent(metalake, metadataObject)),
        metadataObject -> MetadataObjectUtil.toEntityType(metadataObject.type()));
  }

  /**
   * Call {@link AuthorizationExpressionEvaluator} to filter the metadata list
   *
   * @param metalake metalake
   * @param expression authorization expression
   * @param entityType for example, CATALOG, SCHEMA,TABLE, etc.
   * @param nameIdentifiers metaData list.
   * @return metadata List that the user has permission to access.
   */
  public static NameIdentifier[] filterByExpression(
      String metalake,
      String expression,
      Entity.EntityType entityType,
      NameIdentifier[] nameIdentifiers) {
    return filterByExpression(metalake, expression, entityType, nameIdentifiers, e -> e);
  }

  /**
   * Attempts the list-authorization short-circuit: when the listed objects all share one parent and
   * the matching filter expression is granted at a parent scope, the whole list is visible unless
   * an object-level deny may exist. Returns {@code true} only when it is safe to return every
   * identifier without per-object authorization.
   */
  private static boolean allVisibleViaParentScope(
      String metalake,
      String expression,
      Entity.EntityType entityType,
      NameIdentifier[] nameIdentifiers) {
    Principal principal = PrincipalUtils.getCurrentPrincipal();
    Map<String, List<ParentScopeAccessPath>> entityShortCircuits =
        LIST_SHORT_CIRCUITS.get(entityType);
    List<ParentScopeAccessPath> accessPaths =
        entityShortCircuits == null ? null : entityShortCircuits.get(expression);
    if (accessPaths == null) {
      LOG.debug(
          "Parent-scope short-circuit unavailable for principal {}, entity type {} under metalake "
              + "{}: {}.",
          principal.getName(),
          entityType,
          metalake,
          entityShortCircuits == null
              ? "no short-circuit spec is registered for this entity type"
              : "the requested filter expression does not match a registered short-circuit "
                  + "expression");
      return false;
    }

    // The short-circuit reasons about a single parent scope, so every identifier must share it.
    Namespace parent = nameIdentifiers[0].namespace();
    for (NameIdentifier ident : nameIdentifiers) {
      if (!ident.namespace().equals(parent)) {
        LOG.debug(
            "Parent-scope short-circuit skipped for principal {}, entity type {} under metalake "
                + "{}: listed objects span multiple parent namespaces (e.g. {} vs {}).",
            principal.getName(),
            entityType,
            metalake,
            parent,
            ident.namespace());
        return false;
      }
    }

    GravitinoAuthorizer authorizer =
        GravitinoAuthorizerProvider.getInstance().getGravitinoAuthorizer();
    AuthorizationRequestContext requestContext = new AuthorizationRequestContext();
    Map<Entity.EntityType, NameIdentifier> metadataNames =
        NameIdentifierUtil.splitNameIdentifier(metalake, entityType, nameIdentifiers[0]);

    for (ParentScopeAccessPath accessPath : accessPaths) {
      requestContext.setOriginalAuthorizationExpression(accessPath.expression);
      boolean parentGrantsAccess =
          new AuthorizationExpressionEvaluator(accessPath.expression, authorizer)
              .evaluate(metadataNames, requestContext, principal, Optional.empty());
      if (!parentGrantsAccess) {
        continue;
      }

      boolean hasDeny =
          !accessPath.denyPrivileges.isEmpty()
              && authorizer.hasDenyPolicy(
                  principal, metalake, accessPath.denyPrivileges, requestContext);
      if (!hasDeny) {
        return true;
      }

      LOG.debug(
          "Parent-scope access path {} disabled for entity type {} under metalake {}: principal "
              + "{} holds a deny policy on {}.",
          accessPath.expression,
          entityType,
          metalake,
          principal.getName(),
          accessPath.denyPrivileges);
    }

    LOG.debug(
        "Parent-scope short-circuit skipped for entity type {} under metalake {}: principal {} "
            + "has no deny-free parent access path, so per-object authorization is required.",
        entityType,
        metalake,
        principal.getName());
    return false;
  }

  /**
   * Call {@link AuthorizationExpressionEvaluator} to check access
   *
   * @param identifier metadata identifier
   * @param entityType for example, CATALOG, SCHEMA,TABLE, etc.
   * @param expression authorization expression
   * @return whether it has access to the metadata
   */
  public static boolean checkAccess(
      NameIdentifier identifier, Entity.EntityType entityType, String expression) {
    if (!enableAuthorization()) {
      return true;
    }

    String metalake = NameIdentifierUtil.getMetalake(identifier);
    Map<Entity.EntityType, NameIdentifier> nameIdentifierMap =
        NameIdentifierUtil.splitNameIdentifier(metalake, entityType, identifier);
    AuthorizationExpressionEvaluator authorizationExpressionEvaluator =
        new AuthorizationExpressionEvaluator(expression);
    return authorizationExpressionEvaluator.evaluate(
        nameIdentifierMap, new AuthorizationRequestContext());
  }

  /**
   * Call {@link AuthorizationExpressionEvaluator} to filter the metadata list
   *
   * @param metalake metalake
   * @param expression expression
   * @param entityType entity type
   * @param entities entities
   * @param toNameIdentifier convert to NameIdentifier
   * @return Filtered Metadata Entity
   * @param <E> Entity class
   */
  public static <E> E[] filterByExpression(
      String metalake,
      String expression,
      Entity.EntityType entityType,
      E[] entities,
      Function<E, NameIdentifier> toNameIdentifier) {
    // Every list endpoint funnels through here, whichever shape it holds its results in, so the
    // short-circuit and the preloads live at this one point. Keeping them in the NameIdentifier[]
    // overload alone let the verbose catalog listing, which carries Catalog objects, run the
    // per-object loop over every catalog in the metalake.
    NameIdentifier[] nameIdentifiers =
        Arrays.stream(entities).map(toNameIdentifier).toArray(NameIdentifier[]::new);
    if (enableAuthorization() && nameIdentifiers.length > 0) {
      String principalName = PrincipalUtils.getCurrentPrincipal().getName();
      if (allVisibleViaParentScope(metalake, expression, entityType, nameIdentifiers)) {
        // A privilege granted at a parent scope (metalake/catalog/schema) makes every object in
        // the list visible, and no object-level deny exists, so the per-object authorization loop
        // is skipped entirely. See AuthorizationExpressionConstants.*_LIST_PARENT_SCOPE_*.
        LOG.debug(
            "List authorization short-circuit HIT for principal {}, entity type {} under metalake "
                + "{}: all {} listed object(s) are visible via a parent-scope grant; skipping the "
                + "per-object authorization loop.",
            principalName,
            entityType,
            metalake,
            nameIdentifiers.length);
        return entities;
      }
      LOG.debug(
          "List authorization short-circuit MISS for principal {}, entity type {} under metalake "
              + "{} ({} object(s)); falling back to the per-object authorization loop.",
          principalName,
          entityType,
          metalake,
          nameIdentifiers.length);
    }
    preloadToCache(entityType, nameIdentifiers);
    preloadOwner(entityType, nameIdentifiers);

    GravitinoAuthorizer authorizer =
        GravitinoAuthorizerProvider.getInstance().getGravitinoAuthorizer();
    AuthorizationRequestContext authorizationRequestContext = new AuthorizationRequestContext();
    return doFilter(
        expression,
        entities,
        PrincipalUtils.getCurrentPrincipal(),
        authorizer,
        authorizationRequestContext,
        (entity) -> {
          NameIdentifier nameIdentifier = toNameIdentifier.apply(entity);
          return NameIdentifierUtil.splitNameIdentifier(metalake, entityType, nameIdentifier);
        },
        (unused) -> null);
  }

  /**
   * Call {@link AuthorizationExpressionEvaluator} and use specified Principal and
   * GravitinoAuthorizer to filter the metadata list
   *
   * @param metalake metalake name
   * @param expression authorization expression
   * @param entityType entity type
   * @param entities metadata entities
   * @param toNameIdentifier function to convert entity to NameIdentifier
   * @param currentPrincipal The principal to perform the authorization check as. This is intended
   *     as an extension point for external modules to inject a specific security context, so please
   *     do not remove it.
   * @param authorizer The authorizer to use for the authorization check. This is intended as an
   *     extension point for external modules to inject a specific authorization mechanism, so
   *     please do not remove it.
   * @return Filtered Metadata Entity
   * @param <E> Entity class
   */
  public static <E> E[] filterByExpression(
      String metalake,
      String expression,
      Entity.EntityType entityType,
      E[] entities,
      Function<E, NameIdentifier> toNameIdentifier,
      Principal currentPrincipal,
      GravitinoAuthorizer authorizer) {
    AuthorizationRequestContext authorizationRequestContext = new AuthorizationRequestContext();
    return doFilter(
        expression,
        entities,
        currentPrincipal,
        authorizer,
        authorizationRequestContext,
        (entity) -> {
          NameIdentifier nameIdentifier = toNameIdentifier.apply(entity);
          return NameIdentifierUtil.splitNameIdentifier(metalake, entityType, nameIdentifier);
        },
        (unused) -> null);
  }

  /**
   * Filters entities based on authorization expression evaluation.
   *
   * @param expression The authorization expression to evaluate
   * @param entities The array of entities to filter
   * @param currentPrincipal The principal used to evaluate permissions
   * @param authorizer The authorizer used to evaluate permissions
   * @param authorizationRequestContext The context of the authorization request
   * @param extractMetadataNamesMap Function to extract metadata names map from entity
   * @param extractEntityType Function to extract entity type from entity
   * @param <E> The type of entity
   * @return Filtered array of entities that passed authorization check
   */
  private static <E> E[] doFilter(
      String expression,
      E[] entities,
      Principal currentPrincipal,
      GravitinoAuthorizer authorizer,
      AuthorizationRequestContext authorizationRequestContext,
      Function<E, Map<Entity.EntityType, NameIdentifier>> extractMetadataNamesMap,
      Function<E, Entity.EntityType> extractEntityType) {
    if (!enableAuthorization()) {
      return entities;
    }
    checkExecutor();
    authorizationRequestContext.setOriginalAuthorizationExpression(expression);
    List<CompletableFuture<E>> futures = new ArrayList<>();
    for (E entity : entities) {
      futures.add(
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  return PrincipalUtils.doAs(
                      currentPrincipal,
                      () -> {
                        AuthorizationExpressionEvaluator authorizationExpressionEvaluator =
                            new AuthorizationExpressionEvaluator(expression, authorizer);
                        return authorizationExpressionEvaluator.evaluate(
                                extractMetadataNamesMap.apply(entity),
                                authorizationRequestContext,
                                currentPrincipal,
                                Optional.ofNullable(extractEntityType.apply(entity))
                                    .map(Entity.EntityType::name))
                            ? entity
                            : null;
                      });
                } catch (Exception e) {
                  LOG.error("GravitinoAuthorizer error: {}", e.getMessage(), e);
                  return null;
                }
              },
              executor));
    }
    return futures.stream()
        .map(CompletableFuture::join)
        .filter(Objects::nonNull)
        .toArray(size -> createArray(entities.getClass().getComponentType(), size));
  }

  @SuppressWarnings("unchecked")
  private static <E> E[] createArray(Class<?> componentType, int size) {
    return (E[]) Array.newInstance(componentType, size);
  }

  private static boolean enableAuthorization() {
    Config config = GravitinoEnv.getInstance().config();
    return config != null && config.get(Configs.ENABLE_AUTHORIZATION);
  }

  private static void checkExecutor() {
    if (executor == null) {
      synchronized (MetadataAuthzHelper.class) {
        if (executor == null) {
          executor =
              Executors.newFixedThreadPool(
                  GravitinoEnv.getInstance()
                      .config()
                      .get(Configs.GRAVITINO_AUTHORIZATION_THREAD_POOL_SIZE),
                  runnable -> {
                    Thread thread = new Thread(runnable);
                    thread.setDaemon(true);
                    thread.setName("MetadataFilterHelper-ThreadPool-" + thread.getId());
                    return thread;
                  });
        }
      }
    }
  }

  private static void preloadToCache(
      Entity.EntityType entityType, NameIdentifier[] nameIdentifiers) {
    // If cache is not enabled or access control dispatcher is not set, skip preloading to cache
    if (!GravitinoEnv.getInstance().cacheEnabled()
        || GravitinoEnv.getInstance().internalAccessControlDispatcher() == null
        || nameIdentifiers.length == 0) {
      return;
    }

    // Only preload entity types that support batch get operations
    if (!SUPPORTED_PRELOAD_ENTITY_TYPES.contains(entityType)) {
      return;
    }

    if (REQUIRE_SCHEMA_EXISTS.contains(entityType)) {
      // For entity types that require schema existence, check if the schema exists before
      // preloading to cache
      Namespace firstNamespace = nameIdentifiers[0].namespace();
      Preconditions.checkArgument(
          Arrays.stream(nameIdentifiers).allMatch(id -> id.namespace().equals(firstNamespace)),
          "All identifiers must have the same schema");

      if (!GravitinoEnv.getInstance()
          .internalSchemaDispatcher()
          .schemaExists(NameIdentifier.parse(firstNamespace.toString()))) {
        return;
      }
    }

    GravitinoEnv.getInstance()
        .entityStore()
        .batchGet(
            Arrays.asList(nameIdentifiers),
            entityType,
            EntityClassMapper.getEntityClass(entityType));
  }

  private static void preloadOwner(Entity.EntityType entityType, NameIdentifier[] nameIdentifiers) {
    if (!GravitinoEnv.getInstance().cacheEnabled()) {
      return;
    }
    EntityStore entityStore = GravitinoEnv.getInstance().entityStore();
    try {
      entityStore
          .relationOperations()
          .batchListEntitiesByRelation(
              SupportsRelationOperations.Type.OWNER_REL,
              Arrays.stream(nameIdentifiers).toList(),
              entityType);
    } catch (Exception e) {
      LOG.warn("Ignore preloadOwner error:{}", e.getMessage(), e);
    }
  }
}
