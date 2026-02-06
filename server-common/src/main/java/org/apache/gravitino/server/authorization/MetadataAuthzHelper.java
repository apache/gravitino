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

import java.lang.reflect.Array;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Function;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.Metalake;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
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

  private MetadataAuthzHelper() {}

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
    preloadToCache(entityType, nameIdentifiers);
    return filterByExpression(metalake, expression, entityType, nameIdentifiers, e -> e);
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
                  LOG.error("GravitinoAuthorize error:{}", e.getMessage(), e);
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
    if (!GravitinoEnv.getInstance().cacheEnabled()) {
      return;
    }

    // Only preload entity types that support batch get operations
    if (!SUPPORTED_PRELOAD_ENTITY_TYPES.contains(entityType)) {
      return;
    }

    try {
      GravitinoEnv.getInstance()
          .entityStore()
          .batchGet(
              Arrays.asList(nameIdentifiers),
              entityType,
              EntityClassMapper.getEntityClass(entityType));
    } catch (Exception e) {
      // Ignore exceptions when preloading to cache.
      // This can happen when the metadata is not registered in Gravitino
      // (e.g., browsing external catalog data in read-only mode).
      // The metadata can still be accessed directly from the external source.
      LOG.debug(
          "Failed to preload {} entities to cache, this may happen when browsing "
              + "unregistered metadata: {}",
          nameIdentifiers.length,
          e.getMessage());
    }
  }
}
