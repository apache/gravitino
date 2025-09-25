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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Function;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MetadataFilterHelper performs permission checks on the list data returned by the REST API based
 * on expressions or metadata types, and calls {@link GravitinoAuthorizer} for authorization,
 * returning only the metadata that the user has permission to access.
 */
public class MetadataFilterHelper {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataFilterHelper.class);
  private static volatile Executor executor = null;

  private MetadataFilterHelper() {}

  /**
   * Call {@link GravitinoAuthorizer} to filter the metadata list
   *
   * @param entityType for example, CATALOG, SCHEMA,TABLE, etc.
   * @param privilege for example, CREATE_CATALOG, CREATE_TABLE, etc.
   * @param metadataList metadata list.
   * @return metadata List that the user has permission to access.
   */
  public static NameIdentifier[] filterByPrivilege(
      String metalake,
      Entity.EntityType entityType,
      String privilege,
      NameIdentifier[] metadataList) {
    if (!enableAuthorization()) {
      return metadataList;
    }
    checkExecutor();
    GravitinoAuthorizer gravitinoAuthorizer =
        GravitinoAuthorizerProvider.getInstance().getGravitinoAuthorizer();
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    AuthorizationRequestContext authorizationRequestContext = new AuthorizationRequestContext();
    return Arrays.stream(metadataList)
        .filter(
            metaDataName ->
                gravitinoAuthorizer.authorize(
                    currentPrincipal,
                    metalake,
                    NameIdentifierUtil.toMetadataObject(metaDataName, entityType),
                    Privilege.Name.valueOf(privilege),
                    authorizationRequestContext))
        .toArray(NameIdentifier[]::new);
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
    if (!enableAuthorization()) {
      return nameIdentifiers;
    }
    checkExecutor();
    AuthorizationRequestContext authorizationRequestContext = new AuthorizationRequestContext();
    List<CompletableFuture<NameIdentifier>> futures = new ArrayList<>();
    for (NameIdentifier nameIdentifier : nameIdentifiers) {
      Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
      futures.add(
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  return PrincipalUtils.doAs(
                      currentPrincipal,
                      () -> {
                        Map<Entity.EntityType, NameIdentifier> nameIdentifierMap =
                            spiltMetadataNames(metalake, entityType, nameIdentifier);
                        AuthorizationExpressionEvaluator authorizationExpressionEvaluator =
                            new AuthorizationExpressionEvaluator(expression);
                        return authorizationExpressionEvaluator.evaluate(
                                nameIdentifierMap, authorizationRequestContext)
                            ? nameIdentifier
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
        .toArray(NameIdentifier[]::new);
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
    if (!enableAuthorization()) {
      return entities;
    }
    checkExecutor();
    AuthorizationRequestContext authorizationRequestContext = new AuthorizationRequestContext();
    List<CompletableFuture<E>> futures = new ArrayList<>();
    for (E entity : entities) {
      Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
      futures.add(
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  return PrincipalUtils.doAs(
                      currentPrincipal,
                      () -> {
                        AuthorizationExpressionEvaluator authorizationExpressionEvaluator =
                            new AuthorizationExpressionEvaluator(expression);
                        NameIdentifier nameIdentifier = toNameIdentifier.apply(entity);
                        Map<Entity.EntityType, NameIdentifier> nameIdentifierMap =
                            spiltMetadataNames(metalake, entityType, nameIdentifier);
                        return authorizationExpressionEvaluator.evaluate(
                                nameIdentifierMap, authorizationRequestContext)
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
        .toArray(size -> (E[]) Array.newInstance(entities.getClass().getComponentType(), size));
  }

  /**
   * Extract the parent metadata from NameIdentifier. For example, when given a Table
   * NameIdentifier, it returns a map containing the Table itself along with its parent Schema and
   * Catalog.
   *
   * @param metalake metalake
   * @param entityType metadata type
   * @param nameIdentifier metadata name
   * @return A map containing the metadata object and all its parent objects, keyed by their types
   */
  private static Map<Entity.EntityType, NameIdentifier> spiltMetadataNames(
      String metalake, Entity.EntityType entityType, NameIdentifier nameIdentifier) {
    Map<Entity.EntityType, NameIdentifier> nameIdentifierMap = new HashMap<>();
    nameIdentifierMap.put(Entity.EntityType.METALAKE, NameIdentifierUtil.ofMetalake(metalake));
    switch (entityType) {
      case CATALOG:
        nameIdentifierMap.put(Entity.EntityType.CATALOG, nameIdentifier);
        break;
      case SCHEMA:
        nameIdentifierMap.put(Entity.EntityType.SCHEMA, nameIdentifier);
        nameIdentifierMap.put(
            Entity.EntityType.CATALOG, NameIdentifierUtil.getCatalogIdentifier(nameIdentifier));
        break;
      case TABLE:
        nameIdentifierMap.put(Entity.EntityType.TABLE, nameIdentifier);
        nameIdentifierMap.put(
            Entity.EntityType.SCHEMA, NameIdentifierUtil.getSchemaIdentifier(nameIdentifier));
        nameIdentifierMap.put(
            Entity.EntityType.CATALOG, NameIdentifierUtil.getCatalogIdentifier(nameIdentifier));
        break;
      case MODEL:
        nameIdentifierMap.put(Entity.EntityType.MODEL, nameIdentifier);
        nameIdentifierMap.put(
            Entity.EntityType.SCHEMA, NameIdentifierUtil.getSchemaIdentifier(nameIdentifier));
        nameIdentifierMap.put(
            Entity.EntityType.CATALOG, NameIdentifierUtil.getCatalogIdentifier(nameIdentifier));
        break;
      case MODEL_VERSION:
        nameIdentifierMap.put(Entity.EntityType.MODEL_VERSION, nameIdentifier);
        nameIdentifierMap.put(
            Entity.EntityType.MODEL, NameIdentifierUtil.getModelIdentifier(nameIdentifier));
        nameIdentifierMap.put(
            Entity.EntityType.SCHEMA, NameIdentifierUtil.getSchemaIdentifier(nameIdentifier));
        nameIdentifierMap.put(
            Entity.EntityType.CATALOG, NameIdentifierUtil.getCatalogIdentifier(nameIdentifier));
        break;
      case TOPIC:
        nameIdentifierMap.put(Entity.EntityType.TOPIC, nameIdentifier);
        nameIdentifierMap.put(
            Entity.EntityType.SCHEMA, NameIdentifierUtil.getSchemaIdentifier(nameIdentifier));
        nameIdentifierMap.put(
            Entity.EntityType.CATALOG, NameIdentifierUtil.getCatalogIdentifier(nameIdentifier));
        break;
      case FILESET:
        nameIdentifierMap.put(Entity.EntityType.FILESET, nameIdentifier);
        nameIdentifierMap.put(
            Entity.EntityType.SCHEMA, NameIdentifierUtil.getSchemaIdentifier(nameIdentifier));
        nameIdentifierMap.put(
            Entity.EntityType.CATALOG, NameIdentifierUtil.getCatalogIdentifier(nameIdentifier));
        break;
      case METALAKE:
        nameIdentifierMap.put(entityType, nameIdentifier);
        break;
      case ROLE:
        nameIdentifierMap.put(entityType, nameIdentifier);
        break;
      case USER:
        nameIdentifierMap.put(entityType, nameIdentifier);
        break;
      default:
        throw new IllegalArgumentException("Unsupported entity type: " + entityType);
    }
    return nameIdentifierMap;
  }

  private static boolean enableAuthorization() {
    Config config = GravitinoEnv.getInstance().config();
    return config != null && config.get(Configs.ENABLE_AUTHORIZATION);
  }

  private static void checkExecutor() {
    if (executor == null) {
      synchronized (MetadataFilterHelper.class) {
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
}
