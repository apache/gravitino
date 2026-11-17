/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.iceberg.service.dispatcher;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.service.CatalogWrapperForREST;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.cleanup.IcebergCleanupManager;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RegisterViewRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergNamespaceOperationExecutor implements IcebergNamespaceOperationDispatcher {

  private static final Logger LOG =
      LoggerFactory.getLogger(IcebergNamespaceOperationExecutor.class);

  private final IcebergCatalogWrapperManager icebergCatalogWrapperManager;
  private final Optional<IcebergCleanupManager> cleanupManager;

  public IcebergNamespaceOperationExecutor(
      IcebergCatalogWrapperManager icebergCatalogWrapperManager,
      Optional<IcebergCleanupManager> cleanupManager) {
    this.icebergCatalogWrapperManager = icebergCatalogWrapperManager;
    this.cleanupManager = cleanupManager;
  }

  @Override
  public CreateNamespaceResponse createNamespace(
      IcebergRequestContext context, CreateNamespaceRequest createNamespaceRequest) {
    checkLevelsDoNotContainSchemaSeparator(createNamespaceRequest.namespace());
    String authenticatedUser = context.userName();
    if (!AuthConstants.ANONYMOUS_USER.equals(authenticatedUser)) {
      String existingOwner = createNamespaceRequest.properties().get(IcebergConstants.OWNER);

      // Override the owner as the authenticated user if different from authenticated user
      if (!authenticatedUser.equals(existingOwner)) {
        Map<String, String> properties = new HashMap<>(createNamespaceRequest.properties());
        properties.put(IcebergConstants.OWNER, authenticatedUser);
        LOG.debug(
            "Overriding namespace owner from '{}' to authenticated user: '{}'",
            existingOwner,
            authenticatedUser);

        // CreateNamespaceRequest is immutable, so we need to rebuild it with modified properties
        createNamespaceRequest =
            CreateNamespaceRequest.builder()
                .withNamespace(createNamespaceRequest.namespace())
                .setProperties(properties)
                .build();
      }
    }

    return icebergCatalogWrapperManager
        .getCatalogWrapper(context.catalogName())
        .createNamespace(createNamespaceRequest);
  }

  @Override
  public UpdateNamespacePropertiesResponse updateNamespace(
      IcebergRequestContext context,
      Namespace namespace,
      UpdateNamespacePropertiesRequest updateNamespacePropertiesRequest) {
    validatePropertyUpdate(updateNamespacePropertiesRequest);
    return icebergCatalogWrapperManager
        .getCatalogWrapper(context.catalogName())
        .updateNamespaceProperties(
            namespace,
            IcebergOwnerPropertyGuard.protect(
                updateNamespacePropertiesRequest, context.userName()));
  }

  @Override
  public void dropNamespace(IcebergRequestContext context, Namespace namespace) {
    icebergCatalogWrapperManager.getCatalogWrapper(context.catalogName()).dropNamespace(namespace);
  }

  @Override
  public GetNamespaceResponse loadNamespace(IcebergRequestContext context, Namespace namespace) {
    return icebergCatalogWrapperManager
        .getCatalogWrapper(context.catalogName())
        .loadNamespace(namespace);
  }

  @Override
  public ListNamespacesResponse listNamespaces(
      IcebergRequestContext context, Namespace parentNamespace) {
    return icebergCatalogWrapperManager
        .getCatalogWrapper(context.catalogName())
        .listNamespace(parentNamespace);
  }

  @Override
  public boolean namespaceExists(IcebergRequestContext context, Namespace namespace) {
    return icebergCatalogWrapperManager
        .getCatalogWrapper(context.catalogName())
        .namespaceExists(namespace);
  }

  @Override
  public LoadTableResponse registerTable(
      IcebergRequestContext context,
      Namespace namespace,
      RegisterTableRequest registerTableRequest) {
    TableEntity.NAME.validate(registerTableRequest.name(), Entity.EntityType.TABLE);
    IcebergCleanupHelper.rejectIfBeingPurged(
        cleanupManager, context.catalogName(), namespace, registerTableRequest.name());

    CatalogWrapperForREST catalogWrapper =
        icebergCatalogWrapperManager.getCatalogWrapper(context.catalogName());
    TableMetadata metadata =
        catalogWrapper.loadTableMetadataFromLocation(registerTableRequest.metadataLocation());
    IcebergColumnFieldValidator.validateSchema(metadata.schema());
    return catalogWrapper.registerTable(
        namespace, registerTableRequest, context.requestCredentialVending());
  }

  @Override
  public LoadViewResponse registerView(
      IcebergRequestContext context, Namespace namespace, RegisterViewRequest registerViewRequest) {
    return icebergCatalogWrapperManager
        .getCatalogWrapper(context.catalogName())
        .registerView(namespace, registerViewRequest);
  }

  /**
   * With authorization enabled, a namespace is governed as the Gravitino schema named by its levels
   * joined with the schema separator. A level that contains the separator would give two different
   * namespaces, such as {@code ["a:b"]} and {@code ["a", "b"]}, the same schema, and so the same
   * owner and privileges: whoever creates one would take over the other. Such a level is refused
   * before anything is created.
   */
  @VisibleForTesting
  static void checkLevelsDoNotContainSchemaSeparator(Namespace namespace) {
    if (namespace == null || !isAuthorizationEnabled()) {
      return;
    }
    String separator = HierarchicalSchemaUtil.schemaSeparator();
    for (String level : namespace.levels()) {
      if (level.contains(separator)) {
        throw new BadRequestException(
            "Invalid namespace %s: the level '%s' contains '%s', which separates the levels of a"
                + " nested namespace",
            namespace, level, separator);
      }
    }
  }

  /**
   * Rejects a property update that sets a null value or removes a null key. Iceberg's request
   * builder refuses these, but a request read from JSON skips the builder, and the JDBC catalog
   * would store a null value that makes the namespace unreadable.
   */
  @VisibleForTesting
  static void validatePropertyUpdate(UpdateNamespacePropertiesRequest request) {
    Set<String> nullValues =
        request.updates().entrySet().stream()
            .filter(entry -> entry.getKey() == null || entry.getValue() == null)
            .map(entry -> String.valueOf(entry.getKey()))
            .collect(Collectors.toCollection(TreeSet::new));
    if (!nullValues.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid value to update for properties %s: null. Use remove instead", nullValues));
    }
    if (request.removals().contains(null)) {
      throw new IllegalArgumentException("Invalid property to remove: null");
    }
  }

  private static boolean isAuthorizationEnabled() {
    try {
      return IcebergRESTServerContext.getInstance().isAuthorizationEnabled();
    } catch (IllegalStateException e) {
      // The server context is not initialized, as in a standalone executor.
      return false;
    }
  }
}
