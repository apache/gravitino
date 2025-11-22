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

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.credential.CredentialPrivilege;
import org.apache.gravitino.iceberg.common.utils.IcebergIdentifierUtils;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergTableOperationExecutor implements IcebergTableOperationDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergTableOperationExecutor.class);

  private IcebergCatalogWrapperManager icebergCatalogWrapperManager;

  public IcebergTableOperationExecutor(IcebergCatalogWrapperManager icebergCatalogWrapperManager) {
    this.icebergCatalogWrapperManager = icebergCatalogWrapperManager;
  }

  @Override
  public LoadTableResponse createTable(
      IcebergRequestContext context, Namespace namespace, CreateTableRequest createTableRequest) {
    String authenticatedUser = context.userName();
    if (!AuthConstants.ANONYMOUS_USER.equals(authenticatedUser)) {
      String existingOwner = createTableRequest.properties().get(IcebergConstants.OWNER);

      // Override the owner as the authenticated user if different from authenticated user
      if (!authenticatedUser.equals(existingOwner)) {
        Map<String, String> properties = new HashMap<>(createTableRequest.properties());
        properties.put(IcebergConstants.OWNER, authenticatedUser);
        LOG.debug(
            "Overriding table owner from '{}' to authenticated user: '{}'",
            existingOwner,
            authenticatedUser);

        // CreateTableRequest is immutable, so we need to rebuild it with modified properties
        createTableRequest =
            CreateTableRequest.builder()
                .withName(createTableRequest.name())
                .withSchema(createTableRequest.schema())
                .withPartitionSpec(createTableRequest.spec())
                .withWriteOrder(createTableRequest.writeOrder())
                .withLocation(createTableRequest.location())
                .setProperties(properties)
                .build();
      }
    }

    return icebergCatalogWrapperManager
        .getCatalogWrapper(context.catalogName())
        .createTable(namespace, createTableRequest, context.requestCredentialVending());
  }

  @Override
  public LoadTableResponse updateTable(
      IcebergRequestContext context,
      TableIdentifier tableIdentifier,
      UpdateTableRequest updateTableRequest) {
    return icebergCatalogWrapperManager
        .getCatalogWrapper(context.catalogName())
        .updateTable(tableIdentifier, updateTableRequest);
  }

  @Override
  public void dropTable(
      IcebergRequestContext context, TableIdentifier tableIdentifier, boolean purgeRequested) {
    if (purgeRequested) {
      icebergCatalogWrapperManager
          .getCatalogWrapper(context.catalogName())
          .purgeTable(tableIdentifier);
    } else {
      icebergCatalogWrapperManager
          .getCatalogWrapper(context.catalogName())
          .dropTable(tableIdentifier);
    }
  }

  @Override
  public LoadTableResponse loadTable(
      IcebergRequestContext context, TableIdentifier tableIdentifier) {
    CredentialPrivilege privilege = CredentialPrivilege.READ;
    if (context.requestCredentialVending()) {
      privilege = getCredentialPrivilege(context, tableIdentifier);
    }

    return icebergCatalogWrapperManager
        .getCatalogWrapper(context.catalogName())
        .loadTable(tableIdentifier, context.requestCredentialVending(), privilege);
  }

  @Override
  public ListTablesResponse listTable(IcebergRequestContext context, Namespace namespace) {
    return icebergCatalogWrapperManager
        .getCatalogWrapper(context.catalogName())
        .listTable(namespace);
  }

  @Override
  public boolean tableExists(IcebergRequestContext context, TableIdentifier tableIdentifier) {
    return icebergCatalogWrapperManager
        .getCatalogWrapper(context.catalogName())
        .tableExists(tableIdentifier);
  }

  @Override
  public void renameTable(IcebergRequestContext context, RenameTableRequest renameTableRequest) {
    icebergCatalogWrapperManager
        .getCatalogWrapper(context.catalogName())
        .renameTable(renameTableRequest);
  }

  @Override
  public LoadCredentialsResponse getTableCredentials(
      IcebergRequestContext context, TableIdentifier tableIdentifier) {
    CredentialPrivilege privilege = getCredentialPrivilege(context, tableIdentifier);
    return icebergCatalogWrapperManager
        .getCatalogWrapper(context.catalogName())
        .getTableCredentials(tableIdentifier, privilege);
  }

  private static CredentialPrivilege getCredentialPrivilege(
      IcebergRequestContext context, TableIdentifier tableIdentifier) {
    String metalake = IcebergRESTServerContext.getInstance().metalakeName();
    NameIdentifier identifier =
        IcebergIdentifierUtils.toGravitinoTableIdentifier(
            metalake, context.catalogName(), tableIdentifier);
    boolean writable =
        MetadataAuthzHelper.checkAccess(
            identifier,
            Entity.EntityType.TABLE,
            AuthorizationExpressionConstants.filterModifyTableAuthorizationExpression);

    return writable ? CredentialPrivilege.WRITE : CredentialPrivilege.READ;
  }
}
