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

import java.io.IOException;
import java.time.Instant;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.iceberg.common.utils.IcebergIdentifierUtils;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;

public class IcebergTableHookDispatcher implements IcebergTableOperationDispatcher {

  private final IcebergTableOperationDispatcher dispatcher;
  private String metalake;

  public IcebergTableHookDispatcher(IcebergTableOperationDispatcher dispatcher) {
    this.dispatcher = dispatcher;
    this.metalake = IcebergRESTServerContext.getInstance().metalakeName();
  }

  @Override
  public LoadTableResponse createTable(
      IcebergRequestContext context, Namespace namespace, CreateTableRequest createTableRequest) {
    AuthorizationUtils.checkCurrentUser(metalake, context.userName());

    LoadTableResponse response = dispatcher.createTable(context, namespace, createTableRequest);
    importTable(context.catalogName(), namespace, createTableRequest.name());
    IcebergOwnershipUtils.setTableOwner(
        metalake,
        context.catalogName(),
        namespace,
        createTableRequest.name(),
        context.userName(),
        GravitinoEnv.getInstance().ownerDispatcher());

    return response;
  }

  @Override
  public LoadTableResponse updateTable(
      IcebergRequestContext context,
      TableIdentifier tableIdentifier,
      UpdateTableRequest updateTableRequest) {
    return dispatcher.updateTable(context, tableIdentifier, updateTableRequest);
  }

  @Override
  public void dropTable(
      IcebergRequestContext context, TableIdentifier tableIdentifier, boolean purgeRequested) {
    dispatcher.dropTable(context, tableIdentifier, purgeRequested);
    EntityStore store = GravitinoEnv.getInstance().entityStore();
    try {
      if (store != null) {
        // Delete the entity for the dropped table.
        store.delete(
            IcebergIdentifierUtils.toGravitinoTableIdentifier(
                metalake, context.catalogName(), tableIdentifier),
            Entity.EntityType.TABLE);
      }
    } catch (NoSuchEntityException ignore) {
      // Ignore if the table entity does not exist.
    } catch (IOException ioe) {
      throw new RuntimeException("io exception when deleting table entity", ioe);
    }
  }

  @Override
  public LoadTableResponse loadTable(
      IcebergRequestContext context, TableIdentifier tableIdentifier) {
    return dispatcher.loadTable(context, tableIdentifier);
  }

  @Override
  public ListTablesResponse listTable(IcebergRequestContext context, Namespace namespace) {
    return dispatcher.listTable(context, namespace);
  }

  @Override
  public boolean tableExists(IcebergRequestContext context, TableIdentifier tableIdentifier) {
    return dispatcher.tableExists(context, tableIdentifier);
  }

  @Override
  public void renameTable(IcebergRequestContext context, RenameTableRequest renameTableRequest) {
    AuthorizationUtils.checkCurrentUser(metalake, context.userName());

    dispatcher.renameTable(context, renameTableRequest);
    NameIdentifier tableSource =
        IcebergIdentifierUtils.toGravitinoTableIdentifier(
            metalake, context.catalogName(), renameTableRequest.source());
    NameIdentifier tableDest =
        IcebergIdentifierUtils.toGravitinoTableIdentifier(
            metalake, context.catalogName(), renameTableRequest.destination());
    EntityStore store = GravitinoEnv.getInstance().entityStore();
    try {
      if (store != null) {
        // Update the entity for the destination table.
        store.update(
            tableSource,
            TableEntity.class,
            Entity.EntityType.TABLE,
            tableEntity ->
                TableEntity.builder()
                    .withId(tableEntity.id())
                    .withName(tableDest.name())
                    .withNamespace(tableDest.namespace())
                    .withColumns(tableEntity.columns())
                    .withAuditInfo(
                        AuditInfo.builder()
                            .withCreator(tableEntity.auditInfo().creator())
                            .withCreateTime(tableEntity.auditInfo().createTime())
                            .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                            .withLastModifiedTime(Instant.now())
                            .build())
                    .build());
      }
    } catch (NoSuchEntityException ignore) {
      // Ignore if the source table entity does not exist.
    } catch (IOException ioe) {
      throw new RuntimeException("io exception when renaming table entity", ioe);
    }
  }

  @Override
  public LoadCredentialsResponse getTableCredentials(
      IcebergRequestContext context, TableIdentifier tableIdentifier) {
    return dispatcher.getTableCredentials(context, tableIdentifier);
  }

  private void importTable(String catalogName, Namespace namespace, String tableName) {
    TableDispatcher tableDispatcher = GravitinoEnv.getInstance().tableDispatcher();
    if (tableDispatcher != null) {
      tableDispatcher.loadTable(
          IcebergIdentifierUtils.toGravitinoTableIdentifier(
              metalake, catalogName, TableIdentifier.of(namespace, tableName)));
    }
  }
}
