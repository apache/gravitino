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
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.iceberg.common.utils.IcebergIdentifierUtils;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;

/**
 * {@code IcebergSchemaHookDispatcher} is a decorator for {@link
 * IcebergNamespaceOperationDispatcher} that not only delegates namespace operations to the
 * underlying dispatcher but also executes some hook operations before or after the underlying
 * operations.
 */
public class IcebergNamespaceHookDispatcher implements IcebergNamespaceOperationDispatcher {
  private final IcebergNamespaceOperationDispatcher dispatcher;
  private final String metalake;

  public IcebergNamespaceHookDispatcher(IcebergNamespaceOperationDispatcher dispatcher) {
    this.dispatcher = dispatcher;
    this.metalake = IcebergRESTServerContext.getInstance().metalakeName();
  }

  @Override
  public CreateNamespaceResponse createNamespace(
      IcebergRequestContext context, CreateNamespaceRequest createRequest) {
    CreateNamespaceResponse response = dispatcher.createNamespace(context, createRequest);

    importSchema(context.catalogName(), createRequest.namespace());
    IcebergOwnershipUtils.setSchemaOwner(
        metalake,
        context.catalogName(),
        createRequest.namespace(),
        context.userName(),
        GravitinoEnv.getInstance().ownerDispatcher());

    return response;
  }

  @Override
  public UpdateNamespacePropertiesResponse updateNamespace(
      IcebergRequestContext context,
      Namespace namespace,
      UpdateNamespacePropertiesRequest updateRequest) {
    return dispatcher.updateNamespace(context, namespace, updateRequest);
  }

  @Override
  public void dropNamespace(IcebergRequestContext context, Namespace namespace) {
    dispatcher.dropNamespace(context, namespace);

    // Clean up the schema from Gravitino's entity store after successful drop
    EntityStore store = GravitinoEnv.getInstance().entityStore();
    try {
      if (store != null) {
        // Delete the entity for the dropped namespace (schema).
        store.delete(
            IcebergIdentifierUtils.toGravitinoSchemaIdentifier(
                metalake, context.catalogName(), namespace),
            Entity.EntityType.SCHEMA);
      }
    } catch (NoSuchEntityException ignore) {
      // Ignore if the schema entity does not exist.
    } catch (IOException ioe) {
      throw new RuntimeException("io exception when deleting schema entity", ioe);
    }
  }

  @Override
  public GetNamespaceResponse loadNamespace(IcebergRequestContext context, Namespace namespace) {
    return dispatcher.loadNamespace(context, namespace);
  }

  @Override
  public ListNamespacesResponse listNamespaces(
      IcebergRequestContext context, Namespace parentNamespace) {
    return dispatcher.listNamespaces(context, parentNamespace);
  }

  @Override
  public boolean namespaceExists(IcebergRequestContext context, Namespace namespace) {
    return dispatcher.namespaceExists(context, namespace);
  }

  @Override
  public LoadTableResponse registerTable(
      IcebergRequestContext context,
      Namespace namespace,
      RegisterTableRequest registerTableRequest) {
    LoadTableResponse response = dispatcher.registerTable(context, namespace, registerTableRequest);

    // Import the registered table into Gravitino's catalog so it exists as a metadata object
    importTable(context.catalogName(), namespace, registerTableRequest.name());

    // Set the owner of the registered table to the current user
    IcebergOwnershipUtils.setTableOwner(
        metalake,
        context.catalogName(),
        namespace,
        registerTableRequest.name(),
        context.userName(),
        GravitinoEnv.getInstance().ownerDispatcher());

    return response;
  }

  private void importTable(String catalogName, Namespace namespace, String tableName) {
    TableDispatcher tableDispatcher = GravitinoEnv.getInstance().tableDispatcher();
    if (tableDispatcher != null) {
      tableDispatcher.loadTable(
          IcebergIdentifierUtils.toGravitinoTableIdentifier(
              metalake, catalogName, TableIdentifier.of(namespace, tableName)));
    }
  }

  private void importSchema(String catalogName, Namespace namespace) {
    SchemaDispatcher schemaDispatcher = GravitinoEnv.getInstance().schemaDispatcher();
    if (schemaDispatcher != null) {
      schemaDispatcher.loadSchema(
          IcebergIdentifierUtils.toGravitinoSchemaIdentifier(metalake, catalogName, namespace));
    }
  }
}
