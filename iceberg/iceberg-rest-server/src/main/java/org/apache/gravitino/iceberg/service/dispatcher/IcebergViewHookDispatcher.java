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

import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.catalog.ViewDispatcher;
import org.apache.gravitino.iceberg.common.utils.IcebergIdentifierUtils;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code IcebergViewHookDispatcher} is a decorator for {@link IcebergViewOperationDispatcher} that
 * imports views into Gravitino's metadata catalog when they are created or accessed via Iceberg
 * REST.
 *
 * <p>This is the key integration point between Iceberg REST views and Gravitino's view management
 * system. When a view is created or loaded through Iceberg REST, this dispatcher ensures that
 * Gravitino is aware of the view by calling {@link ViewDispatcher#loadView}.
 */
public class IcebergViewHookDispatcher implements IcebergViewOperationDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergViewHookDispatcher.class);

  private final IcebergViewOperationDispatcher dispatcher;
  private final String metalake;

  public IcebergViewHookDispatcher(IcebergViewOperationDispatcher dispatcher, String metalake) {
    this.dispatcher = dispatcher;
    this.metalake = metalake;
  }

  @Override
  public LoadViewResponse createView(
      IcebergRequestContext context, Namespace namespace, CreateViewRequest createViewRequest) {
    // First, create the view in the underlying catalog
    LoadViewResponse response = dispatcher.createView(context, namespace, createViewRequest);

    // Then import it into Gravitino so Gravitino is aware of the view
    importView(context.catalogName(), namespace, createViewRequest.name());

    // TODO(#9746): Enable view ownership once ViewMetaService is implemented
    // Currently disabled because VIEW entity type is not supported in
    // RelationalEntityStoreIdResolver
    // IcebergOwnershipUtils.setViewOwner(
    //     metalake,
    //     context.catalogName(),
    //     namespace,
    //     createViewRequest.name(),
    //     context.userName(),
    //     GravitinoEnv.getInstance().ownerDispatcher());

    return response;
  }

  @Override
  public LoadViewResponse loadView(IcebergRequestContext context, TableIdentifier viewIdentifier) {
    return dispatcher.loadView(context, viewIdentifier);
  }

  @Override
  public LoadViewResponse replaceView(
      IcebergRequestContext context,
      TableIdentifier viewIdentifier,
      UpdateTableRequest replaceViewRequest) {
    return dispatcher.replaceView(context, viewIdentifier, replaceViewRequest);
  }

  @Override
  public void dropView(IcebergRequestContext context, TableIdentifier viewIdentifier) {
    dispatcher.dropView(context, viewIdentifier);
    // Note: We don't remove from Gravitino here as that will be handled by entity storage
    // in future work (issue #9746)
  }

  @Override
  public ListTablesResponse listView(IcebergRequestContext context, Namespace namespace) {
    return dispatcher.listView(context, namespace);
  }

  @Override
  public boolean viewExists(IcebergRequestContext context, TableIdentifier viewIdentifier) {
    return dispatcher.viewExists(context, viewIdentifier);
  }

  @Override
  public void renameView(IcebergRequestContext context, RenameTableRequest renameViewRequest) {
    dispatcher.renameView(context, renameViewRequest);
    // Note: Rename handling in Gravitino will be added with full view support (issue #9746)
  }

  /**
   * Import a view into Gravitino's metadata catalog by loading it through the ViewDispatcher.
   *
   * <p>This method calls {@link ViewDispatcher#loadView} which delegates to the underlying
   * catalog's ViewCatalog implementation. This ensures Gravitino is aware of the view and can apply
   * authorization policies to it.
   *
   * <p>This is a best-effort operation - if it fails, we log a warning but don't fail the Iceberg
   * REST operation, as the view was successfully created in the underlying catalog.
   *
   * @param catalogName The name of the Gravitino catalog.
   * @param namespace The Iceberg namespace containing the view.
   * @param viewName The name of the view.
   */
  private void importView(String catalogName, Namespace namespace, String viewName) {
    ViewDispatcher viewDispatcher = GravitinoEnv.getInstance().viewDispatcher();
    if (viewDispatcher != null) {
      try {
        viewDispatcher.loadView(
            IcebergIdentifierUtils.toGravitinoTableIdentifier(
                metalake, catalogName, TableIdentifier.of(namespace, viewName)));
        LOG.info(
            "Successfully imported view into Gravitino: {}.{}.{}.{}",
            metalake,
            catalogName,
            namespace,
            viewName);
      } catch (Exception e) {
        // Log but don't fail - view import is best-effort
        LOG.warn(
            "Failed to import view into Gravitino: {}.{}.{}.{} - {}",
            metalake,
            catalogName,
            namespace,
            viewName,
            e.getMessage());
      }
    }
  }
}
