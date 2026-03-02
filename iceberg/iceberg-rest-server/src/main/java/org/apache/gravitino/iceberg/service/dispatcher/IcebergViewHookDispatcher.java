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

import com.google.common.base.Preconditions;
import java.io.IOException;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.ViewDispatcher;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.iceberg.common.utils.IcebergIdentifierUtils;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.gravitino.meta.GenericEntity;
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
    Preconditions.checkArgument(dispatcher != null, "dispatcher must not be null");
    Preconditions.checkArgument(metalake != null, "metalake must not be null");
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

    // Set ownership for the newly created view
    IcebergOwnershipUtils.setViewOwner(
        metalake,
        context.catalogName(),
        namespace,
        createViewRequest.name(),
        context.userName(),
        GravitinoEnv.getInstance().ownerDispatcher());

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

    // Remove view from Gravitino entity store
    EntityStore store = GravitinoEnv.getInstance().entityStore();
    try {
      if (store != null) {
        store.delete(
            IcebergIdentifierUtils.toGravitinoTableIdentifier(
                metalake, context.catalogName(), viewIdentifier),
            Entity.EntityType.VIEW);
        LOG.info(
            "Successfully removed view from Gravitino entity store: {}.{}.{}.{}",
            metalake,
            context.catalogName(),
            viewIdentifier.namespace(),
            viewIdentifier.name());
      }
    } catch (NoSuchEntityException ignore) {
      // Ignore if the view entity does not exist in the store
      LOG.debug(
          "View entity does not exist in store: {}.{}.{}.{}",
          metalake,
          context.catalogName(),
          viewIdentifier.namespace(),
          viewIdentifier.name());
    } catch (IOException ioe) {
      LOG.error(
          "Failed to delete view entity from store: {}.{}.{}.{}",
          metalake,
          context.catalogName(),
          viewIdentifier.namespace(),
          viewIdentifier.name(),
          ioe);
      throw new RuntimeException("Failed to delete view entity from store", ioe);
    }
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

    // Update view in Gravitino entity store with new name
    NameIdentifier sourceIdent =
        IcebergIdentifierUtils.toGravitinoTableIdentifier(
            metalake, context.catalogName(), renameViewRequest.source());
    NameIdentifier destIdent =
        IcebergIdentifierUtils.toGravitinoTableIdentifier(
            metalake, context.catalogName(), renameViewRequest.destination());

    EntityStore store = GravitinoEnv.getInstance().entityStore();
    try {
      if (store != null) {
        store.update(
            sourceIdent,
            GenericEntity.class,
            Entity.EntityType.VIEW,
            viewEntity ->
                GenericEntity.builder()
                    .withId(viewEntity.id())
                    .withName(destIdent.name())
                    .withNamespace(destIdent.namespace())
                    .withEntityType(Entity.EntityType.VIEW)
                    .build());
        LOG.info(
            "Successfully renamed view in Gravitino entity store from {} to {}",
            sourceIdent,
            destIdent);
      }
    } catch (NoSuchEntityException ignore) {
      // Ignore if the source view entity does not exist in the store
      LOG.debug("Source view entity does not exist in store: {}", sourceIdent);
    } catch (IOException ioe) {
      LOG.error("Failed to rename view entity in store from {} to {}", sourceIdent, destIdent, ioe);
      throw new RuntimeException("Failed to rename view entity in store", ioe);
    }
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
