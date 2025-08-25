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
package org.apache.gravitino.hook;

import java.util.List;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.FutureGrantManager;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.exceptions.CatalogAlreadyExistsException;
import org.apache.gravitino.exceptions.CatalogInUseException;
import org.apache.gravitino.exceptions.CatalogNotInUseException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code CatalogHookDispatcher} is a decorator for {@link CatalogDispatcher} that not only
 * delegates catalog operations to the underlying catalog dispatcher but also executes some hook
 * operations before or after the underlying operations.
 */
public class CatalogHookDispatcher implements CatalogDispatcher {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogHookDispatcher.class);
  private final CatalogDispatcher dispatcher;

  public CatalogHookDispatcher(CatalogDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public NameIdentifier[] listCatalogs(Namespace namespace) throws NoSuchMetalakeException {
    return dispatcher.listCatalogs(namespace);
  }

  @Override
  public Catalog[] listCatalogsInfo(Namespace namespace) throws NoSuchMetalakeException {
    return dispatcher.listCatalogsInfo(namespace);
  }

  @Override
  public Catalog loadCatalog(NameIdentifier ident) throws NoSuchCatalogException {
    return dispatcher.loadCatalog(ident);
  }

  @Override
  public Catalog createCatalog(
      NameIdentifier ident,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws NoSuchMetalakeException, CatalogAlreadyExistsException {
    // Check whether the current user exists or not
    AuthorizationUtils.checkCurrentUser(
        ident.namespace().level(0), PrincipalUtils.getCurrentUserName());

    Catalog catalog = dispatcher.createCatalog(ident, type, provider, comment, properties);

    try {
      // Set the creator as the owner of the catalog.
      OwnerDispatcher ownerDispatcher = GravitinoEnv.getInstance().ownerDispatcher();
      if (ownerDispatcher != null) {
        ownerDispatcher.setOwner(
            ident.namespace().level(0),
            NameIdentifierUtil.toMetadataObject(ident, Entity.EntityType.CATALOG),
            PrincipalUtils.getCurrentUserName(),
            Owner.Type.USER);
      }

      // Apply the metalake securable object privileges to authorization plugin
      FutureGrantManager futureGrantManager = GravitinoEnv.getInstance().futureGrantManager();
      if (futureGrantManager != null && catalog instanceof BaseCatalog) {
        futureGrantManager.grantNewlyCreatedCatalog(
            ident.namespace().level(0), (BaseCatalog) catalog);
      }
    } catch (Exception e) {
      LOG.warn("Fail to execute the post hook operations, rollback the catalog " + ident, e);
      dispatcher.dropCatalog(ident, true);
      throw e;
    }

    return catalog;
  }

  @Override
  public Catalog alterCatalog(NameIdentifier ident, CatalogChange... changes)
      throws NoSuchCatalogException, IllegalArgumentException {
    Catalog alteredCatalog = dispatcher.alterCatalog(ident, changes);
    CatalogChange.RenameCatalog lastRenameChange = null;
    for (CatalogChange change : changes) {
      if (change instanceof CatalogChange.RenameCatalog) {
        lastRenameChange = (CatalogChange.RenameCatalog) change;
      }
    }
    if (lastRenameChange != null) {
      AuthorizationUtils.authorizationPluginRenamePrivileges(
          ident, Entity.EntityType.CATALOG, lastRenameChange.getNewName());
    }
    return alteredCatalog;
  }

  @Override
  public boolean dropCatalog(NameIdentifier ident) {
    return dropCatalog(ident, false /* force */);
  }

  @Override
  public boolean dropCatalog(NameIdentifier ident, boolean force)
      throws NonEmptyEntityException, CatalogInUseException {
    if (!dispatcher.catalogExists(ident)) {
      return false;
    }

    Catalog catalog = dispatcher.loadCatalog(ident);

    if (catalog != null) {
      List<String> locations =
          AuthorizationUtils.getMetadataObjectLocation(ident, Entity.EntityType.CATALOG);
      AuthorizationUtils.removeCatalogPrivileges(catalog, locations);
    }

    // We should call the authorization plugin before dropping the catalog, because the dropping
    // catalog will close the authorization plugin.
    return dispatcher.dropCatalog(ident, force);
  }

  @Override
  public void testConnection(
      NameIdentifier ident,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws Exception {
    dispatcher.testConnection(ident, type, provider, comment, properties);
  }

  @Override
  public void enableCatalog(NameIdentifier ident)
      throws NoSuchCatalogException, CatalogNotInUseException {
    dispatcher.enableCatalog(ident);
  }

  @Override
  public void disableCatalog(NameIdentifier ident) throws NoSuchCatalogException {
    dispatcher.disableCatalog(ident);
  }

  @Override
  public boolean catalogExists(NameIdentifier ident) {
    return dispatcher.catalogExists(ident);
  }
}
