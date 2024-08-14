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

import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerManager;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.exceptions.CatalogAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * {@code CatalogHookDispatcher} is a decorator for {@link CatalogDispatcher} that not only
 * delegates catalog operations to the underlying catalog dispatcher but also executes some hook
 * operations before or after the underlying operations.
 */
public class CatalogHookDispatcher implements CatalogDispatcher {
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
    Catalog catalog = dispatcher.createCatalog(ident, type, provider, comment, properties);

    // Set the creator as the owner of the catalog.
    OwnerManager ownerManager = GravitinoEnv.getInstance().ownerManager();
    if (ownerManager != null) {
      ownerManager.setOwner(
          ident.namespace().level(0),
          NameIdentifierUtil.toMetadataObject(ident, Entity.EntityType.CATALOG),
          PrincipalUtils.getCurrentUserName(),
          Owner.Type.USER);
    }
    return catalog;
  }

  @Override
  public Catalog alterCatalog(NameIdentifier ident, CatalogChange... changes)
      throws NoSuchCatalogException, IllegalArgumentException {
    return dispatcher.alterCatalog(ident, changes);
  }

  @Override
  public boolean dropCatalog(NameIdentifier ident) {
    return dispatcher.dropCatalog(ident);
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
  public boolean catalogExists(NameIdentifier ident) {
    return dispatcher.catalogExists(ident);
  }
}
