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

package org.apache.gravitino.listener;

import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.exceptions.CatalogAlreadyExistsException;
import org.apache.gravitino.exceptions.CatalogInUseException;
import org.apache.gravitino.exceptions.CatalogNotInUseException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.listener.api.event.AlterCatalogEvent;
import org.apache.gravitino.listener.api.event.AlterCatalogFailureEvent;
import org.apache.gravitino.listener.api.event.CreateCatalogEvent;
import org.apache.gravitino.listener.api.event.CreateCatalogFailureEvent;
import org.apache.gravitino.listener.api.event.DropCatalogEvent;
import org.apache.gravitino.listener.api.event.DropCatalogFailureEvent;
import org.apache.gravitino.listener.api.event.ListCatalogEvent;
import org.apache.gravitino.listener.api.event.ListCatalogFailureEvent;
import org.apache.gravitino.listener.api.event.LoadCatalogEvent;
import org.apache.gravitino.listener.api.event.LoadCatalogFailureEvent;
import org.apache.gravitino.listener.api.info.CatalogInfo;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * {@code CatalogEventDispatcher} is a decorator for {@link CatalogDispatcher} that not only
 * delegates catalog operations to the underlying catalog dispatcher but also dispatches
 * corresponding events to an {@link org.apache.gravitino.listener.EventBus} after each operation is
 * completed. This allows for event-driven workflows or monitoring of catalog operations.
 */
public class CatalogEventDispatcher implements CatalogDispatcher {
  private final EventBus eventBus;
  private final CatalogDispatcher dispatcher;

  /**
   * Constructs a CatalogEventDispatcher with a specified EventBus and CatalogDispatcher.
   *
   * @param eventBus The EventBus to which events will be dispatched.
   * @param dispatcher The underlying {@link CatalogDispatcher} that will perform the actual catalog
   *     operations.
   */
  public CatalogEventDispatcher(EventBus eventBus, CatalogDispatcher dispatcher) {
    this.eventBus = eventBus;
    this.dispatcher = dispatcher;
  }

  @Override
  public NameIdentifier[] listCatalogs(Namespace namespace) throws NoSuchMetalakeException {
    try {
      NameIdentifier[] nameIdentifiers = dispatcher.listCatalogs(namespace);
      eventBus.dispatchEvent(new ListCatalogEvent(PrincipalUtils.getCurrentUserName(), namespace));
      return nameIdentifiers;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new ListCatalogFailureEvent(PrincipalUtils.getCurrentUserName(), e, namespace));
      throw e;
    }
  }

  @Override
  public Catalog[] listCatalogsInfo(Namespace namespace) throws NoSuchMetalakeException {
    try {
      Catalog[] catalogs = dispatcher.listCatalogsInfo(namespace);
      eventBus.dispatchEvent(new ListCatalogEvent(PrincipalUtils.getCurrentUserName(), namespace));
      return catalogs;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new ListCatalogFailureEvent(PrincipalUtils.getCurrentUserName(), e, namespace));
      throw e;
    }
  }

  @Override
  public Catalog loadCatalog(NameIdentifier ident) throws NoSuchCatalogException {
    try {
      Catalog catalog = dispatcher.loadCatalog(ident);
      eventBus.dispatchEvent(
          new LoadCatalogEvent(
              PrincipalUtils.getCurrentUserName(), ident, new CatalogInfo(catalog)));
      return catalog;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new LoadCatalogFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e));
      throw e;
    }
  }

  @Override
  public Catalog createCatalog(
      NameIdentifier ident,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws NoSuchMetalakeException, CatalogAlreadyExistsException {
    try {
      Catalog catalog = dispatcher.createCatalog(ident, type, provider, comment, properties);
      eventBus.dispatchEvent(
          new CreateCatalogEvent(
              PrincipalUtils.getCurrentUserName(), ident, new CatalogInfo(catalog)));
      return catalog;
    } catch (Exception e) {
      CatalogInfo createCatalogRequest =
          new CatalogInfo(ident.name(), type, provider, comment, properties, null);
      eventBus.dispatchEvent(
          new CreateCatalogFailureEvent(
              PrincipalUtils.getCurrentUserName(), ident, e, createCatalogRequest));
      throw e;
    }
  }

  @Override
  public Catalog alterCatalog(NameIdentifier ident, CatalogChange... changes)
      throws NoSuchCatalogException, IllegalArgumentException {
    try {
      Catalog catalog = dispatcher.alterCatalog(ident, changes);
      eventBus.dispatchEvent(
          new AlterCatalogEvent(
              PrincipalUtils.getCurrentUserName(), ident, changes, new CatalogInfo(catalog)));
      return catalog;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new AlterCatalogFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e, changes));
      throw e;
    }
  }

  @Override
  public boolean dropCatalog(NameIdentifier ident, boolean force)
      throws NonEmptyEntityException, CatalogInUseException {
    try {
      boolean isExists = dispatcher.dropCatalog(ident, force);
      eventBus.dispatchEvent(
          new DropCatalogEvent(PrincipalUtils.getCurrentUserName(), ident, isExists));
      return isExists;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new DropCatalogFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e));
      throw e;
    }
  }

  @Override
  public void testConnection(
      NameIdentifier ident,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws Exception {
    // TODO: Support event dispatching for testConnection
    dispatcher.testConnection(ident, type, provider, comment, properties);
  }

  @Override
  public void enableCatalog(NameIdentifier ident)
      throws NoSuchCatalogException, CatalogNotInUseException {
    // todo: support enable catalog event
    dispatcher.enableCatalog(ident);
  }

  @Override
  public void disableCatalog(NameIdentifier ident) throws NoSuchCatalogException {
    // todo: support disable catalog event
    dispatcher.disableCatalog(ident);
  }
}
