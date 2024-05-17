/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener;

import com.datastrato.gravitino.catalog.Catalog;
import com.datastrato.gravitino.CatalogBasic;
import com.datastrato.gravitino.CatalogChange;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.CatalogDispatcher;
import com.datastrato.gravitino.exceptions.CatalogAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.listener.api.event.AlterCatalogEvent;
import com.datastrato.gravitino.listener.api.event.AlterCatalogFailureEvent;
import com.datastrato.gravitino.listener.api.event.CreateCatalogEvent;
import com.datastrato.gravitino.listener.api.event.CreateCatalogFailureEvent;
import com.datastrato.gravitino.listener.api.event.DropCatalogEvent;
import com.datastrato.gravitino.listener.api.event.DropCatalogFailureEvent;
import com.datastrato.gravitino.listener.api.event.ListCatalogEvent;
import com.datastrato.gravitino.listener.api.event.ListCatalogFailureEvent;
import com.datastrato.gravitino.listener.api.event.LoadCatalogEvent;
import com.datastrato.gravitino.listener.api.event.LoadCatalogFailureEvent;
import com.datastrato.gravitino.listener.api.info.CatalogInfo;
import com.datastrato.gravitino.utils.PrincipalUtils;
import java.util.Map;

/**
 * {@code CatalogEventDispatcher} is a decorator for {@link CatalogDispatcher} that not only
 * delegates catalog operations to the underlying catalog dispatcher but also dispatches
 * corresponding events to an {@link com.datastrato.gravitino.listener.EventBus} after each
 * operation is completed. This allows for event-driven workflows or monitoring of catalog
 * operations.
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
      CatalogBasic.Type type,
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
  public boolean dropCatalog(NameIdentifier ident) {
    try {
      boolean isExists = dispatcher.dropCatalog(ident);
      eventBus.dispatchEvent(
          new DropCatalogEvent(PrincipalUtils.getCurrentUserName(), ident, isExists));
      return isExists;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new DropCatalogFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e));
      throw e;
    }
  }
}
