/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.Schema;
import com.datastrato.gravitino.SchemaChange;
import com.datastrato.gravitino.catalog.SchemaDispatcher;
import com.datastrato.gravitino.catalog.SchemaOperationDispatcher;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NonEmptySchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.listener.api.event.AlterSchemaEvent;
import com.datastrato.gravitino.listener.api.event.AlterSchemaFailureEvent;
import com.datastrato.gravitino.listener.api.event.CreateSchemaEvent;
import com.datastrato.gravitino.listener.api.event.CreateSchemaFailureEvent;
import com.datastrato.gravitino.listener.api.event.DropSchemaEvent;
import com.datastrato.gravitino.listener.api.event.DropSchemaFailureEvent;
import com.datastrato.gravitino.listener.api.event.ListSchemaEvent;
import com.datastrato.gravitino.listener.api.event.ListSchemaFailureEvent;
import com.datastrato.gravitino.listener.api.event.LoadSchemaEvent;
import com.datastrato.gravitino.listener.api.event.LoadSchemaFailureEvent;
import com.datastrato.gravitino.listener.api.info.SchemaInfo;
import com.datastrato.gravitino.utils.PrincipalUtils;
import java.util.Map;

/**
 * {@code SchemaEventDispatcher} is a decorator for {@link SchemaDispatcher} that not only delegates
 * schema operations to the underlying schema dispatcher but also dispatches corresponding events to
 * an {@link com.datastrato.gravitino.listener.EventBus} after each operation is completed. This
 * allows for event-driven workflows or monitoring of schema operations.
 */
public class SchemaEventDispatcher implements SchemaDispatcher {
  private final EventBus eventBus;
  private final SchemaDispatcher dispatcher;

  /**
   * Constructs a SchemaEventDispatcher with a specified EventBus and SchemaDispatcher.
   *
   * @param eventBus The EventBus to which events will be dispatched.
   * @param dispatcher The underlying {@link SchemaOperationDispatcher} that will perform the actual
   *     schema operations.
   */
  public SchemaEventDispatcher(EventBus eventBus, SchemaDispatcher dispatcher) {
    this.eventBus = eventBus;
    this.dispatcher = dispatcher;
  }

  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    try {
      NameIdentifier[] nameIdentifiers = dispatcher.listSchemas(namespace);
      eventBus.dispatchEvent(new ListSchemaEvent(PrincipalUtils.getCurrentUserName(), namespace));
      return nameIdentifiers;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new ListSchemaFailureEvent(PrincipalUtils.getCurrentUserName(), namespace, e));
      throw e;
    }
  }

  @Override
  public boolean schemaExists(NameIdentifier ident) {
    return dispatcher.schemaExists(ident);
  }

  @Override
  public Schema createSchema(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    try {
      Schema schema = dispatcher.createSchema(ident, comment, properties);
      eventBus.dispatchEvent(
          new CreateSchemaEvent(
              PrincipalUtils.getCurrentUserName(), ident, new SchemaInfo(schema)));
      return schema;
    } catch (Exception e) {
      SchemaInfo createSchemaRequest = new SchemaInfo(ident.name(), comment, properties, null);
      eventBus.dispatchEvent(
          new CreateSchemaFailureEvent(
              PrincipalUtils.getCurrentUserName(), ident, e, createSchemaRequest));
      throw e;
    }
  }

  @Override
  public Schema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    try {
      Schema schema = dispatcher.loadSchema(ident);
      eventBus.dispatchEvent(
          new LoadSchemaEvent(PrincipalUtils.getCurrentUserName(), ident, new SchemaInfo(schema)));
      return schema;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new LoadSchemaFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e));
      throw e;
    }
  }

  @Override
  public Schema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    try {
      Schema schema = dispatcher.alterSchema(ident, changes);
      eventBus.dispatchEvent(
          new AlterSchemaEvent(
              PrincipalUtils.getCurrentUserName(), ident, changes, new SchemaInfo(schema)));
      return schema;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new AlterSchemaFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e, changes));
      throw e;
    }
  }

  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    try {
      boolean isExists = dispatcher.dropSchema(ident, cascade);
      eventBus.dispatchEvent(
          new DropSchemaEvent(PrincipalUtils.getCurrentUserName(), ident, isExists, cascade));
      return isExists;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new DropSchemaFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e, cascade));
      throw e;
    }
  }
}
