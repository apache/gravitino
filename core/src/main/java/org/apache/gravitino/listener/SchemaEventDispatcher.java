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
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.catalog.SchemaOperationDispatcher;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.listener.api.event.AlterSchemaEvent;
import org.apache.gravitino.listener.api.event.AlterSchemaFailureEvent;
import org.apache.gravitino.listener.api.event.CreateSchemaEvent;
import org.apache.gravitino.listener.api.event.CreateSchemaFailureEvent;
import org.apache.gravitino.listener.api.event.DropSchemaEvent;
import org.apache.gravitino.listener.api.event.DropSchemaFailureEvent;
import org.apache.gravitino.listener.api.event.ListSchemaEvent;
import org.apache.gravitino.listener.api.event.ListSchemaFailureEvent;
import org.apache.gravitino.listener.api.event.LoadSchemaEvent;
import org.apache.gravitino.listener.api.event.LoadSchemaFailureEvent;
import org.apache.gravitino.listener.api.info.SchemaInfo;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * {@code SchemaEventDispatcher} is a decorator for {@link SchemaDispatcher} that not only delegates
 * schema operations to the underlying schema dispatcher but also dispatches corresponding events to
 * an {@link org.apache.gravitino.listener.EventBus} after each operation is completed. This allows
 * for event-driven workflows or monitoring of schema operations.
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
