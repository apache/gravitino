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
import org.apache.gravitino.catalog.ModelDispatcher;
import org.apache.gravitino.exceptions.ModelAlreadyExistsException;
import org.apache.gravitino.exceptions.ModelVersionAliasesAlreadyExistException;
import org.apache.gravitino.exceptions.NoSuchModelException;
import org.apache.gravitino.exceptions.NoSuchModelVersionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.listener.api.event.DeleteModelPreEvent;
import org.apache.gravitino.listener.api.event.DeleteModelVersionPreEvent;
import org.apache.gravitino.listener.api.event.GetModelPreEvent;
import org.apache.gravitino.listener.api.event.GetModelVersionPreEvent;
import org.apache.gravitino.listener.api.event.LinkModelVersionPreEvent;
import org.apache.gravitino.listener.api.event.ListModelPreEvent;
import org.apache.gravitino.listener.api.event.ListModelVersionPreEvent;
import org.apache.gravitino.listener.api.event.RegisterAndLinkModelPreEvent;
import org.apache.gravitino.listener.api.event.RegisterModelPreEvent;
import org.apache.gravitino.listener.api.info.ModelInfo;
import org.apache.gravitino.listener.api.info.ModelVersionInfo;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.model.ModelVersion;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * {@code ModelEventDispatcher} is a decorator for {@link ModelDispatcher} that not only delegates
 * model operations to the underlying catalog dispatcher but also dispatches corresponding events to
 * an {@link org.apache.gravitino.listener.EventBus} after each operation is completed. This allows
 * for event-driven workflows or monitoring of model operations.
 */
public class ModelEventDispatcher implements ModelDispatcher {
  private final EventBus eventBus;
  private final ModelDispatcher dispatcher;

  /**
   * Constructs a {@link ModelEventDispatcher} with a specified EventBus and {@link
   * ModelDispatcher}.
   *
   * @param eventBus The EventBus to which events will be dispatched.
   * @param dispatcher The underlying {@link ModelDispatcher} that will perform the actual model
   *     operations.
   */
  public ModelEventDispatcher(EventBus eventBus, ModelDispatcher dispatcher) {
    this.eventBus = eventBus;
    this.dispatcher = dispatcher;
  }

  /** {@inheritDoc} */
  @Override
  public Model registerModel(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchSchemaException, ModelAlreadyExistsException {
    ModelInfo registerRequest = new ModelInfo(ident.name(), properties, comment);
    eventBus.dispatchEvent(
        new RegisterModelPreEvent(PrincipalUtils.getCurrentUserName(), ident, registerRequest));
    try {
      Model model = dispatcher.registerModel(ident, comment, properties);
      // TODO: ModelEvent
      return model;
    } catch (Exception e) {
      // TODO: failureEvent
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Model registerModel(
      NameIdentifier ident,
      String uri,
      String[] aliases,
      String comment,
      Map<String, String> properties)
      throws NoSuchSchemaException, ModelAlreadyExistsException,
          ModelVersionAliasesAlreadyExistException {
    ModelInfo registerModelRequest = new ModelInfo(ident.name(), properties, comment);
    ModelVersionInfo linkModelVersionRequest =
        new ModelVersionInfo(uri, comment, properties, aliases);

    RegisterAndLinkModelPreEvent registerAndLinkModelPreEvent =
        new RegisterAndLinkModelPreEvent(
            PrincipalUtils.getCurrentUserName(),
            ident,
            registerModelRequest,
            linkModelVersionRequest);
    eventBus.dispatchEvent(registerAndLinkModelPreEvent);
    try {
      // TODO: ModelEvent
      return dispatcher.registerModel(ident, uri, aliases, comment, properties);
    } catch (Exception e) {
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Model getModel(NameIdentifier ident) throws NoSuchModelException {
    eventBus.dispatchEvent(new GetModelPreEvent(PrincipalUtils.getCurrentUserName(), ident));
    try {
      Model model = dispatcher.getModel(ident);
      // TODO: ModelEvent
      return model;
    } catch (Exception e) {
      // TODO: failureEvent
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean deleteModel(NameIdentifier ident) {
    eventBus.dispatchEvent(new DeleteModelPreEvent(PrincipalUtils.getCurrentUserName(), ident));
    try {
      // TODO: ModelEvent
      return dispatcher.deleteModel(ident);
    } catch (Exception e) {
      // TODO: failureEvent
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public NameIdentifier[] listModels(Namespace namespace) throws NoSuchSchemaException {
    eventBus.dispatchEvent(new ListModelPreEvent(PrincipalUtils.getCurrentUserName(), namespace));
    try {
      NameIdentifier[] models = dispatcher.listModels(namespace);
      // TODO: ModelEvent
      return models;
    } catch (Exception e) {
      // TODO: failureEvent
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public void linkModelVersion(
      NameIdentifier ident,
      String uri,
      String[] aliases,
      String comment,
      Map<String, String> properties)
      throws NoSuchModelException, ModelVersionAliasesAlreadyExistException {
    ModelVersionInfo linkModelRequest = new ModelVersionInfo(uri, comment, properties, aliases);
    eventBus.dispatchEvent(
        new LinkModelVersionPreEvent(PrincipalUtils.getCurrentUserName(), ident, linkModelRequest));
    try {
      dispatcher.linkModelVersion(ident, uri, aliases, comment, properties);
      // TODO: ModelEvent
    } catch (Exception e) {
      // TODO: failureEvent
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public ModelVersion getModelVersion(NameIdentifier ident, int version)
      throws NoSuchModelVersionException {
    eventBus.dispatchEvent(
        new GetModelVersionPreEvent(PrincipalUtils.getCurrentUserName(), ident, null, version));
    try {
      // TODO: ModelEvent
      return dispatcher.getModelVersion(ident, version);
    } catch (Exception e) {
      // TODO: failureEvent
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public ModelVersion getModelVersion(NameIdentifier ident, String alias)
      throws NoSuchModelVersionException {
    eventBus.dispatchEvent(
        new GetModelVersionPreEvent(PrincipalUtils.getCurrentUserName(), ident, alias, null));
    try {
      ModelVersion modelVersion = dispatcher.getModelVersion(ident, alias);
      // TODO: ModelEvent
      return modelVersion;
    } catch (Exception e) {
      // TODO: failureEvent
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean deleteModelVersion(NameIdentifier ident, int version) {
    eventBus.dispatchEvent(
        new DeleteModelVersionPreEvent(PrincipalUtils.getCurrentUserName(), ident, null, version));
    try {
      boolean isExists = dispatcher.deleteModelVersion(ident, version);
      // TODO: ModelEvent
      return isExists;
    } catch (Exception e) {
      // TODO: failureEvent
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean deleteModelVersion(NameIdentifier ident, String alias) {
    eventBus.dispatchEvent(
        new DeleteModelVersionPreEvent(PrincipalUtils.getCurrentUserName(), ident, alias, null));
    try {
      boolean isExists = dispatcher.deleteModelVersion(ident, alias);
      // TODO: ModelEvent
      return isExists;
    } catch (Exception e) {
      // TODO: failureEvent
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public int[] listModelVersions(NameIdentifier ident) throws NoSuchModelException {
    eventBus.dispatchEvent(
        new ListModelVersionPreEvent(PrincipalUtils.getCurrentUserName(), ident));
    try {
      int[] versions = dispatcher.listModelVersions(ident);
      // TODO: ModelEvent
      return versions;
    } catch (Exception e) {
      // TODO: failureEvent
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean modelExists(NameIdentifier ident) {
    return dispatcher.modelExists(ident);
  }

  /** {@inheritDoc} */
  @Override
  public boolean modelVersionExists(NameIdentifier ident, int version) {
    return dispatcher.modelVersionExists(ident, version);
  }

  /** {@inheritDoc} */
  @Override
  public boolean modelVersionExists(NameIdentifier ident, String alias) {
    return dispatcher.modelVersionExists(ident, alias);
  }
}
