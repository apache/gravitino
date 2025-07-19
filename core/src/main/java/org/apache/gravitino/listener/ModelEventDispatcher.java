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

import java.util.Arrays;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.ModelDispatcher;
import org.apache.gravitino.exceptions.ModelAlreadyExistsException;
import org.apache.gravitino.exceptions.ModelVersionAliasesAlreadyExistException;
import org.apache.gravitino.exceptions.NoSuchModelException;
import org.apache.gravitino.exceptions.NoSuchModelVersionException;
import org.apache.gravitino.exceptions.NoSuchModelVersionURINameException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.listener.api.event.AlterModelEvent;
import org.apache.gravitino.listener.api.event.AlterModelFailureEvent;
import org.apache.gravitino.listener.api.event.AlterModelPreEvent;
import org.apache.gravitino.listener.api.event.AlterModelVersionEvent;
import org.apache.gravitino.listener.api.event.AlterModelVersionFailureEvent;
import org.apache.gravitino.listener.api.event.AlterModelVersionPreEvent;
import org.apache.gravitino.listener.api.event.DeleteModelEvent;
import org.apache.gravitino.listener.api.event.DeleteModelFailureEvent;
import org.apache.gravitino.listener.api.event.DeleteModelPreEvent;
import org.apache.gravitino.listener.api.event.DeleteModelVersionEvent;
import org.apache.gravitino.listener.api.event.DeleteModelVersionFailureEvent;
import org.apache.gravitino.listener.api.event.DeleteModelVersionPreEvent;
import org.apache.gravitino.listener.api.event.GetModelEvent;
import org.apache.gravitino.listener.api.event.GetModelFailureEvent;
import org.apache.gravitino.listener.api.event.GetModelPreEvent;
import org.apache.gravitino.listener.api.event.GetModelVersionEvent;
import org.apache.gravitino.listener.api.event.GetModelVersionFailureEvent;
import org.apache.gravitino.listener.api.event.GetModelVersionPreEvent;
import org.apache.gravitino.listener.api.event.GetModelVersionUriEvent;
import org.apache.gravitino.listener.api.event.GetModelVersionUriFailureEvent;
import org.apache.gravitino.listener.api.event.GetModelVersionUriPreEvent;
import org.apache.gravitino.listener.api.event.LinkModelVersionEvent;
import org.apache.gravitino.listener.api.event.LinkModelVersionFailureEvent;
import org.apache.gravitino.listener.api.event.LinkModelVersionPreEvent;
import org.apache.gravitino.listener.api.event.ListModelEvent;
import org.apache.gravitino.listener.api.event.ListModelFailureEvent;
import org.apache.gravitino.listener.api.event.ListModelPreEvent;
import org.apache.gravitino.listener.api.event.ListModelVersionFailureEvent;
import org.apache.gravitino.listener.api.event.ListModelVersionInfosEvent;
import org.apache.gravitino.listener.api.event.ListModelVersionPreEvent;
import org.apache.gravitino.listener.api.event.ListModelVersionsEvent;
import org.apache.gravitino.listener.api.event.RegisterAndLinkModelEvent;
import org.apache.gravitino.listener.api.event.RegisterAndLinkModelFailureEvent;
import org.apache.gravitino.listener.api.event.RegisterAndLinkModelPreEvent;
import org.apache.gravitino.listener.api.event.RegisterModelEvent;
import org.apache.gravitino.listener.api.event.RegisterModelFailureEvent;
import org.apache.gravitino.listener.api.event.RegisterModelPreEvent;
import org.apache.gravitino.listener.api.info.Either;
import org.apache.gravitino.listener.api.info.ModelInfo;
import org.apache.gravitino.listener.api.info.ModelVersionInfo;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.model.ModelChange;
import org.apache.gravitino.model.ModelVersion;
import org.apache.gravitino.model.ModelVersionChange;
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
    String user = PrincipalUtils.getCurrentUserName();
    ModelInfo registerRequest = new ModelInfo(ident.name(), properties, comment);

    eventBus.dispatchEvent(new RegisterModelPreEvent(user, ident, registerRequest));
    try {
      Model model = dispatcher.registerModel(ident, comment, properties);
      ModelInfo registeredModel = new ModelInfo(model);
      eventBus.dispatchEvent(new RegisterModelEvent(user, ident, registeredModel));
      return model;
    } catch (Exception e) {
      eventBus.dispatchEvent(new RegisterModelFailureEvent(user, ident, e, registerRequest));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Model registerModel(
      NameIdentifier ident,
      Map<String, String> uris,
      String[] aliases,
      String comment,
      Map<String, String> properties)
      throws NoSuchSchemaException, ModelAlreadyExistsException,
          ModelVersionAliasesAlreadyExistException {
    ModelInfo registerModelRequest = new ModelInfo(ident.name(), properties, comment);
    ModelVersionInfo linkModelVersionRequest =
        new ModelVersionInfo(uris, comment, properties, aliases, null);
    String user = PrincipalUtils.getCurrentUserName();
    RegisterAndLinkModelPreEvent registerAndLinkModelPreEvent =
        new RegisterAndLinkModelPreEvent(
            user, ident, registerModelRequest, linkModelVersionRequest);

    eventBus.dispatchEvent(registerAndLinkModelPreEvent);
    try {
      Model registeredModel = dispatcher.registerModel(ident, uris, aliases, comment, properties);
      ModelInfo registeredModelInfo = new ModelInfo(registeredModel);
      eventBus.dispatchEvent(new RegisterAndLinkModelEvent(user, ident, registeredModelInfo, uris));
      return registeredModel;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new RegisterAndLinkModelFailureEvent(
              user, ident, e, registerModelRequest, linkModelVersionRequest));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Model getModel(NameIdentifier ident) throws NoSuchModelException {
    String user = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new GetModelPreEvent(user, ident));
    try {
      Model model = dispatcher.getModel(ident);
      ModelInfo modelInfo = new ModelInfo(model);
      eventBus.dispatchEvent(new GetModelEvent(user, ident, modelInfo));
      return model;
    } catch (Exception e) {
      eventBus.dispatchEvent(new GetModelFailureEvent(user, ident, e));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean deleteModel(NameIdentifier ident) {
    String user = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new DeleteModelPreEvent(user, ident));
    try {
      boolean isExists = dispatcher.deleteModel(ident);
      eventBus.dispatchEvent(new DeleteModelEvent(user, ident, isExists));
      return isExists;
    } catch (Exception e) {
      eventBus.dispatchEvent(new DeleteModelFailureEvent(user, ident, e));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public NameIdentifier[] listModels(Namespace namespace) throws NoSuchSchemaException {
    String user = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new ListModelPreEvent(user, namespace));
    try {
      NameIdentifier[] models = dispatcher.listModels(namespace);
      eventBus.dispatchEvent(new ListModelEvent(user, namespace));
      return models;
    } catch (Exception e) {
      eventBus.dispatchEvent(new ListModelFailureEvent(user, namespace, e));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public void linkModelVersion(
      NameIdentifier ident,
      Map<String, String> uris,
      String[] aliases,
      String comment,
      Map<String, String> properties)
      throws NoSuchModelException, ModelVersionAliasesAlreadyExistException {
    ModelVersionInfo linkModelRequest =
        new ModelVersionInfo(uris, comment, properties, aliases, null);
    String user = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new LinkModelVersionPreEvent(user, ident, linkModelRequest));
    try {
      dispatcher.linkModelVersion(ident, uris, aliases, comment, properties);
      eventBus.dispatchEvent(new LinkModelVersionEvent(user, ident, linkModelRequest));
    } catch (Exception e) {
      eventBus.dispatchEvent(new LinkModelVersionFailureEvent(user, ident, e, linkModelRequest));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public ModelVersion getModelVersion(NameIdentifier ident, int version)
      throws NoSuchModelVersionException {
    String user = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new GetModelVersionPreEvent(user, ident, null, version));
    try {
      ModelVersion modelVersion = dispatcher.getModelVersion(ident, version);
      ModelVersionInfo modelVersionInfo = new ModelVersionInfo(modelVersion);
      eventBus.dispatchEvent(
          new GetModelVersionEvent(user, ident, modelVersionInfo, null, version));
      return modelVersion;
    } catch (Exception e) {
      eventBus.dispatchEvent(new GetModelVersionFailureEvent(user, ident, e, null, version));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public ModelVersion getModelVersion(NameIdentifier ident, String alias)
      throws NoSuchModelVersionException {
    String user = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new GetModelVersionPreEvent(user, ident, alias, null));
    try {
      ModelVersion modelVersion = dispatcher.getModelVersion(ident, alias);
      ModelVersionInfo modelVersionInfo = new ModelVersionInfo(modelVersion);
      eventBus.dispatchEvent(new GetModelVersionEvent(user, ident, modelVersionInfo, alias, null));
      return modelVersion;
    } catch (Exception e) {
      eventBus.dispatchEvent(new GetModelVersionFailureEvent(user, ident, e, alias, null));
      throw e;
    }
  }

  @Override
  public String getModelVersionUri(NameIdentifier ident, String alias, String uriName)
      throws NoSuchModelVersionException, NoSuchModelVersionURINameException {
    String user = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new GetModelVersionUriPreEvent(user, ident, alias, null, uriName));
    try {
      String uri = dispatcher.getModelVersionUri(ident, alias, uriName);
      eventBus.dispatchEvent(new GetModelVersionUriEvent(user, ident, alias, null, uriName, uri));
      return uri;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new GetModelVersionUriFailureEvent(user, ident, e, alias, null, uriName));
      throw e;
    }
  }

  @Override
  public String getModelVersionUri(NameIdentifier ident, int version, String uriName)
      throws NoSuchModelVersionException, NoSuchModelVersionURINameException {
    String user = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new GetModelVersionUriPreEvent(user, ident, null, version, uriName));
    try {
      String uri = dispatcher.getModelVersionUri(ident, version, uriName);
      eventBus.dispatchEvent(new GetModelVersionUriEvent(user, ident, null, version, uriName, uri));
      return uri;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new GetModelVersionUriFailureEvent(user, ident, e, null, version, uriName));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean deleteModelVersion(NameIdentifier ident, int version) {
    String user = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new DeleteModelVersionPreEvent(user, ident, null, version));
    try {
      boolean isExists = dispatcher.deleteModelVersion(ident, version);
      eventBus.dispatchEvent(new DeleteModelVersionEvent(user, ident, isExists, null, version));
      return isExists;
    } catch (Exception e) {
      eventBus.dispatchEvent(new DeleteModelVersionFailureEvent(user, ident, e, null, version));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean deleteModelVersion(NameIdentifier ident, String alias) {
    String user = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new DeleteModelVersionPreEvent(user, ident, alias, null));
    try {
      boolean isExists = dispatcher.deleteModelVersion(ident, alias);
      eventBus.dispatchEvent(new DeleteModelVersionEvent(user, ident, isExists, alias, null));
      return isExists;
    } catch (Exception e) {
      eventBus.dispatchEvent(new DeleteModelVersionFailureEvent(user, ident, e, alias, null));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Model alterModel(NameIdentifier ident, ModelChange... changes)
      throws NoSuchModelException, IllegalArgumentException {
    String user = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new AlterModelPreEvent(user, ident, changes));
    try {
      Model modelObject = dispatcher.alterModel(ident, changes);
      ModelInfo modelInfo = new ModelInfo(modelObject);
      eventBus.dispatchEvent(new AlterModelEvent(user, ident, modelInfo, changes));

      return modelObject;
    } catch (Exception e) {
      eventBus.dispatchEvent(new AlterModelFailureEvent(user, ident, e, changes));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public ModelVersion alterModelVersion(
      NameIdentifier ident, int version, ModelVersionChange... changes)
      throws NoSuchModelException, NoSuchModelVersionException, IllegalArgumentException {
    String initiator = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(
        new AlterModelVersionPreEvent(initiator, ident, Either.right(version), changes));
    try {
      ModelVersion modelVersion = dispatcher.alterModelVersion(ident, version, changes);
      eventBus.dispatchEvent(
          new AlterModelVersionEvent(
              initiator, ident, new ModelVersionInfo(modelVersion), changes));

      return modelVersion;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new AlterModelVersionFailureEvent(initiator, ident, e, Either.right(version), changes));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public ModelVersion alterModelVersion(
      NameIdentifier ident, String alias, ModelVersionChange... changes)
      throws NoSuchModelException, IllegalArgumentException {
    String initiator = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(
        new AlterModelVersionPreEvent(initiator, ident, Either.left(alias), changes));
    try {
      ModelVersion modelVersion = dispatcher.alterModelVersion(ident, alias, changes);
      eventBus.dispatchEvent(
          new AlterModelVersionEvent(
              initiator, ident, new ModelVersionInfo(modelVersion), changes));

      return modelVersion;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new AlterModelVersionFailureEvent(initiator, ident, e, Either.left(alias), changes));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public int[] listModelVersions(NameIdentifier ident) throws NoSuchModelException {
    String user = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new ListModelVersionPreEvent(user, ident));
    try {
      int[] versions = dispatcher.listModelVersions(ident);
      eventBus.dispatchEvent(new ListModelVersionsEvent(user, ident, versions));
      return versions;
    } catch (Exception e) {
      eventBus.dispatchEvent(new ListModelVersionFailureEvent(user, ident, e));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public ModelVersion[] listModelVersionInfos(NameIdentifier ident) throws NoSuchModelException {
    String user = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new ListModelVersionPreEvent(user, ident));
    try {
      ModelVersion[] modelVersions = dispatcher.listModelVersionInfos(ident);
      ModelVersionInfo[] modelVersionInfos =
          Arrays.stream(modelVersions).map(ModelVersionInfo::new).toArray(ModelVersionInfo[]::new);
      eventBus.dispatchEvent(new ListModelVersionInfosEvent(user, ident, modelVersionInfos));
      return modelVersions;
    } catch (Exception e) {
      eventBus.dispatchEvent(new ListModelVersionFailureEvent(user, ident, e));
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
