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
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.ModelDispatcher;
import org.apache.gravitino.exceptions.ModelAlreadyExistsException;
import org.apache.gravitino.exceptions.ModelVersionAliasesAlreadyExistException;
import org.apache.gravitino.exceptions.NoSuchModelException;
import org.apache.gravitino.exceptions.NoSuchModelVersionException;
import org.apache.gravitino.exceptions.NoSuchModelVersionURINameException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.model.ModelChange;
import org.apache.gravitino.model.ModelVersion;
import org.apache.gravitino.model.ModelVersionChange;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * {@code ModelHookDispatcher} is a decorator for {@link ModelDispatcher} that not only delegates
 * model operations to the underlying model dispatcher but also executes some hook operations before
 * or after the underlying operations.
 */
public class ModelHookDispatcher implements ModelDispatcher {

  private final ModelDispatcher dispatcher;

  public ModelHookDispatcher(ModelDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public NameIdentifier[] listModels(Namespace namespace) throws NoSuchSchemaException {
    return dispatcher.listModels(namespace);
  }

  @Override
  public Model getModel(NameIdentifier ident) throws NoSuchModelException {
    return dispatcher.getModel(ident);
  }

  @Override
  public Model registerModel(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchSchemaException, ModelAlreadyExistsException {
    // Check whether the current user exists or not
    AuthorizationUtils.checkCurrentUser(
        ident.namespace().level(0), PrincipalUtils.getCurrentUserName());

    Model model = dispatcher.registerModel(ident, comment, properties);

    // Set the creator as owner of the model.
    OwnerDispatcher ownerManager = GravitinoEnv.getInstance().ownerDispatcher();
    if (ownerManager != null) {
      ownerManager.setOwner(
          ident.namespace().level(0),
          NameIdentifierUtil.toMetadataObject(ident, Entity.EntityType.MODEL),
          PrincipalUtils.getCurrentUserName(),
          Owner.Type.USER);
    }
    return model;
  }

  @Override
  public boolean deleteModel(NameIdentifier ident) {
    return dispatcher.deleteModel(ident);
  }

  @Override
  public int[] listModelVersions(NameIdentifier ident) throws NoSuchModelException {
    return dispatcher.listModelVersions(ident);
  }

  @Override
  public ModelVersion[] listModelVersionInfos(NameIdentifier ident) throws NoSuchModelException {
    return dispatcher.listModelVersionInfos(ident);
  }

  @Override
  public ModelVersion getModelVersion(NameIdentifier ident, int version)
      throws NoSuchModelVersionException {
    return dispatcher.getModelVersion(ident, version);
  }

  @Override
  public ModelVersion getModelVersion(NameIdentifier ident, String alias)
      throws NoSuchModelVersionException {
    return dispatcher.getModelVersion(ident, alias);
  }

  @Override
  public void linkModelVersion(
      NameIdentifier ident,
      Map<String, String> uris,
      String[] aliases,
      String comment,
      Map<String, String> properties)
      throws NoSuchModelException, ModelVersionAliasesAlreadyExistException {
    dispatcher.linkModelVersion(ident, uris, aliases, comment, properties);
  }

  @Override
  public String getModelVersionUri(NameIdentifier ident, String alias, String uriName)
      throws NoSuchModelVersionException, NoSuchModelVersionURINameException {
    return dispatcher.getModelVersionUri(ident, alias, uriName);
  }

  @Override
  public String getModelVersionUri(NameIdentifier ident, int version, String uriName)
      throws NoSuchModelVersionException, NoSuchModelVersionURINameException {
    return dispatcher.getModelVersionUri(ident, version, uriName);
  }

  @Override
  public boolean deleteModelVersion(NameIdentifier ident, int version) {
    return dispatcher.deleteModelVersion(ident, version);
  }

  @Override
  public boolean deleteModelVersion(NameIdentifier ident, String alias) {
    return dispatcher.deleteModelVersion(ident, alias);
  }

  @Override
  public boolean modelExists(NameIdentifier ident) {
    return dispatcher.modelExists(ident);
  }

  @Override
  public Model registerModel(
      NameIdentifier ident,
      Map<String, String> uris,
      String[] aliases,
      String comment,
      Map<String, String> properties)
      throws NoSuchSchemaException, ModelAlreadyExistsException,
          ModelVersionAliasesAlreadyExistException {
    // Check whether the current user exists or not
    AuthorizationUtils.checkCurrentUser(
        ident.namespace().level(0), PrincipalUtils.getCurrentUserName());

    Model model = dispatcher.registerModel(ident, uris, aliases, comment, properties);

    // Set the creator as owner of the model.
    OwnerDispatcher ownerManager = GravitinoEnv.getInstance().ownerDispatcher();
    if (ownerManager != null) {
      ownerManager.setOwner(
          ident.name(),
          NameIdentifierUtil.toMetadataObject(ident, Entity.EntityType.MODEL),
          PrincipalUtils.getCurrentUserName(),
          Owner.Type.USER);
    }
    return model;
  }

  @Override
  public boolean modelVersionExists(NameIdentifier ident, int version) {
    return dispatcher.modelVersionExists(ident, version);
  }

  @Override
  public boolean modelVersionExists(NameIdentifier ident, String alias) {
    return dispatcher.modelVersionExists(ident, alias);
  }

  @Override
  public Model alterModel(NameIdentifier ident, ModelChange... changes)
      throws NoSuchModelException, IllegalArgumentException {
    return dispatcher.alterModel(ident, changes);
  }

  @Override
  public ModelVersion alterModelVersion(
      NameIdentifier ident, int version, ModelVersionChange... changes)
      throws NoSuchModelException, NoSuchModelVersionException, IllegalArgumentException {
    return dispatcher.alterModelVersion(ident, version, changes);
  }

  @Override
  public ModelVersion alterModelVersion(
      NameIdentifier ident, String alias, ModelVersionChange... changes)
      throws NoSuchModelException, IllegalArgumentException {
    return dispatcher.alterModelVersion(ident, alias, changes);
  }
}
