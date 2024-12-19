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
package org.apache.gravitino.catalog;

import java.util.Map;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.ModelAlreadyExistsException;
import org.apache.gravitino.exceptions.ModelVersionAliasesAlreadyExistException;
import org.apache.gravitino.exceptions.NoSuchModelException;
import org.apache.gravitino.exceptions.NoSuchModelVersionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.model.ModelVersion;
import org.apache.gravitino.storage.IdGenerator;

public class ModelOperationDispatcher extends OperationDispatcher implements ModelDispatcher {

  public ModelOperationDispatcher(
      CatalogManager catalogManager, EntityStore store, IdGenerator idGenerator) {
    super(catalogManager, store, idGenerator);
  }

  @Override
  public NameIdentifier[] listModels(Namespace namespace) throws NoSuchSchemaException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Model getModel(NameIdentifier ident) throws NoSuchModelException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Model registerModel(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchModelException, ModelAlreadyExistsException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean deleteModel(NameIdentifier ident) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int[] listModelVersions(NameIdentifier ident) throws NoSuchModelException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ModelVersion getModelVersion(NameIdentifier ident, int version)
      throws NoSuchModelVersionException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ModelVersion getModelVersion(NameIdentifier ident, String alias)
      throws NoSuchModelVersionException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void linkModelVersion(
      NameIdentifier ident,
      String uri,
      String[] aliases,
      String comment,
      Map<String, String> properties)
      throws NoSuchModelException, ModelVersionAliasesAlreadyExistException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean deleteModelVersion(NameIdentifier ident, int version) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean deleteModelVersion(NameIdentifier ident, String alias) {
    throw new UnsupportedOperationException("Not implemented");
  }
}
