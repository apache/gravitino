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
package org.apache.gravitino.idp.storage.relational;

import java.io.IOException;
import java.util.List;
import org.apache.gravitino.Config;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.idp.meta.IdpEntity;
import org.apache.gravitino.idp.meta.IdpEntityType;

/**
 * Relational entity store for built-in IdP entities. It mirrors {@link
 * org.apache.gravitino.storage.relational.RelationalEntityStore} but uses {@link IdpJDBCBackend}.
 */
public class IdpRelationalEntityStore implements IdpEntityStore {

  private IdpJDBCBackend backend;
  private IdpRelationalGarbageCollector garbageCollector;

  @Override
  public void initialize(Config config) throws RuntimeException {
    this.backend = new IdpJDBCBackend();
    this.backend.initialize(config);
    this.garbageCollector = new IdpRelationalGarbageCollector(backend, config);
    this.garbageCollector.start();
  }

  @Override
  public boolean exists(NameIdentifier ident, IdpEntityType entityType) throws IOException {
    return backend.exists(ident, entityType);
  }

  @Override
  public <E extends IdpEntity & HasIdentifier> void put(E entity, boolean overwritten)
      throws IOException, EntityAlreadyExistsException {
    backend.insert(entity, overwritten);
  }

  @Override
  public <E extends IdpEntity & HasIdentifier> E get(
      NameIdentifier ident, IdpEntityType entityType, Class<E> clazz)
      throws NoSuchEntityException, IOException {
    return clazz.cast(backend.get(ident, entityType));
  }

  @Override
  public <E extends IdpEntity & HasIdentifier> List<E> batchGet(
      List<NameIdentifier> idents, IdpEntityType entityType, Class<E> clazz) {
    return (List<E>) backend.batchGet(idents, entityType);
  }

  @Override
  public boolean delete(NameIdentifier ident, IdpEntityType entityType) throws IOException {
    return backend.delete(ident, entityType, false);
  }

  @Override
  public void close() throws IOException {
    garbageCollector.close();
    backend.close();
  }
}
