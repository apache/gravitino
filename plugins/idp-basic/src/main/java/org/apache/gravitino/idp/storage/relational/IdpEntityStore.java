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
import org.apache.gravitino.idp.exception.AlreadyExistsException;
import org.apache.gravitino.idp.exception.NotFoundException;
import org.apache.gravitino.idp.meta.IdpEntity;
import org.apache.gravitino.idp.meta.IdpEntityType;

/** Entity store for built-in IdP entities backed by JDBC. */
public class IdpEntityStore implements IdpStore {

  private IdpJDBCBackend backend;
  private IdpGarbageCollector garbageCollector;

  @Override
  public void initialize(Config config) throws RuntimeException {
    this.backend = new IdpJDBCBackend();
    this.backend.initialize(config);
    this.garbageCollector = new IdpGarbageCollector(backend, config);
    this.garbageCollector.start();
  }

  @Override
  public boolean exists(String name, IdpEntityType entityType) throws IOException {
    return backend.exists(name, entityType);
  }

  @Override
  public <E extends IdpEntity> void put(E entity, boolean overwritten)
      throws IOException, AlreadyExistsException {
    backend.insert(entity, overwritten);
  }

  @Override
  public <E extends IdpEntity> E get(String name, IdpEntityType entityType, Class<E> clazz)
      throws NotFoundException, IOException {
    return clazz.cast(backend.get(name, entityType));
  }

  @Override
  public void changePassword(String username, String passwordHash) throws NotFoundException {
    backend.changePassword(username, passwordHash);
  }

  @Override
  public void addUsersToGroup(String groupName, List<String> usernames) {
    backend.addUsersToGroup(groupName, usernames);
  }

  @Override
  public void removeUsersFromGroup(String groupName, List<String> usernames) {
    backend.removeUsersFromGroup(groupName, usernames);
  }

  @Override
  public boolean delete(String name, IdpEntityType entityType, boolean cascade) throws IOException {
    return backend.delete(name, entityType, cascade);
  }

  @Override
  public void close() throws IOException {
    if (garbageCollector != null) {
      garbageCollector.close();
    }
    if (backend != null) {
      backend.close();
    }
  }
}
