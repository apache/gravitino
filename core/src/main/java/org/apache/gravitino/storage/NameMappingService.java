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

package org.apache.gravitino.storage;

import java.io.IOException;
import org.apache.gravitino.storage.kv.KvEntityStore;

/**
 * {@link NameMappingService} manages name to id mappings when using {@link KvEntityStore} to store
 * entity.
 *
 * <p>Note. Implementations of this interface should be thread-safe.
 */
public interface NameMappingService extends AutoCloseable {

  /**
   * Get id by name in the mapping service.
   *
   * @param name the name of the entity
   * @return the id of the name, or null if the name does not exist
   * @throws IOException if the underlying storage failed
   */
  Long getIdByName(String name) throws IOException;

  /**
   * Get name by id in the mapping service.
   *
   * @param id the id of the name
   * @return the name of the id, or null if the id does not exist
   * @throws IOException
   */
  String getNameById(long id) throws IOException;

  /**
   * Get id from name. If the name does not exist, we will create a new id for the name and bind the
   * mapping between them
   *
   * @param name the name of the entity
   * @return the id of the name
   * @throws IOException if the underlying storage failed
   */
  long getOrCreateIdFromName(String name) throws IOException;

  /**
   * Update the mapping of the name to id. This method is used to update the mapping when we rename
   * an entity. E.g., If we change the name of entity from oldName to nameName, the following is the
   * mapping before and after the update.
   *
   * <pre>
   * Before:
   *   oldName ------ 1
   *   1       ------ oldName
   *
   * After:
   *  newName ---- 1
   *  1       ---- newName
   * </pre>
   *
   * @param oldName name to be updated
   * @param newName new name
   * @return true if the name exists and is updated successfully, false if the name does not exist
   * @throws IOException if the underlying storage failed
   */
  boolean updateName(String oldName, String newName) throws IOException;

  /**
   * Unbind id-name mapping. Ignore if the name does not exist.
   *
   * @param name name to be unbind
   * @return true if the name exists and is deleted successfully, false if the name does not exist
   * @throws IOException if the underlying storage failed
   */
  boolean unbindNameAndId(String name) throws IOException;
}
