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
package org.apache.gravitino.storage.relational.converters;

import java.io.IOException;
import java.sql.SQLException;
import org.apache.gravitino.Entity;

/** Interface for converter JDBC SQL exceptions to Apache Gravitino exceptions. */
public interface SQLExceptionConverter {
  /**
   * Convert JDBC exception to GravitinoException.
   *
   * @param sqlException The sql exception to map
   * @param type The type of the entity
   * @param name The name of the entity
   * @throws IOException if an I/O error occurs during exception conversion
   */
  void toGravitinoException(SQLException sqlException, Entity.EntityType type, String name)
      throws IOException;
}
