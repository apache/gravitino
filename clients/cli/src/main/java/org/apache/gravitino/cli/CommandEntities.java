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

package org.apache.gravitino.cli;

import java.util.HashSet;

/**
 * The {@code CommandEntities} class defines a set of standard entities that can be used in the
 * Gravitino CLI. It also can validate if a given entity is a valid entity.
 */
public class CommandEntities {
  public static final String METALAKE = "metalake";
  public static final String CATALOG = "catalog";
  public static final String SCHEMA = "schema";
  public static final String TABLE = "table";
  public static final String COLUMN = "column";
  public static final String USER = "user";
  public static final String GROUP = "group";

  private static final HashSet<String> VALID_ENTITIES = new HashSet<>();

  static {
    VALID_ENTITIES.add(METALAKE);
    VALID_ENTITIES.add(CATALOG);
    VALID_ENTITIES.add(SCHEMA);
    VALID_ENTITIES.add(TABLE);
    VALID_ENTITIES.add(COLUMN);
    VALID_ENTITIES.add(USER);
    VALID_ENTITIES.add(GROUP);
  }

  /**
   * Checks if a given command is a valid entity.
   *
   * @param entity The entity to check.
   * @return true if the command is valid, false otherwise.
   */
  public static boolean isValidEntity(String entity) {
    return VALID_ENTITIES.contains(entity);
  }
}
