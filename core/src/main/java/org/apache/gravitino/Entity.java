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
package org.apache.gravitino;

import java.io.Serializable;
import java.util.Map;
import lombok.Getter;

/** This interface defines an entity within the Apache Gravitino framework. */
public interface Entity extends Serializable {

  // The below constants are used for virtual metalakes, catalogs and schemas
  // The system doesn't need to create them. The system uses these constants
  // to organize the system information better.

  /** The system reserved metalake name. */
  String SYSTEM_METALAKE_RESERVED_NAME = "system";

  /** The system reserved catalog name. */
  String SYSTEM_CATALOG_RESERVED_NAME = "system";

  /** The authorization catalog name in the system metalake. */
  String AUTHORIZATION_CATALOG_NAME = "authorization";

  /** The user schema name in the system catalog. */
  String USER_SCHEMA_NAME = "user";

  /** The group schema name in the system catalog. */
  String GROUP_SCHEMA_NAME = "group";

  /** The role schema name in the system catalog. */
  String ROLE_SCHEMA_NAME = "role";

  /** The admin schema name in the authorization catalog of the system metalake. */
  String ADMIN_SCHEMA_NAME = "admin";

  /** The tag schema name in the system catalog. */
  String TAG_SCHEMA_NAME = "tag";

  /** The policy schema name in the system catalog. */
  String POLICY_SCHEMA_NAME = "policy";

  String JOB_TEMPLATE_SCHEMA_NAME = "job_template";

  String JOB_SCHEMA_NAME = "job";

  /** Enumeration defining the types of entities in the Gravitino framework. */
  @Getter
  enum EntityType {
    METALAKE,
    CATALOG,
    SCHEMA,
    TABLE,
    COLUMN,
    FILESET,
    TOPIC,
    USER,
    GROUP,
    ROLE,
    TAG,
    MODEL,
    MODEL_VERSION,
    POLICY,
    TABLE_STATISTIC,
    JOB_TEMPLATE,
    JOB,
    AUDIT;
  }

  /**
   * Validates the entity by ensuring the validity of its field arguments.
   *
   * @throws IllegalArgumentException If the validation fails.
   */
  default void validate() throws IllegalArgumentException {
    fields().forEach(Field::validate);
  }

  /**
   * Retrieves the fields and their associated values of the entity.
   *
   * @return A map of Field to Object representing the entity's schema with values.
   */
  Map<Field, Object> fields();

  /**
   * Retrieves the type of the entity.
   *
   * @return The type of the entity as defined by {@link EntityType}.
   */
  EntityType type();
}
