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

package org.apache.gravitino.spark.connector.authorization;

import java.util.Set;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.exceptions.ForbiddenException;

/** Exposes the privileges required to use a table that was denied during Spark analysis. */
public interface SupportsRequiredPrivileges {

  /**
   * Returns the fully qualified table identifier.
   *
   * @return the table identifier
   */
  String tableIdentifier();

  /**
   * Returns the privileges required to use the table.
   *
   * @return the required privileges
   */
  Set<Privilege.Name> requiredPrivileges();

  /**
   * Returns the original authorization failure.
   *
   * @return the original authorization failure
   */
  ForbiddenException forbiddenException();
}
