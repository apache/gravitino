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

import java.util.Map;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.authorization.SupportsRoles;

/**
 * The interface of a metalake. The metalake is the top level entity in the Apache Gravitino system,
 * containing a set of catalogs.
 */
@Evolving
public interface Metalake extends Auditable {

  /** The property indicating the metalake is in use. */
  String PROPERTY_IN_USE = "in-use";

  /**
   * The name of the metalake.
   *
   * @return The name of the metalake.
   */
  String name();

  /**
   * The comment of the metalake. Note. this method will return null if the comment is not set for
   * this metalake.
   *
   * @return The comment of the metalake.
   */
  String comment();

  /**
   * The properties of the metalake. Note, this method will return null if the properties are not
   * set.
   *
   * @return The properties of the metalake.
   */
  Map<String, String> properties();

  /**
   * @return the {@link SupportsRoles} if the metalake supports role operations.
   * @throws UnsupportedOperationException if the metalake does not support role operations.
   */
  default SupportsRoles supportsRoles() {
    throw new UnsupportedOperationException("Metalake does not support role operations.");
  }
}
