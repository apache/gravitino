/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.authorization;

import java.util.Optional;
import org.apache.gravitino.MetadataObject;

/** Dispatcher interface for managing owners of metadata objects. */
public interface OwnerDispatcher {

  /**
   * Sets the owner of a metadata object.
   *
   * @param metalake the name of the metalake
   * @param metadataObject the metadata object for which the owner is being set
   * @param ownerName the name of the owner
   * @param ownerType the type of the owner (e.g., USER, GROUP)
   */
  void setOwner(
      String metalake, MetadataObject metadataObject, String ownerName, Owner.Type ownerType);

  /**
   * Retrieves the owner of a metadata object.
   *
   * @param metalake the name of the metalake
   * @param metadataObject the metadata object for which the owner is being retrieved
   * @return an Optional containing the owner if it exists, otherwise an empty Optional
   */
  Optional<Owner> getOwner(String metalake, MetadataObject metadataObject);
}
