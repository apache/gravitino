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

package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.info.SchemaInfo;

/** Represents an event fired when a schema is successfully altered. */
@DeveloperApi
public final class AlterSchemaEvent extends SchemaEvent {
  private final SchemaChange[] schemaChanges;
  private final SchemaInfo updatedSchemaInfo;

  public AlterSchemaEvent(
      String user,
      NameIdentifier identifier,
      SchemaChange[] schemaChanges,
      SchemaInfo updatedSchemaInfo) {
    super(user, identifier);
    this.schemaChanges = schemaChanges.clone();
    this.updatedSchemaInfo = updatedSchemaInfo;
  }

  /**
   * Retrieves the updated state of the schema after the successful alteration.
   *
   * @return A {@link SchemaInfo} instance encapsulating the details of the altered schema.
   */
  public SchemaInfo updatedSchemaInfo() {
    return updatedSchemaInfo;
  }

  /**
   * Retrieves the specific changes that were made to the schema during the alteration process.
   *
   * @return An array of {@link SchemaChange} objects detailing each modification applied to the
   *     schema.
   */
  public SchemaChange[] schemaChanges() {
    return schemaChanges;
  }
}
