/*
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

package com.apache.gravitino.listener.api.event;

import com.apache.gravitino.NameIdentifier;
import com.apache.gravitino.SchemaChange;
import com.apache.gravitino.annotation.DeveloperApi;

/**
 * Represents an event that is triggered when an attempt to alter a schema fails due to an
 * exception.
 */
@DeveloperApi
public final class AlterSchemaFailureEvent extends SchemaFailureEvent {
  private final SchemaChange[] schemaChanges;

  public AlterSchemaFailureEvent(
      String user, NameIdentifier identifier, Exception exception, SchemaChange[] schemaChanges) {
    super(user, identifier, exception);
    this.schemaChanges = schemaChanges.clone();
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
