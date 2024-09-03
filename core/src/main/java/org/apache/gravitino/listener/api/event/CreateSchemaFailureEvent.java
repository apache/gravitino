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
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.info.SchemaInfo;

/**
 * Represents an event that is generated when an attempt to create a schema fails due to an
 * exception.
 */
@DeveloperApi
public final class CreateSchemaFailureEvent extends SchemaFailureEvent {
  private final SchemaInfo createSchemaRequest;

  public CreateSchemaFailureEvent(
      String user, NameIdentifier identifier, Exception exception, SchemaInfo createSchemaRequest) {
    super(user, identifier, exception);
    this.createSchemaRequest = createSchemaRequest;
  }

  /**
   * Retrieves the original request information for the attempted schema creation.
   *
   * @return The {@link SchemaInfo} instance representing the request information for the failed
   *     schema creation attempt.
   */
  public SchemaInfo createSchemaRequest() {
    return createSchemaRequest;
  }
}
