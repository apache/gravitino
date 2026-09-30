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

package org.apache.gravitino.secret;

import org.apache.gravitino.annotation.DeveloperApi;

/** Constants for Gravitino entity secret management. */
@DeveloperApi
public final class SecretConstants {

  /** Prefix for Gravitino secret URNs. */
  public static final String URN_PREFIX = "urn:gravitino-secret:";

  /** Write attribute: entity type ({@code catalog}, {@code schema}, or {@code fileset}). */
  public static final String ATTR_ENTITY_TYPE = "entityType";

  /** Write attribute: stable numeric entity id. */
  public static final String ATTR_ENTITY_ID = "entityId";

  /** Write attribute: entity property key that holds the secret. */
  public static final String ATTR_PROPERTY_KEY = "propertyKey";

  private SecretConstants() {}
}
