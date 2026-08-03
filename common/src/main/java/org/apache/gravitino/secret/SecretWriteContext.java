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

/** Context information used when writing a secret through a {@link SecretProvider}. */
@DeveloperApi
public interface SecretWriteContext {

  /**
   * Returns the configured secret provider name used in the URN.
   *
   * @return the provider name
   */
  String providerName();

  /**
   * Returns the entity type (catalog, schema, or fileset).
   *
   * @return the entity type
   */
  String entityType();

  /**
   * Returns the stable entity identifier.
   *
   * @return the entity identifier
   */
  long entityId();

  /**
   * Returns the property key holding the secret.
   *
   * @return the property key
   */
  String propertyKey();
}
