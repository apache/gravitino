/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.credential;

import java.io.Closeable;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Interface for credential providers.
 *
 * <p>A credential provider is responsible for managing and retrieving credentials.
 */
public interface CredentialProvider extends Closeable {
  /**
   * Initializes the credential provider with catalog properties.
   *
   * @param properties catalog properties that can be used to configure the provider. The specific
   *     properties required vary by implementation.
   */
  void initialize(Map<String, String> properties);

  /**
   * Returns the type of credential, it should be identical in Gravitino.
   *
   * @return A string identifying the type of credentials.
   */
  String credentialType();

  /**
   * Obtains a credential based on the provided context information.
   *
   * @param context A context object providing necessary information for retrieving credentials.
   * @return A Credential object containing the authentication information needed to access a system
   *     or resource. Null will be returned if no credential is available.
   */
  @Nullable
  Credential getCredential(CredentialContext context);
}
