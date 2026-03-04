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

/**
 * An interface for generating credentials. Implementations of this interface will contain the
 * actual logic for creating credentials and may have heavy dependencies. They are intended to be
 * loaded via reflection by a {@link CredentialProvider} to avoid classpath issues during service
 * loading.
 *
 * @param <T> The type of credential this generator produces.
 */
public interface CredentialGenerator<T extends Credential> extends Closeable {

  /**
   * Initializes the credential generator with catalog properties.
   *
   * @param properties catalog properties that can be used to configure the provider. The specific
   *     properties required vary by implementation.
   */
  void initialize(Map<String, String> properties);

  /**
   * Generates a credential.
   *
   * @param context The context providing necessary information for credential retrieval.
   * @return The generated credential.
   * @throws Exception if an error occurs during credential generation.
   */
  T generate(CredentialContext context) throws Exception;
}
