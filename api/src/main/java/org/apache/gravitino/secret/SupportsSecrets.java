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

import java.util.Map;

/**
 * Interface to retrieve secret-manager plaintext properties for a metadata object.
 *
 * <p>Only secret-URN property values are resolved and returned. Credential-vending keys (including
 * {@code jdbc-user} / {@code jdbc-password}) are omitted — use {@link
 * org.apache.gravitino.credential.SupportsCredentials} instead. Normal non-secret properties are
 * not included; combine with {@code load*().properties()} on the client.
 */
public interface SupportsSecrets {

  /**
   * Returns secret-manager plaintext properties for this metadata object.
   *
   * @return a map of property key to resolved plaintext value; never null, may be empty
   */
  Map<String, String> getSecrets();
}
