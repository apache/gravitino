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
import org.apache.gravitino.annotation.Evolving;

/**
 * Interface to fetch entity properties whose values are stored as Gravitino secret URNs, resolved
 * to plaintext.
 *
 * <p>Unlike {@link org.apache.gravitino.Catalog#properties()} (and schema/fileset load), which omit
 * secret-backed keys, this API returns only those secret keys with plaintext values for connectors
 * that need Vault-backed custom configuration. Credential vending remains on {@link
 * org.apache.gravitino.credential.SupportsCredentials}.
 *
 * <p>UI clients must not call this API; it is intended for trusted engine connectors.
 */
@Evolving
public interface SupportsSecretProperties {

  /**
   * Returns secret-backed property keys mapped to their plaintext values.
   *
   * <p>Non-secret properties are not included. The map may be empty when the entity has no secret
   * URN properties.
   *
   * @return secret key to plaintext value map; never null
   */
  Map<String, String> getSecretProperties();
}
