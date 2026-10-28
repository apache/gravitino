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
package org.apache.gravitino.connector;

import java.util.Optional;
import org.apache.gravitino.annotation.Evolving;

/**
 * Catalogs that mint a GCS OAuth2 access token into config (for example {@code gcs.oauth2.token} /
 * {@code gcs.oauth2.token-expires-at}) so {@link org.apache.gravitino.catalog.CatalogManager} can
 * refresh the catalog cache entry before the token expires.
 */
@Evolving
public interface GcsOAuthTokenAware {

  /**
   * Epoch millis for {@code gcs.oauth2.token-expires-at}, or empty when unset.
   *
   * @return token expiry epoch millis, or empty
   */
  Optional<Long> expiresAtMs();
}
