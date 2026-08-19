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

import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Catalog;

/** Helpers for applying catalog secret-properties in engine connectors. */
public final class CatalogSecretProperties {

  private CatalogSecretProperties() {}

  /**
   * Fetches secret-backed catalog properties as plaintext, or an empty map when unsupported.
   *
   * @param catalog Gravitino catalog client
   * @return secret key to plaintext map; never null
   */
  public static Map<String, String> getSecretProperties(Catalog catalog) {
    try {
      Map<String, String> props = catalog.supportsSecretProperties().getSecretProperties();
      return props == null ? Collections.emptyMap() : props;
    } catch (UnsupportedOperationException e) {
      return Collections.emptyMap();
    }
  }

  /**
   * Merges secret-backed catalog properties into {@code target}. Existing keys in {@code target}
   * are overwritten when present in secret properties.
   *
   * @param catalog Gravitino catalog client
   * @param target mutable configuration map
   */
  public static void applySecretProperties(Catalog catalog, Map<String, String> target) {
    target.putAll(getSecretProperties(catalog));
  }
}
