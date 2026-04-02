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
package org.apache.gravitino.utils;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import javax.annotation.Nullable;

/**
 * Key used to identify a shared ClassLoader in the {@link ClassLoaderPool}. Stores isolation
 * properties as a generic {@code Map<String, String>}, decoupled from any specific property names.
 * This makes the pool infrastructure key-agnostic — only the logic that builds the key (in {@code
 * CatalogManager}) needs to know which properties matter.
 */
public class ClassLoaderKey {

  private final String provider;
  private final Map<String, String> properties;

  /**
   * Constructs a ClassLoaderKey.
   *
   * @param provider The catalog provider name (e.g., "hive", "lakehouse-iceberg").
   * @param properties The isolation-relevant properties extracted from the catalog configuration,
   *     or null if none.
   */
  public ClassLoaderKey(String provider, @Nullable Map<String, String> properties) {
    this.provider = Objects.requireNonNull(provider, "provider must not be null");
    this.properties =
        properties == null
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(new TreeMap<>(properties));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ClassLoaderKey)) return false;
    ClassLoaderKey that = (ClassLoaderKey) o;
    return Objects.equals(provider, that.provider) && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(provider, properties);
  }

  @Override
  public String toString() {
    return "ClassLoaderKey{" + "provider='" + provider + '\'' + ", properties=" + properties + '}';
  }
}
