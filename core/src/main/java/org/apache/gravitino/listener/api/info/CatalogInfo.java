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
package org.apache.gravitino.listener.api.info;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.annotation.DeveloperApi;

/** Encapsulates read-only information about a catalog, intended for use in event listeners. */
@DeveloperApi
public final class CatalogInfo {
  private final String name;
  private final Catalog.Type type;
  private final String provider;
  @Nullable private final String comment;
  private final Map<String, String> properties;
  @Nullable private final Audit auditInfo;

  /**
   * Constructs catalog information from a given catalog instance.
   *
   * @param catalog The source catalog.
   */
  public CatalogInfo(Catalog catalog) {
    this(
        catalog.name(),
        catalog.type(),
        catalog.provider(),
        catalog.comment(),
        catalog.properties(),
        catalog.auditInfo());
  }

  /**
   * Constructs catalog information with specified details.
   *
   * @param name The catalog name.
   * @param type The catalog type.
   * @param provider The catalog provider.
   * @param comment An optional comment about the catalog.
   * @param properties Catalog properties.
   * @param auditInfo Optional audit information.
   */
  public CatalogInfo(
      String name,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties,
      Audit auditInfo) {
    this.name = name;
    this.type = type;
    this.provider = provider;
    this.comment = comment;
    this.properties = properties == null ? ImmutableMap.of() : ImmutableMap.copyOf(properties);
    this.auditInfo = auditInfo;
  }

  /**
   * Returns the catalog name.
   *
   * @return Catalog name.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the catalog type.
   *
   * @return Catalog type.
   */
  public Catalog.Type type() {
    return type;
  }

  /**
   * Returns the catalog provider.
   *
   * @return Catalog provider.
   */
  public String provider() {
    return provider;
  }

  /**
   * Returns an optional comment about the catalog.
   *
   * @return Catalog comment, or null if not provided.
   */
  @Nullable
  public String comment() {
    return comment;
  }

  /**
   * Returns the catalog properties.
   *
   * @return An immutable map of catalog properties.
   */
  public Map<String, String> properties() {
    return properties;
  }

  /**
   * Returns optional audit information for the catalog.
   *
   * @return Audit information, or null if not provided.
   */
  @Nullable
  public Audit auditInfo() {
    return auditInfo;
  }
}
