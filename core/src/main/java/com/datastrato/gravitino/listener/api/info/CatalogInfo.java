/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.listener.api.info;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.CatalogBasicInfo;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import javax.annotation.Nullable;

/** Encapsulates read-only information about a catalog, intended for use in event listeners. */
@DeveloperApi
public final class CatalogInfo {
  private final String name;
  private final CatalogBasicInfo.Type type;
  private final String provider;
  @Nullable private final String comment;
  private final Map<String, String> properties;
  @Nullable private final Audit auditInfo;

  /**
   * Constructs catalog information from a given catalog instance.
   *
   * @param catalog The source catalog.
   */
  public CatalogInfo(CatalogBasicInfo catalog) {
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
      CatalogBasicInfo.Type type,
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
  public CatalogBasicInfo.Type type() {
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
