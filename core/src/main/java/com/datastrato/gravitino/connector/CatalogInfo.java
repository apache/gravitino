/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.connector;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.annotation.Evolving;
import java.util.Map;

/**
 * CatalogInfo exposes all the information about a catalog to the connector interface. This class is
 * corresponding to CatalogEntity that is used internally.
 *
 * <p>This class object will be passed in through {@link CatalogOperations#initialize(Map,
 * CatalogInfo)}, users can leverage this object to get the information about the catalog.
 */
@Evolving
public final class CatalogInfo {

  private final Long id;

  private final String name;

  private final Catalog.Type type;

  private final String provider;

  private final String comment;

  private final Map<String, String> properties;

  private final Audit auditInfo;

  private final Namespace namespace;

  public CatalogInfo(
      Long id,
      String name,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties,
      Audit auditInfo,
      Namespace namespace) {
    this.id = id;
    this.name = name;
    this.type = type;
    this.provider = provider;
    this.comment = comment;
    this.properties = properties;
    this.auditInfo = auditInfo;
    this.namespace = namespace;
  }

  /** @return The unique id of the catalog. */
  public Long id() {
    return id;
  }

  /** @return The name of the catalog. */
  public String name() {
    return name;
  }

  /** @return The type of the catalog. */
  public Catalog.Type type() {
    return type;
  }

  /** @return The provider of the catalog. */
  public String provider() {
    return provider;
  }

  /** @return The comment or description for the catalog. */
  public String comment() {
    return comment;
  }

  /** @return The associated properties of the catalog. */
  public Map<String, String> properties() {
    return properties;
  }

  /** @return The audit details of the catalog. */
  public Audit auditInfo() {
    return auditInfo;
  }

  /** @return The namespace of the catalog. */
  public Namespace namespace() {
    return namespace;
  }
}
