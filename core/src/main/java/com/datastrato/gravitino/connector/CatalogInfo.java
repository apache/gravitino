/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.connector;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.annotation.Evolving;
import com.datastrato.gravitino.meta.EntityMetadata;
import java.util.Map;

/**
 * CatalogInfo exposes all the information about a catalog to the connector interface. This class is
 * corresponding to CatalogEntity that is used internally.
 *
 * <p>This class object will be passed in through {@link CatalogOperations#initialize(Map,
 * CatalogInfo, HasPropertyMetadata)}, users can leverage this object to get the information about
 * the catalog.
 */
@Evolving
public final class CatalogInfo implements Catalog {

  private final Catalog.Type type;

  private final String provider;

  private final Audit auditInfo;

  private final Namespace namespace;
  private EntityMetadata entityMetadata = new EntityMetadata(null, null, null, null);

  public CatalogInfo(
      Long id,
      String name,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties,
      Audit auditInfo,
      Namespace namespace) {
    this.entityMetadata.setId(id);
    this.entityMetadata.setName(name);
    this.type = type;
    this.provider = provider;
    this.entityMetadata.setComment(comment);
    this.entityMetadata.setProperties(properties);
    this.auditInfo = auditInfo;
    this.namespace = namespace;
  }

  /** @return The unique id of the catalog. */
  public Long id() {
    return entityMetadata.getId();
  }

  /** @return The name of the catalog. */
  @Override
  public String name() {
    return entityMetadata.getName();
  }

  /** @return The type of the catalog. */
  @Override
  public Catalog.Type type() {
    return type;
  }

  /** @return The provider of the catalog. */
  @Override
  public String provider() {
    return provider;
  }

  /** @return The comment or description for the catalog. */
  @Override
  public String comment() {
    return entityMetadata.getComment();
  }

  /** @return The associated properties of the catalog. */
  @Override
  public Map<String, String> properties() {
    return entityMetadata.getProperties();
  }

  /** @return The audit details of the catalog. */
  @Override
  public Audit auditInfo() {
    return auditInfo;
  }

  /** @return The namespace of the catalog. */
  public Namespace namespace() {
    return namespace;
  }
}
