/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog;

import com.datastrato.graviton.Audit;
import com.datastrato.graviton.Catalog;
import com.datastrato.graviton.CatalogProvider;
import com.datastrato.graviton.meta.CatalogEntity;
import com.google.common.base.Preconditions;
import java.util.Map;

/**
 * The abstract base class for Catalog implementations.
 *
 * <p>A typical catalog is combined by two objects, one is {@link CatalogEntity} which represents
 * the metadata of the catalog, the other is {@link CatalogOperations} which is used to trigger the
 * specific operations by the catalog.
 *
 * <p>For example, a hive catalog has a {@link CatalogEntity} metadata and a {@link
 * CatalogOperations} which manipulates Hive DBs and tables.
 *
 * @param <T> The type of the concrete subclass of BaseCatalog.
 */
public abstract class BaseCatalog<T extends BaseCatalog> implements Catalog, CatalogProvider {

  private CatalogEntity entity;

  private Map<String, String> conf;

  private volatile CatalogOperations ops;

  /**
   * Creates a new instance of CatalogOperations.
   *
   * @param config The configuration parameters for creating CatalogOperations.
   * @return A new instance of CatalogOperations.
   */
  protected abstract CatalogOperations newOps(Map<String, String> config);

  /**
   * Retrieves the CatalogOperations instance associated with this catalog. Lazily initializes the
   * instance if not already created.
   *
   * @return The CatalogOperations instance.
   * @throws IllegalArgumentException If the entity or configuration is not set.
   */
  public CatalogOperations ops() {
    if (ops == null) {
      synchronized (this) {
        if (ops == null) {
          Preconditions.checkArgument(
              entity != null && conf != null, "entity and conf must be set before calling ops()");
          ops = newOps(conf);
        }
      }
    }

    return ops;
  }

  /**
   * Sets the CatalogEntity for this catalog.
   *
   * @param entity The CatalogEntity representing the metadata of the catalog.
   * @return The instance of the concrete subclass of BaseCatalog.
   */
  public T withCatalogEntity(CatalogEntity entity) {
    this.entity = entity;
    return (T) this;
  }

  /**
   * Sets the configuration for this catalog.
   *
   * @param conf The configuration parameters as a map.
   * @return The instance of the concrete subclass of BaseCatalog.
   */
  public T withCatalogConf(Map<String, String> conf) {
    this.conf = conf;
    return (T) this;
  }

  /**
   * Retrieves the CatalogEntity associated with this catalog.
   *
   * @return The CatalogEntity instance.
   */
  public CatalogEntity entity() {
    return entity;
  }

  @Override
  public String name() {
    Preconditions.checkArgument(entity != null, "entity is not set");
    return entity.name();
  }

  @Override
  public Type type() {
    Preconditions.checkArgument(entity != null, "entity is not set");
    return entity.getType();
  }

  @Override
  public String provider() {
    Preconditions.checkArgument(entity != null, "entity is not set");
    return entity.getProvider();
  }

  @Override
  public String comment() {
    Preconditions.checkArgument(entity != null, "entity is not set");
    return entity.getComment();
  }

  @Override
  public Map<String, String> properties() {
    Preconditions.checkArgument(entity != null, "entity is not set");
    return entity.getProperties();
  }

  @Override
  public Audit auditInfo() {
    Preconditions.checkArgument(entity != null, "entity is not set");
    return entity.auditInfo();
  }
}
