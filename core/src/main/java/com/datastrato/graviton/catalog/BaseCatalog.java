/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog;

import java.util.Map;

import com.datastrato.graviton.Audit;
import com.datastrato.graviton.Catalog;
import com.datastrato.graviton.CatalogProvider;
import com.datastrato.graviton.meta.CatalogEntity;
import com.google.common.base.Preconditions;

/**
 * The abstract base class for Catalog implementations.
 *
 * <p>A typical catalog is combined by two objects, one is {@link CatalogEntity} which represents
 * the metadata of the catalog, the other is {@link CatalogOperations} which is used to trigger the
 * specific operations by the catalog.
 *
 * <p>For example, a hive catalog has a {@link CatalogEntity} metadata and a {@link
 * CatalogOperations} which manipulates Hive DBs and tables.
 */
public abstract class BaseCatalog<T extends BaseCatalog> implements Catalog, CatalogProvider {

  private CatalogEntity entity;

  private Map<String, String> conf;

  private volatile CatalogOperations ops;

  protected abstract CatalogOperations newOps(Map<String, String> config);

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

  public T withCatalogEntity(CatalogEntity entity) {
    this.entity = entity;
    return (T) this;
  }

  public T withCatalogConf(Map<String, String> conf) {
    this.conf = conf;
    return (T) this;
  }

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
