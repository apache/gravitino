/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.CatalogProvider;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Optional;

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
public abstract class BaseCatalog<T extends BaseCatalog>
    implements Catalog, CatalogProvider, HasPropertyMetadata {

  private CatalogEntity entity;

  private Map<String, String> conf;

  private volatile CatalogOperations ops;

  private volatile Map<String, String> properties;

  private static String ENTITY_IS_NOT_SET = "entity is not set";

  // Any Gravitino configuration that starts with this prefix will be trim and passed to the
  // specific
  // catalog implementation. For example, if the configuration is
  // "gravitino.bypass.hive.metastore.uris",
  // then we will trim the prefix and pass "hive.metastore.uris" to the hive client configurations.
  public static final String CATALOG_BYPASS_PREFIX = "gravitino.bypass.";

  /**
   * Creates a new instance of CatalogOperations.
   *
   * @param config The configuration parameters for creating CatalogOperations.
   * @return A new instance of CatalogOperations.
   */
  protected abstract CatalogOperations newOps(Map<String, String> config);

  @Override
  public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
    return ops().tablePropertiesMetadata();
  }

  @Override
  public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
    return ops().catalogPropertiesMetadata();
  }

  @Override
  public PropertiesMetadata schemaPropertiesMetadata() throws UnsupportedOperationException {
    return ops().schemaPropertiesMetadata();
  }

  @Override
  public PropertiesMetadata filesetPropertiesMetadata() throws UnsupportedOperationException {
    return ops().filesetPropertiesMetadata();
  }

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
          CatalogOperations newOps = newOps(conf);
          ops =
              newProxyPlugin(conf)
                  .map(
                      proxyPlugin -> {
                        return asProxyOps(newOps, proxyPlugin);
                      })
                  .orElse(newOps);
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
    Preconditions.checkArgument(entity != null, ENTITY_IS_NOT_SET);
    return entity.name();
  }

  @Override
  public Type type() {
    Preconditions.checkArgument(entity != null, ENTITY_IS_NOT_SET);
    return entity.getType();
  }

  @Override
  public String provider() {
    Preconditions.checkArgument(entity != null, ENTITY_IS_NOT_SET);
    return entity.getProvider();
  }

  @Override
  public String comment() {
    Preconditions.checkArgument(entity != null, ENTITY_IS_NOT_SET);
    return entity.getComment();
  }

  @Override
  public Map<String, String> properties() {
    if (properties == null) {
      synchronized (this) {
        if (properties == null) {
          Preconditions.checkArgument(entity != null, ENTITY_IS_NOT_SET);
          Map<String, String> tempProperties = Maps.newHashMap(entity.getProperties());
          tempProperties
              .entrySet()
              .removeIf(
                  entry -> ops().catalogPropertiesMetadata().isHiddenProperty(entry.getKey()));
          properties = tempProperties;
        }
      }
    }
    return properties;
  }

  @Override
  public Audit auditInfo() {
    Preconditions.checkArgument(entity != null, ENTITY_IS_NOT_SET);
    return entity.auditInfo();
  }

  protected CatalogOperations asProxyOps(CatalogOperations ops, ProxyPlugin plugin) {
    return OperationsProxy.createProxy(ops, plugin);
  }

  protected Optional<ProxyPlugin> newProxyPlugin(Map<String, String> config) {
    return Optional.empty();
  }
}
