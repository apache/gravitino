/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.connector;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.CatalogProvider;
import com.datastrato.gravitino.annotation.Evolving;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
@Evolving
public abstract class BaseCatalog<T extends BaseCatalog>
    implements Catalog, CatalogProvider, HasPropertyMetadata {
  private static final Logger LOG = LoggerFactory.getLogger(BaseCatalog.class);

  // A hack way to inject custom operation to Gravitino, the object you used is not stable, don't
  // use it unless you know what you are doing.
  @VisibleForTesting public static final String CATALOG_OPERATION_IMPL = "ops-impl";

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
   * Creates a new instance of CatalogOperations. The child class should implement this method to
   * provide a specific CatalogOperations instance regarding that catalog.
   *
   * @param config The configuration parameters for creating CatalogOperations.
   * @return A new instance of CatalogOperations.
   */
  @Evolving
  protected abstract CatalogOperations newOps(Map<String, String> config);

  /**
   * Create a new instance of ProxyPlugin, it is optional. If the child class needs to support the
   * specific proxy logic, it should implement this method to provide a specific ProxyPlugin.
   *
   * @param config The configuration parameters for creating ProxyPlugin.
   * @return A new instance of ProxyPlugin.
   */
  @Evolving
  protected Optional<ProxyPlugin> newProxyPlugin(Map<String, String> config) {
    return Optional.empty();
  }

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

  @Override
  public PropertiesMetadata topicPropertiesMetadata() throws UnsupportedOperationException {
    return ops().topicPropertiesMetadata();
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
          CatalogOperations newOps = createOps(conf);
          newOps.initialize(conf, entity.toCatalogInfo());
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

  private CatalogOperations createOps(Map<String, String> conf) {
    String customCatalogOperationClass = conf.get(CATALOG_OPERATION_IMPL);
    return Optional.ofNullable(customCatalogOperationClass)
        .map(className -> loadCustomOps(className))
        .orElse(newOps(conf));
  }

  private CatalogOperations loadCustomOps(String className) {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    try {
      // Class.forName use classloader of the caller class (BaseCatalog.class), it's global
      // classloader not the catalog specific classloader, so we must specify the classloader
      // explicitly.
      return (CatalogOperations)
          Class.forName(className, true, classLoader).getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      LOG.error("Failed to load custom catalog operations, {}", className, e);
      throw new RuntimeException(e);
    }
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

  private CatalogOperations asProxyOps(CatalogOperations ops, ProxyPlugin plugin) {
    return OperationsProxy.createProxy(ops, plugin);
  }
}
