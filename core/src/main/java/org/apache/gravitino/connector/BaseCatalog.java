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
package org.apache.gravitino.connector;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogProvider;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.connector.authorization.BaseAuthorization;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.credential.CatalogCredentialManager;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.utils.IsolatedClassLoader;
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
    implements Catalog, CatalogProvider, HasPropertyMetadata, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(BaseCatalog.class);

  // This variable is used as a key in properties of catalogs to inject custom operation to
  // Gravitino.
  // You can use your own object to replace the default catalog operation.
  // The object you used is not stable, don't use it unless you know what you are doing.
  public static final String CATALOG_OPERATION_IMPL = "ops-impl";

  // Underlying access control system plugin for this catalog.
  private volatile AuthorizationPlugin authorizationPlugin;

  private CatalogEntity entity;

  private Map<String, String> conf;

  private volatile CatalogOperations ops;

  private volatile Capability capability;

  private volatile Map<String, String> properties;

  private volatile CatalogCredentialManager catalogCredentialManager;

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
   * Create a new instance of {@link Capability}, if the child class has special capabilities, it
   * should implement this method to provide a specific {@link Capability} instance regarding that
   * catalog.
   *
   * @return A new instance of {@link Capability}.
   */
  @Evolving
  protected Capability newCapability() {
    return Capability.DEFAULT;
  }

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
    throw new UnsupportedOperationException(
        "The catalog does not support table properties metadata");
  }

  @Override
  public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "The catalog does not support catalog properties metadata");
  }

  @Override
  public PropertiesMetadata schemaPropertiesMetadata() throws UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "The catalog does not support schema properties metadata");
  }

  @Override
  public PropertiesMetadata filesetPropertiesMetadata() throws UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "The catalog does not support fileset properties metadata");
  }

  @Override
  public PropertiesMetadata topicPropertiesMetadata() throws UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "The catalog does not support topic properties metadata");
  }

  @Override
  public PropertiesMetadata modelPropertiesMetadata() throws UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "The catalog does not support model properties metadata");
  }

  @Override
  public PropertiesMetadata modelVersionPropertiesMetadata() throws UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "The catalog does not support model version properties metadata");
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
          newOps.initialize(conf, entity.toCatalogInfo(), this);
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

  public AuthorizationPlugin getAuthorizationPlugin() {
    if (authorizationPlugin == null) {
      synchronized (this) {
        if (authorizationPlugin == null) {
          return null;
        }
      }
    }
    return authorizationPlugin;
  }

  public void initAuthorizationPluginInstance(IsolatedClassLoader classLoader) {
    if (authorizationPlugin == null) {
      synchronized (this) {
        if (authorizationPlugin == null) {
          String authorizationProvider =
              (String) catalogPropertiesMetadata().getOrDefault(conf, AUTHORIZATION_PROVIDER);
          if (authorizationProvider == null) {
            LOG.info("Authorization provider is not set!");
            return;
          }

          // use try-with-resources to auto-close authorization object if exit with exception
          try (BaseAuthorization<?> authorization =
              BaseAuthorization.createAuthorization(classLoader, authorizationProvider)) {

            authorizationPlugin =
                classLoader.withClassLoader(
                    cl ->
                        authorization.newPlugin(
                            entity.namespace().level(0), provider(), this.conf));

          } catch (Exception e) {
            LOG.error("Failed to load authorization with class loader", e);
            throw new RuntimeException(e);
          }
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (ops != null) {
      ops.close();
      ops = null;
    }
    if (authorizationPlugin != null) {
      authorizationPlugin.close();
      authorizationPlugin = null;
    }
    if (catalogCredentialManager != null) {
      catalogCredentialManager.close();
      catalogCredentialManager = null;
    }
  }

  public Capability capability() {
    if (capability == null) {
      synchronized (this) {
        if (capability == null) {
          capability = newCapability();
        }
      }
    }

    return capability;
  }

  public CatalogCredentialManager catalogCredentialManager() {
    if (catalogCredentialManager == null) {
      synchronized (this) {
        if (catalogCredentialManager == null) {
          this.catalogCredentialManager = new CatalogCredentialManager(name(), properties());
        }
      }
    }
    return catalogCredentialManager;
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
              .removeIf(entry -> catalogPropertiesMetadata().isHiddenProperty(entry.getKey()));
          tempProperties.putIfAbsent(
              PROPERTY_IN_USE,
              catalogPropertiesMetadata().getDefaultValue(PROPERTY_IN_USE).toString());

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
