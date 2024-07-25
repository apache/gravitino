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
package org.apache.gravitino.catalog;

import static org.apache.gravitino.StringIdentifier.DUMMY_ID;
import static org.apache.gravitino.StringIdentifier.ID_KEY;
import static org.apache.gravitino.catalog.PropertiesMetadataHelpers.validatePropertyForAlter;
import static org.apache.gravitino.catalog.PropertiesMetadataHelpers.validatePropertyForCreate;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.CatalogChange.RemoveProperty;
import org.apache.gravitino.CatalogChange.SetProperty;
import org.apache.gravitino.CatalogProvider;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.PropertyEntry;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.CatalogAlreadyExistsException;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.file.FilesetCatalog;
import org.apache.gravitino.messaging.TopicCatalog;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.rel.SupportsPartitions;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.IsolatedClassLoader;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.gravitino.utils.ThrowableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages the catalog instances and operations. */
public class CatalogManager implements CatalogDispatcher, Closeable {

  private static final String CATALOG_DOES_NOT_EXIST_MSG = "Catalog %s does not exist";
  private static final String METALAKE_DOES_NOT_EXIST_MSG = "Metalake %s does not exist";

  private static final Logger LOG = LoggerFactory.getLogger(CatalogManager.class);

  /** Wrapper class for a catalog instance and its class loader. */
  public static class CatalogWrapper {
    private BaseCatalog catalog;
    private IsolatedClassLoader classLoader;

    public CatalogWrapper(BaseCatalog catalog, IsolatedClassLoader classLoader) {
      this.catalog = catalog;
      this.classLoader = classLoader;
    }

    public <R> R doWithSchemaOps(ThrowableFunction<SupportsSchemas, R> fn) throws Exception {
      return classLoader.withClassLoader(
          cl -> {
            if (asSchemas() == null) {
              throw new UnsupportedOperationException("Catalog does not support schema operations");
            }
            return fn.apply(asSchemas());
          });
    }

    public <R> R doWithTableOps(ThrowableFunction<TableCatalog, R> fn) throws Exception {
      return classLoader.withClassLoader(
          cl -> {
            if (asTables() == null) {
              throw new UnsupportedOperationException("Catalog does not support table operations");
            }
            return fn.apply(asTables());
          });
    }

    public <R> R doWithFilesetOps(ThrowableFunction<FilesetCatalog, R> fn) throws Exception {
      return classLoader.withClassLoader(
          cl -> {
            if (asFilesets() == null) {
              throw new UnsupportedOperationException(
                  "Catalog does not support fileset operations");
            }
            return fn.apply(asFilesets());
          });
    }

    public <R> R doWithTopicOps(ThrowableFunction<TopicCatalog, R> fn) throws Exception {
      return classLoader.withClassLoader(
          cl -> {
            if (asTopics() == null) {
              throw new UnsupportedOperationException("Catalog does not support topic operations");
            }
            return fn.apply(asTopics());
          });
    }

    public <R> R doWithCatalogOps(ThrowableFunction<CatalogOperations, R> fn) throws Exception {
      return classLoader.withClassLoader(cl -> fn.apply(catalog.ops()));
    }

    public <R> R doWithPartitionOps(
        NameIdentifier tableIdent, ThrowableFunction<SupportsPartitions, R> fn) throws Exception {
      return classLoader.withClassLoader(
          cl -> {
            Preconditions.checkArgument(
                asTables() != null, "Catalog does not support table operations");
            Table table = asTables().loadTable(tableIdent);
            Preconditions.checkArgument(
                table.supportPartitions() != null, "Table does not support partition operations");
            return fn.apply(table.supportPartitions());
          });
    }

    public <R> R doWithPropertiesMeta(ThrowableFunction<HasPropertyMetadata, R> fn)
        throws Exception {
      return classLoader.withClassLoader(cl -> fn.apply(catalog));
    }

    public Capability capabilities() throws Exception {
      return classLoader.withClassLoader(cl -> catalog.capability());
    }

    public void close() {
      try {
        classLoader.withClassLoader(
            cl -> {
              if (catalog != null) {
                catalog.close();
              }
              catalog = null;
              return null;
            });
      } catch (Exception e) {
        LOG.warn("Failed to close catalog", e);
      }

      classLoader.close();
    }

    private SupportsSchemas asSchemas() {
      return catalog.ops() instanceof SupportsSchemas ? (SupportsSchemas) catalog.ops() : null;
    }

    private TableCatalog asTables() {
      return catalog.ops() instanceof TableCatalog ? (TableCatalog) catalog.ops() : null;
    }

    private FilesetCatalog asFilesets() {
      return catalog.ops() instanceof FilesetCatalog ? (FilesetCatalog) catalog.ops() : null;
    }

    private TopicCatalog asTopics() {
      return catalog.ops() instanceof TopicCatalog ? (TopicCatalog) catalog.ops() : null;
    }
  }

  private final Config config;

  @VisibleForTesting final Cache<NameIdentifier, CatalogWrapper> catalogCache;

  private final EntityStore store;

  private final IdGenerator idGenerator;

  /**
   * Constructs a CatalogManager instance.
   *
   * @param config The configuration for the manager.
   * @param store The entity store to use.
   * @param idGenerator The id generator to use.
   */
  public CatalogManager(Config config, EntityStore store, IdGenerator idGenerator) {
    this.config = config;
    this.store = store;
    this.idGenerator = idGenerator;

    long cacheEvictionIntervalInMs = config.get(Configs.CATALOG_CACHE_EVICTION_INTERVAL_MS);
    this.catalogCache =
        Caffeine.newBuilder()
            .expireAfterAccess(cacheEvictionIntervalInMs, TimeUnit.MILLISECONDS)
            .removalListener(
                (k, v, c) -> {
                  LOG.info("Closing catalog {}.", k);
                  ((CatalogWrapper) v).close();
                })
            .scheduler(
                Scheduler.forScheduledExecutorService(
                    new ScheduledThreadPoolExecutor(
                        1,
                        new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("catalog-cleaner-%d")
                            .build())))
            .build();
  }

  /**
   * Closes the CatalogManager and releases any resources associated with it. This method
   * invalidates all cached catalog instances and clears the cache.
   */
  @Override
  public void close() {
    catalogCache.invalidateAll();
  }

  /**
   * Lists the catalogs within the specified namespace.
   *
   * @param namespace The namespace for which to list catalogs.
   * @return An array of NameIdentifier objects representing the catalogs.
   * @throws NoSuchMetalakeException If the specified metalake does not exist.
   */
  @Override
  public NameIdentifier[] listCatalogs(Namespace namespace) throws NoSuchMetalakeException {
    NameIdentifier metalakeIdent = NameIdentifier.of(namespace.levels());
    checkMetalakeExists(metalakeIdent);

    try {
      return store.list(namespace, CatalogEntity.class, EntityType.CATALOG).stream()
          .map(entity -> NameIdentifier.of(namespace, entity.name()))
          .toArray(NameIdentifier[]::new);

    } catch (IOException ioe) {
      LOG.error("Failed to list catalogs in metalake {}", metalakeIdent, ioe);
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public Catalog[] listCatalogsInfo(Namespace namespace) throws NoSuchMetalakeException {
    NameIdentifier metalakeIdent = NameIdentifier.of(namespace.levels());
    checkMetalakeExists(metalakeIdent);

    try {
      List<CatalogEntity> catalogEntities =
          store.list(namespace, CatalogEntity.class, EntityType.CATALOG);

      // Using provider as key to avoid loading the same type catalog instance multiple times
      Map<String, Set<String>> hiddenProps = new HashMap<>();
      Multimaps.index(catalogEntities, CatalogEntity::getProvider)
          .asMap()
          .forEach((p, e) -> hiddenProps.put(p, getHiddenPropertyNames(e.iterator().next())));

      return catalogEntities.stream()
          .map(e -> e.toCatalogInfoWithoutHiddenProps(hiddenProps.get(e.getProvider())))
          .toArray(Catalog[]::new);
    } catch (IOException ioe) {
      LOG.error("Failed to list catalogs in metalake {}", metalakeIdent, ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Loads the catalog with the specified identifier.
   *
   * @param ident The identifier of the catalog to load.
   * @return The loaded catalog.
   * @throws NoSuchCatalogException If the specified catalog does not exist.
   */
  @Override
  public Catalog loadCatalog(NameIdentifier ident) throws NoSuchCatalogException {
    return loadCatalogAndWrap(ident).catalog;
  }

  /**
   * Creates a new catalog with the provided details.
   *
   * @param ident The identifier of the new catalog.
   * @param type The type of the new catalog.
   * @param provider The provider of the new catalog.
   * @param comment The comment for the new catalog.
   * @param properties The properties of the new catalog.
   * @return The created catalog.
   * @throws NoSuchMetalakeException If the specified metalake does not exist.
   * @throws CatalogAlreadyExistsException If a catalog with the same identifier already exists.
   */
  @Override
  public Catalog createCatalog(
      NameIdentifier ident,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws NoSuchMetalakeException, CatalogAlreadyExistsException {
    Map<String, String> mergedConfig = buildCatalogConf(provider, properties);

    long uid = idGenerator.nextId();
    StringIdentifier stringId = StringIdentifier.fromId(uid);
    Instant now = Instant.now();
    String creator = PrincipalUtils.getCurrentPrincipal().getName();
    CatalogEntity e =
        CatalogEntity.builder()
            .withId(uid)
            .withName(ident.name())
            .withNamespace(ident.namespace())
            .withType(type)
            .withProvider(provider)
            .withComment(comment)
            .withProperties(StringIdentifier.newPropertiesWithId(stringId, mergedConfig))
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(creator)
                    .withCreateTime(now)
                    .withLastModifier(creator)
                    .withLastModifiedTime(now)
                    .build())
            .build();

    boolean createSuccess = false;
    try {
      NameIdentifier metalakeIdent = NameIdentifier.of(ident.namespace().levels());
      if (!store.exists(metalakeIdent, EntityType.METALAKE)) {
        LOG.warn("Metalake {} does not exist", metalakeIdent);
        throw new NoSuchMetalakeException(METALAKE_DOES_NOT_EXIST_MSG, metalakeIdent);
      }

      store.put(e, false /* overwrite */);
      CatalogWrapper wrapper = catalogCache.get(ident, id -> createCatalogWrapper(e));
      createSuccess = true;
      return wrapper.catalog;
    } catch (EntityAlreadyExistsException e1) {
      LOG.warn("Catalog {} already exists", ident, e1);
      throw new CatalogAlreadyExistsException("Catalog %s already exists", ident);
    } catch (IllegalArgumentException | NoSuchMetalakeException e2) {
      throw e2;
    } catch (Exception e3) {
      catalogCache.invalidate(ident);
      LOG.error("Failed to create catalog {}", ident, e3);
      if (e3 instanceof RuntimeException) {
        throw (RuntimeException) e3;
      }
      throw new RuntimeException(e3);
    } finally {
      if (!createSuccess) {
        try {
          store.delete(ident, EntityType.CATALOG, true);
        } catch (IOException e4) {
          LOG.error("Failed to clean up catalog {}", ident, e4);
        }
      }
    }
  }

  /**
   * Test whether a catalog can be created with the specified parameters, without actually creating
   * it.
   *
   * @param ident The identifier of the catalog to be tested.
   * @param type the type of the catalog.
   * @param provider the provider of the catalog.
   * @param comment the comment of the catalog.
   * @param properties the properties of the catalog.
   */
  @Override
  public void testConnection(
      NameIdentifier ident,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties) {
    NameIdentifier metalakeIdent = NameIdentifier.of(ident.namespace().levels());
    try {
      if (!store.exists(metalakeIdent, EntityType.METALAKE)) {
        throw new NoSuchMetalakeException(METALAKE_DOES_NOT_EXIST_MSG, metalakeIdent);
      }

      if (store.exists(ident, EntityType.CATALOG)) {
        throw new CatalogAlreadyExistsException("Catalog %s already exists", ident);
      }

      Map<String, String> mergedConfig = buildCatalogConf(provider, properties);
      Instant now = Instant.now();
      String creator = PrincipalUtils.getCurrentPrincipal().getName();
      CatalogEntity dummyEntity =
          CatalogEntity.builder()
              .withId(DUMMY_ID.id())
              .withName(ident.name())
              .withNamespace(ident.namespace())
              .withType(type)
              .withProvider(provider)
              .withComment(comment)
              .withProperties(StringIdentifier.newPropertiesWithId(DUMMY_ID, mergedConfig))
              .withAuditInfo(
                  AuditInfo.builder()
                      .withCreator(creator)
                      .withCreateTime(now)
                      .withLastModifier(creator)
                      .withLastModifiedTime(now)
                      .build())
              .build();

      CatalogWrapper wrapper = createCatalogWrapper(dummyEntity);
      wrapper.doWithCatalogOps(
          c -> {
            c.testConnection(ident, type, provider, comment, mergedConfig);
            return null;
          });
    } catch (GravitinoRuntimeException e) {
      throw e;
    } catch (Exception e) {
      LOG.warn("Failed to test catalog creation {}", ident, e);
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new RuntimeException(e);
    }
  }

  /**
   * Alters an existing catalog with the specified changes.
   *
   * @param ident The identifier of the catalog to alter.
   * @param changes The changes to apply to the catalog.
   * @return The altered catalog.
   * @throws NoSuchCatalogException If the specified catalog does not exist.
   * @throws IllegalArgumentException If an unsupported catalog change is provided.
   */
  @Override
  public Catalog alterCatalog(NameIdentifier ident, CatalogChange... changes)
      throws NoSuchCatalogException, IllegalArgumentException {
    // There could be a race issue that someone is using the catalog from cache while we are
    // updating it.

    CatalogWrapper catalogWrapper = loadCatalogAndWrap(ident);
    if (catalogWrapper == null) {
      throw new NoSuchCatalogException(CATALOG_DOES_NOT_EXIST_MSG, ident);
    }

    try {
      catalogWrapper.doWithPropertiesMeta(
          f -> {
            Pair<Map<String, String>, Map<String, String>> alterProperty =
                getCatalogAlterProperty(changes);
            validatePropertyForAlter(
                f.catalogPropertiesMetadata(), alterProperty.getLeft(), alterProperty.getRight());
            return null;
          });
    } catch (IllegalArgumentException e1) {
      throw e1;
    } catch (Exception e) {
      LOG.error("Failed to alter catalog {}", ident, e);
      throw new RuntimeException(e);
    }

    catalogCache.invalidate(ident);
    try {
      CatalogEntity updatedCatalog =
          store.update(
              ident,
              CatalogEntity.class,
              EntityType.CATALOG,
              catalog -> {
                CatalogEntity.Builder newCatalogBuilder =
                    CatalogEntity.builder()
                        .withId(catalog.id())
                        .withName(catalog.name())
                        .withNamespace(ident.namespace())
                        .withType(catalog.getType())
                        .withProvider(catalog.getProvider())
                        .withComment(catalog.getComment());

                AuditInfo newInfo =
                    AuditInfo.builder()
                        .withCreator(catalog.auditInfo().creator())
                        .withCreateTime(catalog.auditInfo().createTime())
                        .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                        .withLastModifiedTime(Instant.now())
                        .build();
                newCatalogBuilder.withAuditInfo(newInfo);

                Map<String, String> newProps =
                    catalog.getProperties() == null
                        ? new HashMap<>()
                        : new HashMap<>(catalog.getProperties());
                newCatalogBuilder = updateEntity(newCatalogBuilder, newProps, changes);

                return newCatalogBuilder.build();
              });
      return catalogCache.get(
              updatedCatalog.nameIdentifier(), id -> createCatalogWrapper(updatedCatalog))
          .catalog;

    } catch (NoSuchEntityException ne) {
      LOG.warn("Catalog {} does not exist", ident, ne);
      throw new NoSuchCatalogException(CATALOG_DOES_NOT_EXIST_MSG, ident);

    } catch (IllegalArgumentException iae) {
      LOG.warn("Failed to alter catalog {} with unknown change", ident, iae);
      throw iae;

    } catch (IOException ioe) {
      LOG.error("Failed to alter catalog {}", ident, ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Drops (deletes) the catalog with the specified identifier.
   *
   * @param ident The identifier of the catalog to drop.
   * @return {@code true} if the catalog was successfully dropped, {@code false} otherwise.
   */
  @Override
  public boolean dropCatalog(NameIdentifier ident) {
    // There could be a race issue that someone is using the catalog while we are dropping it.
    catalogCache.invalidate(ident);

    try {
      CatalogEntity catalogEntity = store.get(ident, EntityType.CATALOG, CatalogEntity.class);
      if (catalogEntity.getProvider().equals("kafka")) {
        // Kafka catalog needs to cascade drop the default schema
        List<SchemaEntity> schemas =
            store.list(
                Namespace.of(ident.namespace().level(0), ident.name()),
                SchemaEntity.class,
                EntityType.SCHEMA);
        // If there is only one schema, it must be the default schema, because we don't allow to
        // drop the default schema.
        if (schemas.size() == 1) {
          return store.delete(ident, EntityType.CATALOG, true);
        }
      }
      return store.delete(ident, EntityType.CATALOG);
    } catch (NoSuchEntityException e) {
      return false;
    } catch (IOException ioe) {
      LOG.error("Failed to drop catalog {}", ident, ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Loads the catalog with the specified identifier, wraps it in a CatalogWrapper, and caches the
   * wrapper for reuse.
   *
   * @param ident The identifier of the catalog to load.
   * @return The wrapped CatalogWrapper containing the loaded catalog.
   * @throws NoSuchCatalogException If the specified catalog does not exist.
   */
  public CatalogWrapper loadCatalogAndWrap(NameIdentifier ident) throws NoSuchCatalogException {
    return catalogCache.get(ident, this::loadCatalogInternal);
  }

  private Map<String, String> buildCatalogConf(String provider, Map<String, String> properties) {
    Map<String, String> newProperties = Optional.ofNullable(properties).orElse(Maps.newHashMap());
    // load catalog-related configuration from catalog-specific configuration file
    Map<String, String> catalogSpecificConfig = loadCatalogSpecificConfig(newProperties, provider);
    return mergeConf(newProperties, catalogSpecificConfig);
  }

  private Pair<Map<String, String>, Map<String, String>> getCatalogAlterProperty(
      CatalogChange... catalogChanges) {
    Map<String, String> upserts = Maps.newHashMap();
    Map<String, String> deletes = Maps.newHashMap();

    Arrays.stream(catalogChanges)
        .forEach(
            catalogChange -> {
              if (catalogChange instanceof SetProperty) {
                SetProperty setProperty = (SetProperty) catalogChange;
                upserts.put(setProperty.getProperty(), setProperty.getValue());
              } else if (catalogChange instanceof RemoveProperty) {
                RemoveProperty removeProperty = (RemoveProperty) catalogChange;
                deletes.put(removeProperty.getProperty(), removeProperty.getProperty());
              }
            });

    return Pair.of(upserts, deletes);
  }

  private void checkMetalakeExists(NameIdentifier ident) throws NoSuchMetalakeException {
    try {
      if (!store.exists(ident, EntityType.METALAKE)) {
        throw new NoSuchMetalakeException(METALAKE_DOES_NOT_EXIST_MSG, ident);
      }
    } catch (IOException e) {
      LOG.error("Failed to do storage operation", e);
      throw new RuntimeException(e);
    }
  }

  private CatalogWrapper loadCatalogInternal(NameIdentifier ident) throws NoSuchCatalogException {
    try {
      CatalogEntity entity = store.get(ident, EntityType.CATALOG, CatalogEntity.class);
      return createCatalogWrapper(entity);

    } catch (NoSuchEntityException ne) {
      LOG.warn("Catalog {} does not exist", ident, ne);
      throw new NoSuchCatalogException(CATALOG_DOES_NOT_EXIST_MSG, ident);

    } catch (IOException ioe) {
      LOG.error("Failed to load catalog {}", ident, ioe);
      throw new RuntimeException(ioe);
    }
  }

  private CatalogWrapper createCatalogWrapper(CatalogEntity entity) {
    Map<String, String> conf = entity.getProperties();
    String provider = entity.getProvider();

    IsolatedClassLoader classLoader = createClassLoader(provider, conf);
    BaseCatalog<?> catalog = createBaseCatalog(classLoader, entity);

    CatalogWrapper wrapper = new CatalogWrapper(catalog, classLoader);
    // Validate catalog properties and initialize the config
    classLoader.withClassLoader(
        cl -> {
          Map<String, String> configWithoutId = Maps.newHashMap(conf);
          configWithoutId.remove(ID_KEY);
          validatePropertyForCreate(catalog.catalogPropertiesMetadata(), configWithoutId);

          // Call wrapper.catalog.properties() to make BaseCatalog#properties in IsolatedClassLoader
          // not null. Why do we do this? Because wrapper.catalog.properties() needs to be called in
          // the IsolatedClassLoader, as it needs to load the specific catalog class
          // such as HiveCatalog or similar. To simplify, we will preload the value of properties
          // so that AppClassLoader can get the value of properties.
          wrapper.catalog.properties();
          wrapper.catalog.capability();
          return null;
        },
        IllegalArgumentException.class);

    return wrapper;
  }

  private Set<String> getHiddenPropertyNames(CatalogEntity entity) {
    Map<String, String> conf = entity.getProperties();
    String provider = entity.getProvider();

    try (IsolatedClassLoader classLoader = createClassLoader(provider, conf)) {
      BaseCatalog<?> catalog = createBaseCatalog(classLoader, entity);
      return classLoader.withClassLoader(
          cl ->
              catalog.catalogPropertiesMetadata().propertyEntries().values().stream()
                  .filter(PropertyEntry::isHidden)
                  .map(PropertyEntry::getName)
                  .collect(Collectors.toSet()),
          RuntimeException.class);
    }
  }

  private BaseCatalog<?> createBaseCatalog(IsolatedClassLoader classLoader, CatalogEntity entity) {
    // Load Catalog class instance
    BaseCatalog<?> catalog = createCatalogInstance(classLoader, entity.getProvider());
    catalog.withCatalogConf(entity.getProperties()).withCatalogEntity(entity);
    return catalog;
  }

  private IsolatedClassLoader createClassLoader(String provider, Map<String, String> conf) {
    if (config.get(Configs.CATALOG_LOAD_ISOLATED)) {
      String pkgPath = buildPkgPath(conf, provider);
      String confPath = buildConfPath(conf, provider);
      return IsolatedClassLoader.buildClassLoader(Lists.newArrayList(pkgPath, confPath));
    } else {
      // This will use the current class loader, it is mainly used for test.
      return new IsolatedClassLoader(
          Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    }
  }

  private BaseCatalog<?> createCatalogInstance(IsolatedClassLoader classLoader, String provider) {
    BaseCatalog<?> catalog;
    try {
      catalog =
          classLoader.withClassLoader(
              cl -> {
                try {
                  Class<? extends CatalogProvider> providerClz =
                      lookupCatalogProvider(provider, cl);
                  return (BaseCatalog) providerClz.getDeclaredConstructor().newInstance();
                } catch (Exception e) {
                  LOG.error("Failed to load catalog with provider: {}", provider, e);
                  throw new RuntimeException(e);
                }
              });
    } catch (Exception e) {
      LOG.error("Failed to load catalog with class loader", e);
      throw new RuntimeException(e);
    }

    if (catalog == null) {
      throw new RuntimeException("Failed to load catalog with provider: " + provider);
    }
    return catalog;
  }

  private Map<String, String> loadCatalogSpecificConfig(
      Map<String, String> properties, String provider) {
    if ("test".equals(provider)) {
      return Maps.newHashMap();
    }

    String catalogSpecificConfigFile = provider + ".conf";
    Map<String, String> catalogSpecificConfig = Maps.newHashMap();

    String fullPath =
        buildConfPath(properties, provider) + File.separator + catalogSpecificConfigFile;
    try (InputStream inputStream = FileUtils.openInputStream(new File(fullPath))) {
      Properties loadProperties = new Properties();
      loadProperties.load(inputStream);
      loadProperties.forEach(
          (key, value) -> catalogSpecificConfig.put(key.toString(), value.toString()));
    } catch (Exception e) {
      LOG.warn(
          "Failed to load catalog specific configurations, file name: '{}'",
          catalogSpecificConfigFile,
          e);
    }
    return catalogSpecificConfig;
  }

  static Map<String, String> mergeConf(Map<String, String> properties, Map<String, String> conf) {
    Map<String, String> mergedConf = conf != null ? Maps.newHashMap(conf) : Maps.newHashMap();
    Optional.ofNullable(properties).ifPresent(mergedConf::putAll);
    return Collections.unmodifiableMap(mergedConf);
  }

  /**
   * Build the config path from the specific provider. Usually, the configuration file is under the
   * conf and conf and package are under the same directory.
   */
  private String buildConfPath(Map<String, String> properties, String provider) {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    Preconditions.checkArgument(gravitinoHome != null, "GRAVITINO_HOME not set");
    boolean testEnv = System.getenv("GRAVITINO_TEST") != null;

    String confPath;
    String pkg = properties.get(Catalog.PROPERTY_PACKAGE);
    if (pkg != null) {
      confPath = String.join(File.separator, pkg, "conf");
    } else if (testEnv) {
      confPath =
          String.join(
              File.separator,
              gravitinoHome,
              "catalogs",
              "catalog-" + provider,
              "build",
              "resources",
              "main");
    } else {
      confPath = String.join(File.separator, gravitinoHome, "catalogs", provider, "conf");
    }
    return confPath;
  }

  private String buildPkgPath(Map<String, String> conf, String provider) {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    Preconditions.checkArgument(gravitinoHome != null, "GRAVITINO_HOME not set");
    boolean testEnv = System.getenv("GRAVITINO_TEST") != null;

    String pkg = conf.get(Catalog.PROPERTY_PACKAGE);
    String pkgPath;
    if (pkg != null) {
      pkgPath = String.join(File.separator, pkg, "libs");
    } else if (testEnv) {
      // In test, the catalog package is under the build directory.
      pkgPath =
          String.join(
              File.separator, gravitinoHome, "catalogs", "catalog-" + provider, "build", "libs");
    } else {
      // In real environment, the catalog package is under the catalog directory.
      pkgPath = String.join(File.separator, gravitinoHome, "catalogs", provider, "libs");
    }

    return pkgPath;
  }

  private Class<? extends CatalogProvider> lookupCatalogProvider(String provider, ClassLoader cl) {
    ServiceLoader<CatalogProvider> loader = ServiceLoader.load(CatalogProvider.class, cl);

    List<Class<? extends CatalogProvider>> providers =
        Streams.stream(loader.iterator())
            .filter(p -> p.shortName().equalsIgnoreCase(provider))
            .map(CatalogProvider::getClass)
            .collect(Collectors.toList());

    if (providers.isEmpty()) {
      throw new IllegalArgumentException("No catalog provider found for: " + provider);
    } else if (providers.size() > 1) {
      throw new IllegalArgumentException("Multiple catalog providers found for: " + provider);
    } else {
      return Iterables.getOnlyElement(providers);
    }
  }

  private CatalogEntity.Builder updateEntity(
      CatalogEntity.Builder builder, Map<String, String> newProps, CatalogChange... changes) {
    for (CatalogChange change : changes) {
      if (change instanceof CatalogChange.RenameCatalog) {
        CatalogChange.RenameCatalog rename = (CatalogChange.RenameCatalog) change;

        if (Entity.SYSTEM_CATALOG_RESERVED_NAME.equals(
            ((CatalogChange.RenameCatalog) change).getNewName())) {
          throw new IllegalArgumentException(
              "Can't rename a catalog with with reserved name `system`");
        }

        builder.withName(rename.getNewName());

      } else if (change instanceof CatalogChange.UpdateCatalogComment) {
        CatalogChange.UpdateCatalogComment updateComment =
            (CatalogChange.UpdateCatalogComment) change;
        builder.withComment(updateComment.getNewComment());

      } else if (change instanceof CatalogChange.SetProperty) {
        CatalogChange.SetProperty setProperty = (CatalogChange.SetProperty) change;
        newProps.put(setProperty.getProperty(), setProperty.getValue());

      } else if (change instanceof CatalogChange.RemoveProperty) {
        CatalogChange.RemoveProperty removeProperty = (CatalogChange.RemoveProperty) change;
        newProps.remove(removeProperty.getProperty());

      } else {
        throw new IllegalArgumentException(
            "Unsupported catalog change: " + change.getClass().getSimpleName());
      }
    }

    return builder.withProperties(newProps);
  }
}
