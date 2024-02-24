/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import static com.datastrato.gravitino.StringIdentifier.ID_KEY;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.CatalogChange;
import com.datastrato.gravitino.CatalogChange.RemoveProperty;
import com.datastrato.gravitino.CatalogChange.SetProperty;
import com.datastrato.gravitino.CatalogProvider;
import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.Entity.EntityType;
import com.datastrato.gravitino.EntityAlreadyExistsException;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.StringIdentifier;
import com.datastrato.gravitino.SupportsCatalogs;
import com.datastrato.gravitino.exceptions.CatalogAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.file.FilesetCatalog;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.rel.SupportsSchemas;
import com.datastrato.gravitino.rel.TableCatalog;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.utils.IsolatedClassLoader;
import com.datastrato.gravitino.utils.PrincipalUtils;
import com.datastrato.gravitino.utils.ThrowableFunction;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages the catalog instances and operations. */
public class CatalogManager implements SupportsCatalogs, Closeable {

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

    public <R> R doWithPropertiesMeta(ThrowableFunction<HasPropertyMetadata, R> fn)
        throws Exception {
      return classLoader.withClassLoader(cl -> fn.apply(catalog.ops()));
    }

    public void close() {
      try {
        classLoader.withClassLoader(
            cl -> {
              if (catalog != null) {
                catalog.ops().close();
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

    boolean metalakeExists;
    try {
      metalakeExists = store.exists(metalakeIdent, EntityType.METALAKE);
    } catch (IOException e) {
      LOG.error("Failed to do storage operation", e);
      throw new RuntimeException(e);
    }

    if (!metalakeExists) {
      throw new NoSuchMetalakeException(METALAKE_DOES_NOT_EXIST_MSG, metalakeIdent);
    }

    try {
      return store.list(namespace, CatalogEntity.class, EntityType.CATALOG).stream()
          .map(entity -> NameIdentifier.of(namespace, entity.name()))
          .toArray(NameIdentifier[]::new);

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

    // load catalog-related configuration from catalog-specific configuration file
    Map<String, String> catalogSpecificConfig = loadCatalogSpecificConfig(provider);
    Map<String, String> mergedConfig = mergeConf(properties, catalogSpecificConfig);

    long uid = idGenerator.nextId();
    StringIdentifier stringId = StringIdentifier.fromId(uid);
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
                    .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                    .withCreateTime(Instant.now())
                    .build())
            .build();

    try {
      NameIdentifier metalakeIdent = NameIdentifier.of(ident.namespace().levels());
      if (!store.exists(metalakeIdent, EntityType.METALAKE)) {
        LOG.warn("Metalake {} does not exist", metalakeIdent);
        throw new NoSuchMetalakeException(METALAKE_DOES_NOT_EXIST_MSG, metalakeIdent);
      }

      // TODO: should avoid a race condition here
      CatalogWrapper wrapper = catalogCache.get(ident, id -> createCatalogWrapper(e));
      store.put(e, false /* overwrite */);
      return wrapper.catalog;
    } catch (EntityAlreadyExistsException e1) {
      LOG.warn("Catalog {} already exists", ident, e1);
      throw new CatalogAlreadyExistsException("Catalog %s already exists", ident);
    } catch (IllegalArgumentException | NoSuchMetalakeException e2) {
      throw e2;
    } catch (Exception e3) {
      catalogCache.invalidate(ident);
      LOG.error("Failed to create catalog {}", ident, e3);
      throw new RuntimeException(e3);
    }
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
            f.catalogPropertiesMetadata()
                .validatePropertyForAlter(alterProperty.getLeft(), alterProperty.getRight());
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
      return store.delete(ident, EntityType.CATALOG);
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

    IsolatedClassLoader classLoader;
    if (config.get(Configs.CATALOG_LOAD_ISOLATED)) {
      String pkgPath = buildPkgPath(conf, provider);
      String confPath = buildConfPath(provider);
      classLoader = IsolatedClassLoader.buildClassLoader(Lists.newArrayList(pkgPath, confPath));
    } else {
      // This will use the current class loader, it is mainly used for test.
      classLoader =
          new IsolatedClassLoader(
              Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    }

    // Load Catalog class instance
    BaseCatalog<?> catalog = createCatalogInstance(classLoader, provider);
    catalog.withCatalogConf(conf).withCatalogEntity(entity);

    CatalogWrapper wrapper = new CatalogWrapper(catalog, classLoader);
    // Validate catalog properties and initialize the config
    classLoader.withClassLoader(
        cl -> {
          Map<String, String> configWithoutId = Maps.newHashMap(conf);
          configWithoutId.remove(ID_KEY);
          catalog.ops().catalogPropertiesMetadata().validatePropertyForCreate(configWithoutId);

          // Call wrapper.catalog.properties() to make BaseCatalog#properties in IsolatedClassLoader
          // not null. Why we do this? Because wrapper.catalog.properties() need to be called in the
          // IsolatedClassLoader, it needs to load the specific catalog class such as HiveCatalog or
          // so. For simply, We will preload the value of properties and thus AppClassLoader can get
          // the value of properties.
          wrapper.catalog.properties();
          return null;
        },
        IllegalArgumentException.class);

    return wrapper;
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

  private Map<String, String> loadCatalogSpecificConfig(String provider) {
    if ("test".equals(provider)) {
      return Maps.newHashMap();
    }

    String catalogSpecificConfigFile = provider + ".conf";
    Map<String, String> catalogSpecificConfig = Maps.newHashMap();

    String fullPath = buildConfPath(provider) + File.separator + catalogSpecificConfigFile;
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
  private String buildConfPath(String provider) {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    Preconditions.checkArgument(gravitinoHome != null, "GRAVITINO_HOME not set");
    boolean testEnv = System.getenv("GRAVITINO_TEST") != null;
    if (testEnv) {
      return String.join(
          File.separator,
          gravitinoHome,
          "catalogs",
          "catalog-" + provider,
          "build",
          "resources",
          "main");
    }

    return String.join(File.separator, gravitinoHome, "catalogs", provider, "conf");
  }

  private String buildPkgPath(Map<String, String> conf, String provider) {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    Preconditions.checkArgument(gravitinoHome != null, "GRAVITINO_HOME not set");
    boolean testEnv = System.getenv("GRAVITINO_TEST") != null;

    String pkg = conf.get(Catalog.PROPERTY_PACKAGE);
    String pkgPath;
    if (pkg != null) {
      pkgPath = pkg;
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
