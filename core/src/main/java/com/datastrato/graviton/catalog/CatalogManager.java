/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog;

import com.datastrato.graviton.Catalog;
import com.datastrato.graviton.CatalogChange;
import com.datastrato.graviton.CatalogProvider;
import com.datastrato.graviton.Config;
import com.datastrato.graviton.Configs;
import com.datastrato.graviton.Entity.EntityIdentifer;
import com.datastrato.graviton.Entity.EntityType;
import com.datastrato.graviton.EntityAlreadyExistsException;
import com.datastrato.graviton.EntityStore;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.NoSuchEntityException;
import com.datastrato.graviton.SupportsCatalogs;
import com.datastrato.graviton.exceptions.CatalogAlreadyExistsException;
import com.datastrato.graviton.exceptions.NoSuchCatalogException;
import com.datastrato.graviton.exceptions.NoSuchMetalakeException;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.meta.BaseMetalake;
import com.datastrato.graviton.meta.CatalogEntity;
import com.datastrato.graviton.rel.SupportsSchemas;
import com.datastrato.graviton.rel.TableCatalog;
import com.datastrato.graviton.util.IsolatedClassLoader;
import com.datastrato.graviton.util.ThrowableFunction;
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
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatalogManager implements SupportsCatalogs, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogManager.class);

  public static class CatalogWrapper {
    private BaseCatalog catalog;
    private IsolatedClassLoader classLoader;

    public CatalogWrapper(BaseCatalog catalog, IsolatedClassLoader classLoader) {
      this.catalog = catalog;
      this.classLoader = classLoader;
    }

    public <R> R doWithSchemaOps(ThrowableFunction<SupportsSchemas, R> fn) throws Exception {
      if (asSchemas() == null) {
        throw new UnsupportedOperationException("Catalog does not support schema operations");
      }

      return classLoader.withClassLoader(cl -> fn.apply(asSchemas()));
    }

    public <R> R doWithTableOps(ThrowableFunction<TableCatalog, R> fn) throws Exception {
      if (asTables() == null) {
        throw new UnsupportedOperationException("Catalog does not support table operations");
      }

      return classLoader.withClassLoader(cl -> fn.apply(asTables()));
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
  }

  private final Config config;

  @VisibleForTesting final Cache<NameIdentifier, CatalogWrapper> catalogCache;

  private final EntityStore store;

  public CatalogManager(Config config, EntityStore store) {
    this.config = config;
    this.store = store;

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

  @Override
  public void close() {
    catalogCache.invalidateAll();
  }

  @Override
  public NameIdentifier[] listCatalogs(Namespace namespace) throws NoSuchMetalakeException {
    NameIdentifier metalakeIdent = NameIdentifier.of(namespace.levels());

    boolean metalakeExists;
    try {
      metalakeExists = store.exists(EntityIdentifer.of(metalakeIdent, EntityType.CATALOG));
    } catch (IOException e) {
      LOG.error("Failed to do storage operation", e);
      throw new RuntimeException(e);
    }

    if (!metalakeExists) {
      throw new NoSuchMetalakeException("Metalake " + metalakeIdent + " does not exist");
    }

    try {
      // Start means we want to list all catalogs in the metalake
      NameIdentifier nameIdentifier = NameIdentifier.of(namespace, NameIdentifier.WILDCARD_FLAG);
      return store.list(EntityIdentifer.of(nameIdentifier, EntityType.CATALOG), CatalogEntity.class)
          .stream()
          .map(entity -> NameIdentifier.of(namespace, entity.name()))
          .toArray(NameIdentifier[]::new);

    } catch (IOException ioe) {
      LOG.error("Failed to list catalogs in metalake {}", metalakeIdent, ioe);
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public Catalog loadCatalog(NameIdentifier ident) throws NoSuchCatalogException {
    return loadCatalogAndWrap(ident).catalog;
  }

  @Override
  public Catalog createCatalog(
      NameIdentifier ident, Catalog.Type type, String comment, Map<String, String> properties)
      throws NoSuchMetalakeException, CatalogAlreadyExistsException {
    try {
      CatalogEntity entity =
          store.executeInTransaction(
              () -> {
                NameIdentifier metalakeIdent = NameIdentifier.of(ident.namespace().levels());
                BaseMetalake metalake =
                    store.get(
                        EntityIdentifer.of(metalakeIdent, EntityType.METALAKE), BaseMetalake.class);

                CatalogEntity e =
                    new CatalogEntity.Builder()
                        .withId(1L /* TODO. use ID generator */)
                        .withMetalakeId(metalake.getId())
                        .withName(ident.name())
                        .withNamespace(ident.namespace())
                        .withType(type)
                        .withComment(comment)
                        .withProperties(properties)
                        .withAuditInfo(
                            new AuditInfo.Builder()
                                .withCreator("graviton") /* TODO. Should change to real user */
                                .withCreateTime(Instant.now())
                                .build())
                        .build();

                store.put(e, false /* overwrite */);
                return e;
              });
      return catalogCache.get(ident, id -> createCatalogWrapper(entity)).catalog;

    } catch (NoSuchEntityException ne) {
      LOG.warn("Metalake {} does not exist", ident.namespace(), ne);
      throw new NoSuchMetalakeException("Metalake " + ident.namespace() + " does not exist");

    } catch (EntityAlreadyExistsException ee) {
      LOG.warn("Catalog {} already exists", ident, ee);
      throw new CatalogAlreadyExistsException("Catalog " + ident + " already exists");

    } catch (IOException ioe) {
      LOG.error("Failed to create catalog {}", ident, ioe);
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public Catalog alterCatalog(NameIdentifier ident, CatalogChange... changes)
      throws NoSuchCatalogException, IllegalArgumentException {
    // There could be a race issue that someone is using the catalog from cache while we are
    // updating it.
    catalogCache.invalidate(ident);

    try {
      CatalogEntity updatedCatalog =
          store.update(
              EntityIdentifer.of(ident, EntityType.CATALOG),
              CatalogEntity.class,
              catalog -> {
                CatalogEntity.Builder newCatalogBuilder =
                    new CatalogEntity.Builder()
                        .withId(catalog.getId())
                        .withMetalakeId(catalog.getMetalakeId())
                        .withName(catalog.name())
                        .withNamespace(ident.namespace())
                        .withType(catalog.getType())
                        .withComment(catalog.getComment());

                AuditInfo newInfo =
                    new AuditInfo.Builder()
                        .withCreator(catalog.auditInfo().creator())
                        .withCreateTime(catalog.auditInfo().createTime())
                        .withLastModifier(
                            catalog.auditInfo().creator()) /* TODO. We should use real user */
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
      throw new NoSuchCatalogException("Catalog " + ident + " does not exist");

    } catch (IllegalArgumentException iae) {
      LOG.warn("Failed to alter catalog {} with unknown change", ident, iae);
      throw iae;

    } catch (IOException ioe) {
      LOG.error("Failed to alter catalog {}", ident, ioe);
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public boolean dropCatalog(NameIdentifier ident) {
    // There could be a race issue that someone is using the catalog while we are dropping it.
    catalogCache.invalidate(ident);

    try {
      return store.delete(EntityIdentifer.of(ident, EntityType.CATALOG));
    } catch (IOException ioe) {
      LOG.error("Failed to drop catalog {}", ident, ioe);
      throw new RuntimeException(ioe);
    }
  }

  public CatalogWrapper loadCatalogAndWrap(NameIdentifier ident) throws NoSuchCatalogException {
    return catalogCache.get(ident, this::loadCatalogInternal);
  }

  private CatalogWrapper loadCatalogInternal(NameIdentifier ident) throws NoSuchCatalogException {
    try {
      CatalogEntity entity =
          store.get(EntityIdentifer.of(ident, EntityType.CATALOG), CatalogEntity.class);
      return createCatalogWrapper(entity);

    } catch (NoSuchEntityException ne) {
      LOG.warn("Catalog {} does not exist", ident, ne);
      throw new NoSuchCatalogException("Catalog " + ident + " does not exist");

    } catch (IOException ioe) {
      LOG.error("Failed to load catalog {}", ident, ioe);
      throw new RuntimeException(ioe);
    }
  }

  private CatalogWrapper createCatalogWrapper(CatalogEntity entity) {
    Map<String, String> mergedConf =
        mergeConf(entity.getProperties(), catalogConf(entity.name(), config));

    String provider = mergedConf.get(Catalog.PROPERTY_PROVIDER);
    Preconditions.checkArgument(
        provider != null,
        "'provider' not set in catalog properties or conf via "
            + "'graviton.catalog.<name>.provider'");

    IsolatedClassLoader classLoader;
    if (config.get(Configs.CATALOG_LOAD_ISOLATED)) {
      String pkgPath = buildPkgPath(mergedConf, provider);
      classLoader = buildClassLoader(pkgPath);
    } else {
      // This will use the current class loader, it is mainly used for test.
      classLoader =
          new IsolatedClassLoader(
              Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    }

    // Load Catalog class instance
    BaseCatalog catalog;
    try {
      catalog =
          classLoader.withClassLoader(
              cl -> {
                try {
                  Class<? extends CatalogProvider> providerClz =
                      lookupCatalogProvider(provider, cl);
                  return (BaseCatalog) providerClz.newInstance();
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

    // Initialize the catalog
    catalog = catalog.withCatalogEntity(entity).withCatalogConf(mergedConf);
    return new CatalogWrapper(catalog, classLoader);
  }

  static Map<String, String> mergeConf(Map<String, String> properties, Map<String, String> conf) {
    Map<String, String> mergedConf = conf != null ? Maps.newHashMap(conf) : Maps.newHashMap();
    Optional.ofNullable(properties).ifPresent(mergedConf::putAll);
    return Collections.unmodifiableMap(mergedConf);
  }

  private Map<String, String> catalogConf(String name, Config config) {
    String confPrefix = "graviton.catalog." + name + ".";
    return config.getConfigsWithPrefix(confPrefix);
  }

  private String buildPkgPath(Map<String, String> conf, String provider) {
    String pkg = conf.get(Catalog.PROPERTY_PACKAGE);

    String gravitonHome = System.getenv("GRAVITON_HOME");
    Preconditions.checkArgument(gravitonHome != null, "GRAVITON_HOME not set");
    boolean testEnv = System.getenv("GRAVITON_TEST") != null;

    String pkgPath;
    if (pkg != null) {
      pkgPath = pkg;
    } else if (!testEnv) {
      pkgPath = gravitonHome + File.separator + "catalogs" + File.separator + provider;
    } else {
      pkgPath =
          new StringBuilder()
              .append(gravitonHome)
              .append(File.separator)
              .append("catalog-")
              .append(provider)
              .append(File.separator)
              .append("build")
              .append(File.separator)
              .append("libs")
              .toString();
    }

    return pkgPath;
  }

  private IsolatedClassLoader buildClassLoader(String pkgPath) {
    // Listing all the jars under the package path and build the isolated class loader.
    File pkgFolder = new File(pkgPath);
    if (!pkgFolder.exists()
        || !pkgFolder.isDirectory()
        || !pkgFolder.canRead()
        || !pkgFolder.canExecute()) {
      throw new IllegalArgumentException("Invalid package path: " + pkgPath);
    }

    List<URL> jars = Lists.newArrayList();
    Arrays.stream(pkgFolder.listFiles())
        .forEach(
            f -> {
              try {
                jars.add(f.toURI().toURL());
              } catch (MalformedURLException e) {
                LOG.warn("Failed to read jar file: {}", f.getAbsolutePath(), e);
              }
            });

    return new IsolatedClassLoader(jars, Collections.emptyList(), Collections.emptyList());
  }

  private Class<? extends CatalogProvider> lookupCatalogProvider(String provider, ClassLoader cl) {
    ServiceLoader<CatalogProvider> loader = ServiceLoader.load(CatalogProvider.class, cl);

    List<Class<? extends CatalogProvider>> providers =
        Streams.stream(loader.iterator())
            .filter(p -> p.shortName().equalsIgnoreCase(provider))
            .map(CatalogProvider::getClass)
            .collect(Collectors.toList());

    if (providers.size() == 0) {
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
