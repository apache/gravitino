/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastrato.graviton.Catalog;
import com.datastrato.graviton.CatalogChange;
import com.datastrato.graviton.CatalogProvider;
import com.datastrato.graviton.Config;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.SupportCatalogs;
import com.datastrato.graviton.configs;
import com.datastrato.graviton.catalog.CatalogManager.CatalogWrapper;
import com.datastrato.graviton.exceptions.CatalogAlreadyExistsException;
import com.datastrato.graviton.exceptions.NoSuchCatalogException;
import com.datastrato.graviton.exceptions.NoSuchMetalakeException;
import com.datastrato.graviton.meta.CatalogEntity;
import com.datastrato.graviton.rel.SupportsSchemas;
import com.datastrato.graviton.rel.TableCatalog;
import com.datastrato.graviton.util.IsolatedClassLoader;
import com.datastrato.graviton.util.ThrowableFunction;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.AbstractScheduledService.Scheduler;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class CatalogManager implements SupportCatalogs {

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

  private final Cache<NameIdentifier, CatalogWrapper> catalogCache;

  public CatalogManager(Config config) {
    this.config = config;
    long cacheEvictionIntervalInMs = config.get(configs.CATALOG_CACHE_EVICTION_INTERVAL_MS);

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
  public Catalog[] listCatalogs(Namespace namespace) throws NoSuchMetalakeException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Catalog loadCatalog(NameIdentifier ident) throws NoSuchCatalogException {
    return loadCatalogWrapper(ident).catalog;
  }

  @Override
  public Catalog createCatalog(
      NameIdentifier ident, Catalog.Type type, String comment, Map<String, String> properties)
      throws NoSuchMetalakeException, CatalogAlreadyExistsException {
    // Check if the metalake is existed, throw NoSuchMetalakeException if it is not existed.

    // Build CatalogEntity

    // Check and store CatalogEntity

    // Create CatalogWrapper and put it into catalogCache

    // return the created catalog

    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Catalog alterCatalog(NameIdentifier ident, CatalogChange... changes)
      throws NoSuchCatalogException, IllegalArgumentException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public boolean dropCatalog(NameIdentifier ident) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public CatalogWrapper loadCatalogWrapper(NameIdentifier ident) throws NoSuchCatalogException {
    return catalogCache.get(ident, this::loadCatalogInternal);
  }

  private CatalogWrapper loadCatalogInternal(NameIdentifier ident) throws NoSuchCatalogException {
    // Load CatalogEntity from CatalogStore

    // Create CatalogWrapper

    // return CatalogWrapper

    throw new UnsupportedOperationException("Not implemented yet");
  }

  private CatalogWrapper createCatalogWrapper(CatalogEntity entity) {
    Map<String, String> mergedConf =
        mergeConf(entity.getProperties(), catalogConf(entity.name(), config));

    // Building package path and class loader
    String provider = mergedConf.get(Catalog.PROPERTY_PROVIDER);
    Preconditions.checkArgument(
        provider != null,
        "'provider' not set in catalog properties or conf via "
            + "'graviton.catalog.<name>.provider'");

    String pkg = mergedConf.get(Catalog.PROPERTY_PACKAGE);
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

    IsolatedClassLoader classLoader = buildClassLoader(pkgPath);

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
            .map(p -> p.getClass())
            .collect(Collectors.toList());

    if (providers.size() == 0) {
      throw new IllegalArgumentException("No catalog provider found for: " + provider);
    } else if (providers.size() > 1) {
      throw new IllegalArgumentException("Multiple catalog providers found for: " + provider);
    } else {
      return Iterables.getOnlyElement(providers);
    }
  }
}
