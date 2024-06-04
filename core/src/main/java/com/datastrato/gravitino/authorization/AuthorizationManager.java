/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.exceptions.NoSuchAuthorizationException;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.utils.IsolatedClassLoader;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Closeable;
import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages the authorization instances and operations. */
public class AuthorizationManager implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(AuthorizationManager.class);

  public static final String AUTHORIZATION_PROVIDER = "AUTHORIZATION_PROVIDER";

  private final Config config;

  @VisibleForTesting final Cache<CatalogEntity, AuthorizationWrapper> authorizationCache;

  public AuthorizationManager(Config config) {
    this.config = config;

    long cacheEvictionIntervalInMs = config.get(Configs.AUTHORIZATION_CACHE_EVICTION_INTERVAL_MS);
    this.authorizationCache =
        Caffeine.newBuilder()
            .expireAfterAccess(cacheEvictionIntervalInMs, TimeUnit.MILLISECONDS)
            .removalListener(
                (k, v, c) -> {
                  LOG.info("Closing authorization {}.", k);
                  ((AuthorizationWrapper) v).close();
                })
            .scheduler(
                Scheduler.forScheduledExecutorService(
                    new ScheduledThreadPoolExecutor(
                        1,
                        new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("authorization-cleaner-%d")
                            .build())))
            .build();
  }

  @Override
  public void close() {
    authorizationCache.invalidateAll();
  }

  public AuthorizationWrapper loadAuthorizationAndWrap(CatalogEntity entity)
      throws NoSuchAuthorizationException {
    return authorizationCache.get(entity, this::createAuthorizationWrapper);
  }

  /** Wrapper class for an Authorization instance and its class loader. */
  public static class AuthorizationWrapper {
    private BaseAuthorization authorization;

    private IsolatedClassLoader classLoader;

    public AuthorizationWrapper(BaseAuthorization auth, IsolatedClassLoader classLoader) {
      this.authorization = auth;
      this.classLoader = classLoader;
    }

    public <R> boolean runAuthorizationChain(Function<AuthorizationOperations, R>... functions) {
      try {
        return classLoader.withClassLoader(
            cl -> {
              for (Function<AuthorizationOperations, R> function : functions) {
                function.apply(authorization.ops());
              }
              return true;
            });
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @VisibleForTesting
    public AuthorizationOperations getOps() {
      return authorization.ops();
    }

    public void close() {
      try {
        classLoader.withClassLoader(
            cl -> {
              if (authorization != null) {
                authorization.close();
              }
              authorization = null;
              return null;
            });
      } catch (Exception e) {
        LOG.warn("Failed to close authorization", e);
      }

      classLoader.close();
    }
  }

  @SafeVarargs
  public final <R> boolean runAuthorizationChain(
      CatalogEntity entity, Function<AuthorizationOperations, R>... functions) {
    return loadAuthorizationAndWrap(entity).runAuthorizationChain(functions);
  }

  private AuthorizationWrapper createAuthorizationWrapper(CatalogEntity entity) {
    Map<String, String> conf = entity.getProperties();
    String provider = conf.get(AUTHORIZATION_PROVIDER);

    IsolatedClassLoader classLoader = createClassLoader(provider, conf);
    BaseAuthorization<?> baseAuthorization = createBaseAuthorization(classLoader, entity);

    return new AuthorizationWrapper(baseAuthorization, classLoader);
  }

  private IsolatedClassLoader createClassLoader(String provider, Map<String, String> conf) {
    if (config.get(Configs.AUTHORIZATION_LOAD_ISOLATED)) {
      String pkgPath = buildPkgPath(conf, provider);
      String confPath = buildConfPath(conf, provider);
      return IsolatedClassLoader.buildClassLoader(Lists.newArrayList(pkgPath, confPath));
    } else {
      // This will use the current class loader, it is mainly used for test.
      return new IsolatedClassLoader(
          Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    }
  }

  private BaseAuthorization<?> createBaseAuthorization(
      IsolatedClassLoader classLoader, CatalogEntity entity) {
    // Load Authorization class instance
    BaseAuthorization<?> authorization =
        createAuthorizationInstance(
            classLoader, entity.getProperties().get(AUTHORIZATION_PROVIDER));
    authorization.withAuthorizationConf(entity.getProperties());
    return authorization;
  }

  private BaseAuthorization<?> createAuthorizationInstance(
      IsolatedClassLoader classLoader, String provider) {
    BaseAuthorization<?> authorization;
    try {
      authorization =
          classLoader.withClassLoader(
              cl -> {
                try {
                  Class<? extends AuthorizationProvider> providerClz =
                      lookupAuthorizationProvider(provider, cl);
                  return (BaseAuthorization) providerClz.getDeclaredConstructor().newInstance();
                } catch (Exception e) {
                  LOG.error("Failed to load authorization with provider: {}", provider, e);
                  throw new RuntimeException(e);
                }
              });
    } catch (Exception e) {
      LOG.error("Failed to load authorization with class loader", e);
      throw new RuntimeException(e);
    }

    if (authorization == null) {
      throw new RuntimeException("Failed to load authorization with provider: " + provider);
    }
    return authorization;
  }

  private Class<? extends AuthorizationProvider> lookupAuthorizationProvider(
      String provider, ClassLoader cl) {
    ServiceLoader<AuthorizationProvider> loader =
        ServiceLoader.load(AuthorizationProvider.class, cl);

    List<Class<? extends AuthorizationProvider>> providers =
        Streams.stream(loader.iterator())
            .filter(p -> p.shortName().equalsIgnoreCase(provider))
            .map(AuthorizationProvider::getClass)
            .collect(Collectors.toList());
    if (providers.isEmpty()) {
      throw new IllegalArgumentException("No authorization provider found for: " + provider);
    } else if (providers.size() > 1) {
      throw new IllegalArgumentException("Multiple authorization providers found for: " + provider);
    } else {
      return Iterables.getOnlyElement(providers);
    }
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
      // In test, the authorization package is under the build directory.
      pkgPath =
          String.join(
              File.separator,
              gravitinoHome,
              "authorizations",
              "authorization-" + provider,
              "build",
              "libs");
    } else {
      // In real environment, the authorization package is under the authorization directory.
      pkgPath = String.join(File.separator, gravitinoHome, "authorizations", provider, "libs");
    }

    return pkgPath;
  }

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
              "authorizations",
              "authorization-" + provider,
              "build",
              "resources",
              "main");
    } else {
      confPath = String.join(File.separator, gravitinoHome, "authorizations", provider, "conf");
    }
    return confPath;
  }
}
