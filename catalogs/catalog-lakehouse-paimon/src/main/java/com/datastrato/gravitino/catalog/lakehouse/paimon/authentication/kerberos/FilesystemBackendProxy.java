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
package com.datastrato.gravitino.catalog.lakehouse.paimon.authentication.kerberos;

import com.datastrato.gravitino.utils.PrincipalUtils;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.hadoop.HadoopFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Proxy class for FilesystemCatalog to support kerberos authentication. We can also make
 * FilesystemCatalog as a generic type and pass it as a parameter to the constructor.
 */
public class FilesystemBackendProxy implements MethodInterceptor {

  public static final Logger LOG = LoggerFactory.getLogger(FilesystemBackendProxy.class);

  // modified by gravitino.bypass.paimon.catalog.cache.max.capacity
  private static final String PAIMON_CATALOG_CACHE_MAX_CAPACITY_KEY =
      "paimon.catalog.cache.max.capacity";
  private static final int catalogCacheMaxCapacity = 100;
  // modified by gravitino.bypass.paimon.catalog.cache.expire.after.access
  private static final String PAIMON_CATALOG_CACHE_EXPIRE_AFTER_ACCESS_KEY =
      "paimon.catalog.cache.expire.after.access";
  private static final long catalogCacheExpireAfterAccess = 1000L * 60 * 60 * 3;
  private FileSystemCatalog target;
  private final CatalogContext catalogContext;
  private final String kerberosRealm;
  private final UserGroupInformation proxyUser;
  private final Cache<String, FileSystemCatalog> fileSystemCatalogCache;

  public FilesystemBackendProxy(
      FileSystemCatalog target, CatalogContext catalogContext, String kerberosRealm) {
    this.target = target;
    this.catalogContext = catalogContext;
    this.kerberosRealm = kerberosRealm;
    try {
      proxyUser = UserGroupInformation.getCurrentUser();
      ScheduledThreadPoolExecutor scheduler =
          new ScheduledThreadPoolExecutor(1, newDaemonThreadFactory());
      Options options = catalogContext.options();
      long maxCapacity =
          Long.parseLong(
              options.getString(
                  PAIMON_CATALOG_CACHE_MAX_CAPACITY_KEY, String.valueOf(catalogCacheMaxCapacity)));
      long expireAfterAccess =
          Long.parseLong(
              options.getString(
                  PAIMON_CATALOG_CACHE_EXPIRE_AFTER_ACCESS_KEY,
                  String.valueOf(catalogCacheExpireAfterAccess)));
      this.fileSystemCatalogCache =
          Caffeine.newBuilder()
              .maximumSize(maxCapacity)
              .expireAfterAccess(expireAfterAccess, TimeUnit.MILLISECONDS)
              .scheduler(Scheduler.forScheduledExecutorService(scheduler))
              .removalListener(
                  (key, value, cause) -> {
                    try {
                      FileSystemCatalog fileSystemCatalog = (FileSystemCatalog) value;
                      if (fileSystemCatalog != null) {
                        // TODO:
                        // If a large number of users access the Paimon catalog in a short period of
                        // time,
                        // it is easy to cause the FilesystemCatalog instance to be removed from the
                        // cache.
                        // When the FilesystemCatalog instance is removed from the cache,
                        // the Filesystem client is still in use.
                        // Closing immediately the Filesystem clients may cause exceptions.
                        closeFileSystem(fileSystemCatalog);
                      }
                    } catch (NoSuchFieldException | IllegalAccessException e) {
                      LOG.warn(
                          String.format(
                              "Cannot close the file system for Paimon catalog when removing cache. Removal cause: %s",
                              cause),
                          e);
                    }
                  })
              .build();
    } catch (IOException e) {
      throw new RuntimeException("Failed to get current user", e);
    }
  }

  @Override
  public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy)
      throws Throwable {

    String proxyKerberosPrincipalName = PrincipalUtils.getCurrentPrincipal().getName();
    if (!proxyKerberosPrincipalName.contains("@")) {
      proxyKerberosPrincipalName =
          String.format("%s@%s", proxyKerberosPrincipalName, kerberosRealm);
    }

    UserGroupInformation realUser =
        UserGroupInformation.createProxyUser(proxyKerberosPrincipalName, proxyUser);
    String finalProxyKerberosPrincipalName = proxyKerberosPrincipalName;

    return realUser.doAs(
        (PrivilegedExceptionAction<Object>)
            () -> {
              try {
                FileSystemCatalog catalogWithRealUser =
                    fileSystemCatalogCache.get(
                        finalProxyKerberosPrincipalName, this::createFileSystemCatalog);
                return methodProxy.invoke(catalogWithRealUser, objects);
              } catch (Throwable e) {
                if (RuntimeException.class.isAssignableFrom(e.getClass())) {
                  throw (RuntimeException) e;
                }
                throw new RuntimeException("Failed to invoke method", e);
              }
            });
  }

  public FileSystemCatalog getProxy() {
    Enhancer e = new Enhancer();
    e.setClassLoader(target.getClass().getClassLoader());
    e.setSuperclass(target.getClass());
    e.setCallback(this);
    // FileSystemCatalog does not have a no argument constructor.
    return (FileSystemCatalog)
        e.create(
            new Class[] {FileIO.class, Path.class, Options.class},
            new Object[] {
              target.fileIO(), new Path(target.warehouse()), new Options(target.options())
            });
  }

  private ThreadFactory newDaemonThreadFactory() {
    return new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("paimon-fileSystemCatalog-cache-cleaner" + "-%d")
        .build();
  }

  /**
   * Before invoking the doAs method, a Filesystem client has been created when initializing the
   * FilesystemCatalog. Therefore, the Filesystem client should be recreated within the doAs method
   * to ensure the ugi corresponds to the impersonated user.
   */
  private FileSystemCatalog createFileSystemCatalog(String proxyKerberosPrincipalName) {
    return (FileSystemCatalog) CatalogFactory.createCatalog(catalogContext);
  }

  private void closeFileSystem(FileSystemCatalog catalog)
      throws NoSuchFieldException, IllegalAccessException {
    Map<Pair<String, String>, FileSystem> fsMap = getPairFileSystemMap(catalog);
    fsMap
        .values()
        .forEach(
            fs -> {
              if (fs != null) {
                try {
                  fs.close();
                } catch (IOException e) {
                  LOG.warn("Failed to close Hadoop Filesystem client", e);
                }
              }
            });
  }

  private static Map<Pair<String, String>, FileSystem> getPairFileSystemMap(
      FileSystemCatalog catalog) throws NoSuchFieldException, IllegalAccessException {
    Class<?> superclass = catalog.getClass().getSuperclass();
    Field fileIO = superclass.getDeclaredField("fileIO");
    fileIO.setAccessible(true);
    HadoopFileIO hadoopFileIO = (HadoopFileIO) fileIO.get(catalog);
    Field fsMapField = hadoopFileIO.getClass().getDeclaredField("fsMap");
    fsMapField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<Pair<String, String>, FileSystem> fsMap =
        (Map<Pair<String, String>, FileSystem>) fsMapField.get(hadoopFileIO);
    return fsMap;
  }
}
