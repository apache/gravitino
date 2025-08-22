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
package org.apache.gravitino.iceberg.common.ops;

import com.google.common.base.Preconditions;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.logging.LogFactory;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergCatalogBackend;
import org.apache.gravitino.iceberg.common.ClosableHiveCatalog;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.utils.IcebergCatalogUtil;
import org.apache.gravitino.utils.IsolatedClassLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.rest.CatalogHandlers;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper for Iceberg catalog backend, provides the common interface for Iceberg REST server and
 * Gravitino Iceberg catalog.
 */
public class IcebergCatalogWrapper implements AutoCloseable {

  public static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogWrapper.class);

  @Getter protected Catalog catalog;
  private SupportsNamespaces asNamespaceCatalog;
  private final IcebergCatalogBackend catalogBackend;
  private String catalogUri = null;
  private Map<String, String> catalogPropertiesMap;

  public IcebergCatalogWrapper(IcebergConfig icebergConfig) {
    this.catalogBackend =
        IcebergCatalogBackend.valueOf(
            icebergConfig.get(IcebergConfig.CATALOG_BACKEND).toUpperCase(Locale.ROOT));
    if (!IcebergCatalogBackend.MEMORY.equals(catalogBackend)
        && !IcebergCatalogBackend.REST.equals(catalogBackend)) {
      // check whether IcebergConfig.CATALOG_WAREHOUSE exists
      if (StringUtils.isBlank(icebergConfig.get(IcebergConfig.CATALOG_WAREHOUSE))) {
        throw new IllegalArgumentException("The 'warehouse' parameter must have a value.");
      }
    }
    if (!IcebergCatalogBackend.MEMORY.equals(catalogBackend)) {
      this.catalogUri = icebergConfig.get(IcebergConfig.CATALOG_URI);
    }
    this.catalog = IcebergCatalogUtil.loadCatalogBackend(catalogBackend, icebergConfig);
    if (catalog instanceof SupportsNamespaces) {
      this.asNamespaceCatalog = (SupportsNamespaces) catalog;
    }

    this.catalogPropertiesMap = icebergConfig.getIcebergCatalogProperties();
  }

  private void validateNamespace(Optional<Namespace> namespace) {
    namespace.ifPresent(
        n -> Preconditions.checkArgument(!n.toString().isEmpty(), "Namespace couldn't be empty"));
    if (asNamespaceCatalog == null) {
      throw new UnsupportedOperationException(
          "The underlying catalog doesn't support namespace operation");
    }
  }

  private ViewCatalog getViewCatalog() {
    if (!(catalog instanceof ViewCatalog)) {
      throw new UnsupportedOperationException(catalog.name() + " is not support view");
    }
    return (ViewCatalog) catalog;
  }

  public CreateNamespaceResponse createNamespace(CreateNamespaceRequest request) {
    validateNamespace(Optional.of(request.namespace()));
    return CatalogHandlers.createNamespace(asNamespaceCatalog, request);
  }

  public void dropNamespace(Namespace namespace) {
    validateNamespace(Optional.of(namespace));
    CatalogHandlers.dropNamespace(asNamespaceCatalog, namespace);
  }

  public GetNamespaceResponse loadNamespace(Namespace namespace) {
    validateNamespace(Optional.of(namespace));
    return CatalogHandlers.loadNamespace(asNamespaceCatalog, namespace);
  }

  public boolean namespaceExists(Namespace namespace) {
    validateNamespace(Optional.of(namespace));
    return asNamespaceCatalog.namespaceExists(namespace);
  }

  public ListNamespacesResponse listNamespace(Namespace parent) {
    validateNamespace(Optional.empty());
    return CatalogHandlers.listNamespaces(asNamespaceCatalog, parent);
  }

  public UpdateNamespacePropertiesResponse updateNamespaceProperties(
      Namespace namespace, UpdateNamespacePropertiesRequest updateNamespacePropertiesRequest) {
    validateNamespace(Optional.of(namespace));
    return CatalogHandlers.updateNamespaceProperties(
        asNamespaceCatalog, namespace, updateNamespacePropertiesRequest);
  }

  public LoadTableResponse registerTable(Namespace namespace, RegisterTableRequest request) {
    return CatalogHandlers.registerTable(catalog, namespace, request);
  }

  /**
   * Reload hadoop configuration, this is useful when the hadoop configuration UserGroupInformation
   * is shared by multiple threads. UserGroupInformation#authenticationMethod was first initialized
   * in KerberosClient, however, when switching to iceberg-rest thread,
   * UserGroupInformation#authenticationMethod will be reset to the default value; we need to
   * reinitialize it again.
   */
  public void reloadHadoopConf() {
    Configuration configuration = new Configuration();
    this.catalogPropertiesMap.forEach(configuration::set);
    UserGroupInformation.setConfiguration(configuration);
  }

  public LoadTableResponse createTable(Namespace namespace, CreateTableRequest request) {
    request.validate();
    if (request.stageCreate()) {
      return CatalogHandlers.stageTableCreate(catalog, namespace, request);
    }
    return CatalogHandlers.createTable(catalog, namespace, request);
  }

  public void dropTable(TableIdentifier tableIdentifier) {
    CatalogHandlers.dropTable(catalog, tableIdentifier);
  }

  public void purgeTable(TableIdentifier tableIdentifier) {
    CatalogHandlers.purgeTable(catalog, tableIdentifier);
  }

  public LoadTableResponse loadTable(TableIdentifier tableIdentifier) {
    return CatalogHandlers.loadTable(catalog, tableIdentifier);
  }

  public boolean tableExists(TableIdentifier tableIdentifier) {
    return catalog.tableExists(tableIdentifier);
  }

  public ListTablesResponse listTable(Namespace namespace) {
    return CatalogHandlers.listTables(catalog, namespace);
  }

  public void renameTable(RenameTableRequest renameTableRequest) {
    CatalogHandlers.renameTable(catalog, renameTableRequest);
  }

  public LoadTableResponse updateTable(
      TableIdentifier tableIdentifier, UpdateTableRequest updateTableRequest) {
    return CatalogHandlers.updateTable(catalog, tableIdentifier, updateTableRequest);
  }

  public LoadTableResponse updateTable(IcebergTableChange icebergTableChange) {
    Transaction transaction = icebergTableChange.getTransaction();
    transaction.commitTransaction();
    return loadTable(icebergTableChange.getTableIdentifier());
  }

  public LoadViewResponse createView(Namespace namespace, CreateViewRequest request) {
    request.validate();
    return CatalogHandlers.createView(getViewCatalog(), namespace, request);
  }

  public LoadViewResponse updateView(TableIdentifier viewIdentifier, UpdateTableRequest request) {
    request.validate();
    return CatalogHandlers.updateView(getViewCatalog(), viewIdentifier, request);
  }

  public LoadViewResponse loadView(TableIdentifier viewIdentifier) {
    return CatalogHandlers.loadView(getViewCatalog(), viewIdentifier);
  }

  public void dropView(TableIdentifier viewIdentifier) {
    CatalogHandlers.dropView(getViewCatalog(), viewIdentifier);
  }

  public void renameView(RenameTableRequest request) {
    request.validate();
    CatalogHandlers.renameView(getViewCatalog(), request);
  }

  public boolean viewExists(TableIdentifier viewIdentifier) {
    return getViewCatalog().viewExists(viewIdentifier);
  }

  public ListTablesResponse listView(Namespace namespace) {
    return CatalogHandlers.listViews(getViewCatalog(), namespace);
  }

  public boolean supportsViewOperations() {
    return catalog instanceof ViewCatalog;
  }

  @Override
  public void close() throws Exception {
    if (catalog instanceof AutoCloseable) {
      // JdbcCatalog and WrappedHiveCatalog need close.
      ((AutoCloseable) catalog).close();
    }

    // Because each catalog in Gravitino has its own classloader, after a catalog is no longer used
    // for a long time or dropped, the instance of classloader needs to be released. In order to
    // let JVM GC remove the classloader, we need to release the resources of the classloader. The
    // resources include the driver of the catalog backend and the
    // AbandonedConnectionCleanupThread of MySQL. For more information about
    // AbandonedConnectionCleanupThread, please refer to the corresponding java doc of MySQL
    // driver.
    if (catalogUri != null && catalogUri.contains("mysql")) {
      closeMySQLCatalogResource();
    } else if (catalogUri != null && catalogUri.contains("postgresql")) {
      closePostgreSQLCatalogResource();
    } else if (catalogBackend.equals(IcebergCatalogBackend.HIVE)) {
      // TODO(yuqi) add close for other catalog types such Hive catalog, for more, please refer to
      // https://github.com/apache/gravitino/pull/2548/commits/ab876b69b7e094bbd8c174d48a2365a18ed5176d
    }

    try {
      Field statisticsCleanerField =
          FieldUtils.getField(FileSystem.Statistics.class, "STATS_DATA_CLEANER", true);
      Object statisticsCleaner = statisticsCleanerField.get(null);
      if (statisticsCleaner != null) {
        ((Thread) statisticsCleaner).interrupt();
        ((Thread) statisticsCleaner).setContextClassLoader(null);
        ((Thread) statisticsCleaner).join();
      }

      FileSystem.closeAll();

      // Clear all thread references to the ClosableHiveCatalog class loader.
      Thread[] threads = getAllThreads();
      for (Thread thread : threads) {
        if (isPeerCacheThread(thread)) {
          LOG.info("Interrupting peer cache thread: {}", thread.getName());
          thread.setContextClassLoader(null);
          thread.interrupt();
          try {
            thread.join();
          } catch (InterruptedException e) {
            LOG.warn("Failed to join peer cache thread: {}", thread.getName(), e);
          }
        }
      }

      clearClassLoaderReferences();

      releaseClassLoaderReferences(this.getClass().getClassLoader());

      releaseReference();

    } catch (Exception e) {
      LOG.warn("Failed to close FileSystem statistics cleaner", e);
    }
  }

  private static Thread[] getAllThreads() {
    ThreadGroup rootGroup = Thread.currentThread().getThreadGroup();
    ThreadGroup parentGroup;
    while ((parentGroup = rootGroup.getParent()) != null) {
      rootGroup = parentGroup;
    }

    Thread[] threads = new Thread[rootGroup.activeCount()];
    while (rootGroup.enumerate(threads, true) == threads.length) {
      threads = new Thread[threads.length * 2];
    }
    return threads;
  }

  private static boolean isPeerCacheThread(Thread thread) {
    return thread != null
        //        && thread.getName() != null
        //        && thread.getName().contains("org.apache.hadoop.hdfs.PeerCache")
        && thread.getContextClassLoader() == ClosableHiveCatalog.class.getClassLoader();
  }

  public static void clearClassLoaderReferences() {
    try {
      Class<?> shutdownHooks = Class.forName("java.lang.ApplicationShutdownHooks");
      Field hooksField = shutdownHooks.getDeclaredField("hooks");
      hooksField.setAccessible(true);

      IdentityHashMap<Thread, Thread> hooks =
          (IdentityHashMap<Thread, Thread>) hooksField.get(null);

      hooks
          .entrySet()
          .removeIf(
              entry -> {
                Thread thread = entry.getKey();
                return thread.getContextClassLoader() == ClosableHiveCatalog.class.getClassLoader();
              });
    } catch (Exception e) {
      throw new RuntimeException("Failed to clean shutdown hooks", e);
    }
  }

  public static void releaseClassLoaderReferences(ClassLoader classLoader) {
    try {
      Class<?> logFactoryClass =
          Class.forName("org.apache.htrace.shaded.commons.logging.LogFactory");

      // 获取静态字段 thisClassLoader
      Field thisClassLoaderField = logFactoryClass.getDeclaredField("thisClassLoader");
      thisClassLoaderField.setAccessible(true);

      // 如果当前被引用的就是我们要清理的类加载器
      if (thisClassLoaderField.get(null) == classLoader) {
        // 重置为系统类加载器
        thisClassLoaderField.set(null, ClassLoader.getSystemClassLoader());
      }

      // 尝试清除工厂缓存
      try {
        Method release = logFactoryClass.getMethod("release", ClassLoader.class);
        release.invoke(null, classLoader);
      } catch (NoSuchMethodException e) {
        // 旧版本可能没有此方法
      }
    } catch (Exception e) {
      // 日志记录或处理异常
    }
  }

  public static void releaseReference() {
    try {

      LogFactory.release(IcebergCatalogWrapper.class.getClassLoader());

      Field cacheClass = FieldUtils.getDeclaredField(Configuration.class, "CACHE_CLASSES", true);
      cacheClass.setAccessible(true);
      Map<ClassLoader, Map<String, WeakReference<Class<?>>>> cacheClasses =
          (Map<ClassLoader, Map<String, WeakReference<Class<?>>>>) cacheClass.get(null);
      cacheClasses.clear();

      //      Field field = Proxy.class.getDeclaredField("proxyClassCache");
      //      Field modifiersField = Field.class.getDeclaredField("modifiers");
      //      modifiersField.setAccessible(true);
      //      modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
      //
      //      field.setAccessible(true);
      //      field.set(null, null);

      Field f = FieldUtils.getDeclaredField(UserGroupInformation.class, "conf", true);
      f.setAccessible(true);
      f.set(null, null);

      f = FieldUtils.getDeclaredField(UserGroupInformation.class, "loginUser", true);
      f.setAccessible(true);
      f.set(null, null);

      f = FieldUtils.getDeclaredField(FileSystem.Statistics.class, "STATS_DATA_CLEANER", true);
      f.setAccessible(true);
      Thread t = (Thread) f.get(null);
      t.interrupt();
      t.join();
      t.setContextClassLoader(null);

      // Clear all service loader class loaded by this class loader
      f = FieldUtils.getDeclaredField(SecurityUtil.class, "securityInfoProviders", true);
      f.setAccessible(true);
      ServiceLoader<SecurityInfo> securityInfoProviders = (ServiceLoader<SecurityInfo>) f.get(null);
      securityInfoProviders.reload();
      f.set(null, null);

      cacheClass = FieldUtils.getDeclaredField(WritableComparator.class, "comparators", true);
      cacheClass.setAccessible(true);
      ConcurrentHashMap<Class, WritableComparator> value =
          (ConcurrentHashMap<Class, WritableComparator>) cacheClass.get(null);
      value.clear();

    } catch (Exception e) {
      // 日志记录或处理异常
      LOG.warn("Failed to release references", e);
    }
  }

  private void closeMySQLCatalogResource() {
    try {
      // Close thread AbandonedConnectionCleanupThread if we are using `com.mysql.cj.jdbc.Driver`,
      // for driver `com.mysql.jdbc.Driver` (deprecated), the daemon thread maybe not this one.
      Class.forName("com.mysql.cj.jdbc.AbandonedConnectionCleanupThread")
          .getMethod("uncheckedShutdown")
          .invoke(null);
      LOG.info("AbandonedConnectionCleanupThread has been shutdown...");

      // Unload the MySQL driver, only Unload the driver if it is loaded by
      // IsolatedClassLoader.
      closeDriverLoadedByIsolatedClassLoader(catalogUri);
    } catch (Exception e) {
      LOG.warn("Failed to shutdown AbandonedConnectionCleanupThread or deregister MySQL driver", e);
    }
  }

  private void closeDriverLoadedByIsolatedClassLoader(String uri) {
    try {
      Driver driver = DriverManager.getDriver(uri);
      if (driver.getClass().getClassLoader().getClass()
          == IsolatedClassLoader.CUSTOM_CLASS_LOADER_CLASS) {
        DriverManager.deregisterDriver(driver);
        LOG.info("Driver {} has been deregistered...", driver);
      }
    } catch (Exception e) {
      LOG.warn("Failed to deregister driver", e);
    }
  }

  private void closePostgreSQLCatalogResource() {
    closeDriverLoadedByIsolatedClassLoader(catalogUri);
  }

  @Getter
  @Setter
  public static final class IcebergTableChange {

    private TableIdentifier tableIdentifier;
    private Transaction transaction;

    public IcebergTableChange(TableIdentifier tableIdentifier, Transaction transaction) {
      this.tableIdentifier = tableIdentifier;
      this.transaction = transaction;
    }
  }
}
