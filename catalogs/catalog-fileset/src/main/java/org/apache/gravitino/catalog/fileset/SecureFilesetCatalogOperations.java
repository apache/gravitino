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

package org.apache.gravitino.catalog.fileset;

import static org.apache.gravitino.file.Fileset.PROPERTY_DEFAULT_LOCATION_NAME;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import javax.security.auth.Subject;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.commons.logging.LogFactory;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.audit.CallerContext;
import org.apache.gravitino.catalog.FilesetFileOps;
import org.apache.gravitino.catalog.fileset.authentication.UserContext;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.connector.credential.PathContext;
import org.apache.gravitino.connector.credential.SupportsPathBasedCredentials;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.CredentialUtils;
import org.apache.gravitino.exceptions.FilesetAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchFilesetException;
import org.apache.gravitino.exceptions.NoSuchLocationNameException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.file.FileInfo;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetCatalog;
import org.apache.gravitino.file.FilesetChange;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("removal")
public class SecureFilesetCatalogOperations
    implements CatalogOperations,
        SupportsSchemas,
        FilesetCatalog,
        FilesetFileOps,
        SupportsPathBasedCredentials {

  public static final Logger LOG = LoggerFactory.getLogger(SecureFilesetCatalogOperations.class);

  private final FilesetCatalogOperations filesetCatalogOperations;

  public static final String GRAVITINO_KEYTAB_FORMAT = "keytabs/gravitino-%s";

  private UserContext catalogUserContext;

  private Map<String, String> catalogProperties;

  public SecureFilesetCatalogOperations() {
    this.filesetCatalogOperations = new FilesetCatalogOperations();
  }

  public SecureFilesetCatalogOperations(EntityStore store) {
    this.filesetCatalogOperations = new FilesetCatalogOperations(store);
  }

  @Override
  public void initialize(
      Map<String, String> config, CatalogInfo info, HasPropertyMetadata propertiesMetadata)
      throws RuntimeException {
    filesetCatalogOperations.initialize(config, info, propertiesMetadata);
    this.catalogUserContext =
        UserContext.getUserContext(
            NameIdentifier.of(info.namespace(), info.name()),
            config,
            filesetCatalogOperations.getHadoopConf(),
            info);
    this.catalogProperties = info.properties();
  }

  @VisibleForTesting
  public FilesetCatalogOperations getBaseFilesetCatalogOperations() {
    return filesetCatalogOperations;
  }

  @Override
  public Fileset createMultipleLocationFileset(
      NameIdentifier ident,
      String comment,
      Fileset.Type type,
      Map<String, String> storageLocations,
      Map<String, String> properties)
      throws NoSuchSchemaException, FilesetAlreadyExistsException {
    String apiUser = PrincipalUtils.getCurrentUserName();

    UserContext userContext =
        UserContext.getUserContext(
            ident, properties, null, filesetCatalogOperations.getCatalogInfo());
    return userContext.doAs(
        () -> {
          try {
            setUser(apiUser);
            return filesetCatalogOperations.createMultipleLocationFileset(
                ident, comment, type, storageLocations, properties);
          } finally {
            unsetUser(apiUser);
          }
        },
        ident);
  }

  @Override
  public boolean dropFileset(NameIdentifier ident) {
    FilesetEntity filesetEntity;
    try {
      filesetEntity =
          filesetCatalogOperations
              .store()
              .get(ident, Entity.EntityType.FILESET, FilesetEntity.class);
    } catch (NoSuchEntityException e) {
      LOG.warn("Fileset {} does not exist", ident);
      return false;
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to delete fileset " + ident, ioe);
    }

    UserContext userContext =
        UserContext.getUserContext(
            ident, filesetEntity.properties(), null, filesetCatalogOperations.getCatalogInfo());
    boolean r = userContext.doAs(() -> filesetCatalogOperations.dropFileset(ident), ident);
    UserContext.clearUserContext(ident);
    return r;
  }

  @Override
  public Schema createSchema(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    String apiUser = PrincipalUtils.getCurrentUserName();
    UserContext userContext =
        UserContext.getUserContext(
            ident, properties, null, filesetCatalogOperations.getCatalogInfo());
    return userContext.doAs(
        () -> {
          try {
            setUser(apiUser);
            return filesetCatalogOperations.createSchema(ident, comment, properties);
          } finally {
            unsetUser(apiUser);
          }
        },
        ident);
  }

  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    try {
      SchemaEntity schemaEntity =
          filesetCatalogOperations.store().get(ident, Entity.EntityType.SCHEMA, SchemaEntity.class);
      Map<String, String> properties =
          Optional.ofNullable(schemaEntity.properties()).orElse(Collections.emptyMap());

      UserContext userContext =
          UserContext.getUserContext(
              ident, properties, null, filesetCatalogOperations.getCatalogInfo());
      boolean r =
          userContext.doAs(() -> filesetCatalogOperations.dropSchema(ident, cascade), ident);
      UserContext.clearUserContext(ident);

      return r;
    } catch (NoSuchEntityException e) {
      LOG.warn("Schema {} does not exist", ident);
      return false;

    } catch (IOException ioe) {
      throw new RuntimeException("Failed to delete schema " + ident, ioe);
    }
  }

  @Override
  public Fileset alterFileset(NameIdentifier ident, FilesetChange... changes)
      throws NoSuchFilesetException, IllegalArgumentException {
    Fileset fileset = filesetCatalogOperations.alterFileset(ident, changes);

    String finalName = ident.name();
    for (FilesetChange change : changes) {
      if (change instanceof FilesetChange.RenameFileset) {
        finalName = ((FilesetChange.RenameFileset) change).getNewName();
      }
    }
    if (!ident.name().equals(finalName)) {
      UserContext.clearUserContext(NameIdentifier.of(ident.namespace(), finalName));
    }

    return fileset;
  }

  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    return filesetCatalogOperations.listSchemas(namespace);
  }

  @Override
  public Schema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    return filesetCatalogOperations.loadSchema(ident);
  }

  @Override
  public Schema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    return filesetCatalogOperations.alterSchema(ident, changes);
  }

  @Override
  public NameIdentifier[] listFilesets(Namespace namespace) throws NoSuchSchemaException {
    return filesetCatalogOperations.listFilesets(namespace);
  }

  @Override
  public FileInfo[] listFiles(NameIdentifier ident, String locationName, String subPath)
      throws NoSuchFilesetException, IOException {
    return filesetCatalogOperations.listFiles(ident, locationName, subPath);
  }

  @Override
  public Fileset loadFileset(NameIdentifier ident) throws NoSuchFilesetException {
    return filesetCatalogOperations.loadFileset(ident);
  }

  @Override
  public String getFileLocation(NameIdentifier ident, String subPath, String locationName)
      throws NoSuchFilesetException, NoSuchLocationNameException {
    return filesetCatalogOperations.getFileLocation(ident, subPath, locationName);
  }

  @Override
  public void close() throws IOException {
    filesetCatalogOperations.close();

    catalogUserContext.close();

    UserContext.cleanAllUserContext();

    try {

      closeStatsDataClearerInFileSystem();

      FileSystem.closeAll();

      // Clear all thread references to the ClosableHiveCatalog class loader.
      Thread[] threads = getAllThreads();
      for (Thread thread : threads) {
        // Clear thread local map for webserver threads in the current class loader
        clearThreadLocalMap(thread);

        // Close all threads that are using the FilesetCatalogOperations class loader
        if (runningWithCurrentClassLoader(thread)) {
          LOG.info("Interrupting peer cache thread: {}", thread.getName());
          thread.setContextClassLoader(null);
          thread.interrupt();
          try {
            thread.join(5000);
          } catch (InterruptedException e) {
            LOG.warn("Failed to join peer cache thread: {}", thread.getName(), e);
          }
        }
      }

      // Release the LogFactory for the FilesetCatalogOperations class loader
      LogFactory.release(SecureFilesetCatalogOperations.class.getClassLoader());

      // For Aws SDK metrics, unregister the metric admin MBean
      try {
        Class<?> methodUtilsClass = Class.forName("com.amazonaws.metrics.AwsSdkMetrics");
        MethodUtils.invokeStaticMethod(methodUtilsClass, "unregisterMetricAdminMBean");
      } catch (Exception e) {
        LOG.warn("Failed to unregister AWS SDK metrics admin MBean", e);
        // This is not critical, so we just log the warning
      }

      // For GCS
      try {
        Class<?> relocatedLogFactory =
            Class.forName("org.apache.gravitino.gcp.shaded.org.apache.commons.logging.LogFactory");
        MethodUtils.invokeStaticMethod(
            relocatedLogFactory, "release", SecureFilesetCatalogOperations.class.getClassLoader());
      } catch (Exception e) {
        LOG.warn("Failed to find GCS shaded LogFactory", e);
      }

      // For Azure
      try {
        Class<?> relocatedLogFactory =
            Class.forName(
                "org.apache.gravitino.azure.shaded.org.apache.commons.logging.LogFactory");
        MethodUtils.invokeStaticMethod(
            relocatedLogFactory, "release", SecureFilesetCatalogOperations.class.getClassLoader());

        // Clear timer in AbfsClientThrottlingAnalyzer
        Class<?> abfsClientThrottlingInterceptClass =
            Class.forName("org.apache.hadoop.fs.azurebfs.services.AbfsClientThrottlingIntercept");
        Object abfsClientThrottlingIntercept =
            FieldUtils.readStaticField(abfsClientThrottlingInterceptClass, "singleton", true);

        Object readThrottler =
            FieldUtils.readField(abfsClientThrottlingIntercept, "readThrottler", true);
        Object writeThrottler =
            FieldUtils.readField(abfsClientThrottlingIntercept, "writeThrottler", true);

        Timer readTimer = (Timer) FieldUtils.readField(readThrottler, "timer", true);
        readTimer.cancel();
        Timer writeTimer = (Timer) FieldUtils.readField(writeThrottler, "timer", true);
        writeTimer.cancel();
      } catch (Exception e) {
        LOG.warn("Failed to find GCS shaded LogFactory", e);
      }

      clearShutdownHooks();
    } catch (Exception e) {
      LOG.warn("Failed to clear FileSystem statistics cleaner thread", e);
    }
  }

  private static void clearThreadLocalMap(Thread thread) {
    if (thread != null && thread.getName().startsWith("Gravitino-webserver-")) {
      // try to
      try {
        Field threadLocalsField = Thread.class.getDeclaredField("threadLocals");
        threadLocalsField.setAccessible(true);
        Object threadLocalMap = threadLocalsField.get(thread);

        if (threadLocalMap != null) {
          Class<?> tlmClass = Class.forName("java.lang.ThreadLocal$ThreadLocalMap");
          Field tableField = tlmClass.getDeclaredField("table");
          tableField.setAccessible(true);
          Object[] table = (Object[]) tableField.get(threadLocalMap);

          for (Object entry : table) {
            if (entry != null) {
              Object value = FieldUtils.readField(entry, "value", true);
              if (value != null
                  && value.getClass().getClassLoader() != null
                  && value.getClass().getClassLoader()
                      == SecureFilesetCatalogOperations.class.getClassLoader()) {
                LOG.info(
                    "Cleaning up thread local {} for thread {} with custom class loader",
                    value,
                    thread.getName());
                FieldUtils.writeField(entry, "value", null, true);
              }
            }
          }
        }
      } catch (Exception e) {
        LOG.warn("Failed to clean up thread locals for thread {}", thread.getName(), e);
      }
    }
  }

  private static void closeStatsDataClearerInFileSystem()
      throws IllegalAccessException, InterruptedException {
    ScheduledExecutorService scheduler =
        (ScheduledExecutorService)
            FieldUtils.readStaticField(MutableQuantiles.class, "scheduler", true);
    scheduler.shutdownNow();
    Field statisticsCleanerField =
        FieldUtils.getField(FileSystem.Statistics.class, "STATS_DATA_CLEANER", true);
    Object statisticsCleaner = statisticsCleanerField.get(null);
    if (statisticsCleaner != null) {
      ((Thread) statisticsCleaner).interrupt();
      ((Thread) statisticsCleaner).setContextClassLoader(null);
      ((Thread) statisticsCleaner).join();
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

  private static boolean runningWithCurrentClassLoader(Thread thread) {
    return thread != null
        && thread.getContextClassLoader() == FilesetCatalogOperations.class.getClassLoader();
  }

  public static void clearShutdownHooks() {
    try {
      Class<?> shutdownHooks = Class.forName("java.lang.ApplicationShutdownHooks");
      IdentityHashMap<Thread, Thread> hooks =
          (IdentityHashMap<Thread, Thread>)
              FieldUtils.readStaticField(shutdownHooks, "hooks", true);

      hooks
          .entrySet()
          .removeIf(
              entry -> {
                Thread thread = entry.getKey();
                return thread.getContextClassLoader()
                    == FilesetCatalogOperations.class.getClassLoader();
              });
    } catch (Exception e) {
      throw new RuntimeException("Failed to clean shutdown hooks", e);
    }
  }

  @Override
  public void testConnection(
      NameIdentifier catalogIdent,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties) {
    filesetCatalogOperations.testConnection(catalogIdent, type, provider, comment, properties);
  }

  @Override
  public List<PathContext> getPathContext(NameIdentifier filesetIdentifier) {
    Fileset fileset = loadFileset(filesetIdentifier);
    String path = getTargetLocation(fileset);

    Set<String> providers =
        CredentialUtils.getCredentialProvidersByOrder(
            fileset::properties,
            () -> {
              Namespace namespace = filesetIdentifier.namespace();
              NameIdentifier schemaIdentifier =
                  NameIdentifierUtil.ofSchema(
                      namespace.level(0), namespace.level(1), namespace.level(2));
              return loadSchema(schemaIdentifier).properties();
            },
            () -> catalogProperties);
    return providers.stream()
        .map(provider -> new PathContext(path, provider))
        .collect(Collectors.toList());
  }

  @VisibleForTesting
  protected String getTargetLocation(Fileset fileset) {
    CallerContext callerContext = CallerContext.CallerContextHolder.get();
    String targetLocationName;
    String targetLocation;
    if (callerContext != null
        && callerContext
            .context()
            .containsKey(CredentialConstants.HTTP_HEADER_CURRENT_LOCATION_NAME)) {
      // case 1: target location name is passed in the header
      targetLocationName =
          callerContext.context().get(CredentialConstants.HTTP_HEADER_CURRENT_LOCATION_NAME);
      Preconditions.checkArgument(
          fileset.storageLocations().containsKey(targetLocationName),
          "The location name %s is not in the fileset %s, expected location names are %s",
          targetLocationName,
          fileset.name(),
          fileset.storageLocations().keySet());
      targetLocation = fileset.storageLocations().get(targetLocationName);

    } else if (fileset.storageLocations().size() == 1) {
      // case 2: target location name is not passed in the header, but there is only one location.
      // note: mainly used for backward compatibility since the old code does not pass the header
      // and only supports one location
      targetLocation = fileset.storageLocations().values().iterator().next();
      targetLocationName = fileset.storageLocations().keySet().iterator().next();

    } else {
      // case 3: target location name is not passed in the header, and there are multiple locations.
      // use the default location name
      targetLocationName = fileset.properties().get(PROPERTY_DEFAULT_LOCATION_NAME);
      // this should never happen, but just in case
      Preconditions.checkArgument(
          StringUtils.isNotBlank(targetLocationName),
          "The default location name of the fileset %s should not be empty.",
          fileset.name());
      targetLocation = fileset.storageLocations().get(targetLocationName);
    }

    Preconditions.checkArgument(
        StringUtils.isNotBlank(targetLocation),
        "The location with the location name %s of the fileset %s should not be empty.",
        targetLocationName,
        fileset.name());
    return targetLocation;
  }

  /**
   * Add the user to the subject so that we can get the last user in the subject. Fileset catalog
   * uses this method to pass api user from the client side, so that we can get the user in the
   * subject. Please do not mix it with UserGroupInformation.getCurrentUser().
   *
   * @param apiUser the username to set.
   */
  private void setUser(String apiUser) {
    java.security.AccessControlContext context = java.security.AccessController.getContext();
    Subject subject = Subject.getSubject(context);
    subject.getPrincipals().add(new UserPrincipal(apiUser));
  }

  /**
   * Unset the user from the subject. This is used to remove the api user from the subject after the
   * operation is done.
   *
   * @param apiUser the username to unset.
   */
  private void unsetUser(String apiUser) {
    java.security.AccessControlContext context = java.security.AccessController.getContext();
    Subject subject = Subject.getSubject(context);
    subject.getPrincipals().removeIf(principal -> principal.getName().equals(apiUser));
  }
}
