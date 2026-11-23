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
package org.apache.gravitino.catalog.hadoop.fs;

import static org.apache.gravitino.catalog.hadoop.fs.Constants.AUTH_KERBEROS;
import static org.apache.gravitino.catalog.hadoop.fs.Constants.FS_DISABLE_CACHE;
import static org.apache.gravitino.catalog.hadoop.fs.FileSystemProvider.GRAVITINO_BYPASS;
import static org.apache.gravitino.catalog.hadoop.fs.HDFSFileSystemProvider.IPC_FALLBACK_TO_SIMPLE_AUTH_ALLOWED;
import static org.apache.gravitino.catalog.hadoop.fs.HDFSFileSystemProvider.additionalHDFSConfig;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.apache.gravitino.catalog.hadoop.auth.KerberosAuthUtils;
import org.apache.gravitino.catalog.hadoop.auth.KerberosClient;
import org.apache.gravitino.catalog.hadoop.fs.kerberos.AuthenticationConfig;
import org.apache.gravitino.catalog.hadoop.fs.kerberos.KerberosConfig;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.utils.ExceptionMessages;
import org.apache.gravitino.utils.KeytabNames;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A FileSystem wrapper that runs all operations under a specific UGI (UserGroupInformation). */
public class HDFSFileSystemProxy implements MethodInterceptor {

  private static final Logger LOG = LoggerFactory.getLogger(HDFSFileSystemProxy.class);

  public static final String GRAVITINO_KEYTAB_FORMAT = "keytabs/gravitino-%s";
  private static final String GRAVITINO_ID_KEY = "gravitino.identifier";
  // The Hadoop properties the login settings may come from.
  private static final List<String> LOGIN_HADOOP_KEYS =
      ImmutableList.of(
          HADOOP_SECURITY_AUTHENTICATION,
          Constants.HADOOP_SECURITY_PRINCIPAL,
          Constants.HADOOP_SECURITY_KEYTAB,
          Constants.HADOOP_KRB5_CONF);
  // The source hadoopConfiguration sets the authentication type with when it replaces a value
  // that refers to a variable.
  private static final String EXPANDED_AUTHENTICATION_SOURCE =
      "Gravitino, replacing a value with variables";

  // Keytab files not deleted yet, because their proxy is still open or failed to delete them on
  // close. A shutdown hook hands them to File.deleteOnExit, as calling it for every proxy would
  // keep every name ever used until the JVM exits.
  private static final Set<File> LIVE_KEYTAB_FILES = ConcurrentHashMap.newKeySet();
  private static final AtomicReference<Thread> KEYTAB_CLEANER = new AtomicReference<>();
  // Keytabs are a few KB. A file that is larger, or not a regular file, is not read to name it, and
  // its name is not reused.
  private static final int MAX_KEYTAB_NAME_BYTES = 1 << 20;
  // Set once the shutdown hook has handed LIVE_KEYTAB_FILES over, or when the JVM is shutting
  // down and the hook cannot be added. Files recorded after that go to File.deleteOnExit directly.
  private static volatile boolean keytabFilesHandedOver;

  @Nullable private KerberosClient kerberosClient;
  @Nullable private File keytabFile;
  @Nullable private String keytabSlotPrefix;
  private int keytabSlot;
  protected UserGroupInformation initUgi;
  private FileSystem fs;
  private Configuration configuration;
  protected boolean impersonationEnabled;
  protected String kerberosRealm;

  protected HDFSFileSystemProxy() {}

  /**
   * Create a HDFSAuthenticationFileSystem with the given path and configuration. Supports both
   * simple and Kerberos authentication.
   *
   * @param path the HDFS path
   * @param config the configuration map of Gravitino
   */
  public HDFSFileSystemProxy(Path path, Map<String, String> config) {
    initFileSystem(path, config);
  }

  protected void initFileSystem(Path path, Map<String, String> config) {
    initFileSystem(path, config, null);
  }

  /**
   * Logs in and creates the file system with the given Hadoop configuration.
   *
   * @param path the HDFS path
   * @param config the configuration map of Gravitino
   * @param hadoopConf the configuration {@link #hadoopConfiguration(Map)} built from {@code
   *     config}, or null to build it here
   */
  protected void initFileSystem(
      Path path, Map<String, String> config, @Nullable Configuration hadoopConf) {
    try {
      Configuration conf = hadoopConf != null ? hadoopConf : hadoopConfiguration(config);
      this.configuration = conf;

      AuthenticationConfig authenticationConfig = new AuthenticationConfig(config, configuration);
      this.impersonationEnabled = authenticationConfig.isImpersonationEnabled();
      String authType = authenticationConfig.getAuthType();
      if (AUTH_KERBEROS.equalsIgnoreCase(authType)) {
        this.initUgi = initKerberosUgi(config, configuration);
      } else {
        this.initUgi = UserGroupInformation.getCurrentUser();
      }

      UserGroupInformation requestUgi = getRequestUser();
      this.fs =
          requestUgi.doAs(
              (PrivilegedExceptionAction<FileSystem>)
                  () -> FileSystem.newInstance(path.toUri(), conf));

    } catch (GravitinoRuntimeException e) {
      close();
      throw e;
    } catch (Exception e) {
      close();
      throw new GravitinoRuntimeException(e, "Failed to create HDFS FileSystem with UGI: %s", path);
    } catch (Error e) {
      close();
      throw e;
    }
  }

  /**
   * Get the proxied FileSystem instance.
   *
   * @return the proxied FileSystem
   * @throws IOException if an I/O error occurs
   */
  public FileSystem getProxy() throws IOException {
    try {
      Enhancer e = new Enhancer();
      ClassLoader enhancerClassLoader = Enhancer.class.getClassLoader();
      e.setClassLoader(enhancerClassLoader);
      e.setSuperclass(fs.getClass());
      e.setCallback(this);
      FileSystem proxyFs = (FileSystem) e.create();
      fs.setConf(configuration);
      return proxyFs;
    } catch (Throwable t) {
      // The caller gets no proxy to close, for example when the file system class is final. CGLIB
      // rethrows one cached exception for a class it failed to proxy, so t is left unchanged.
      try {
        fs.close();
      } catch (IOException | RuntimeException e) {
        LOG.warn("Failed to close the file system that could not be proxied", e);
      }
      close();
      throw t;
    }
  }

  @Override
  public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy)
      throws Throwable {
    // Intercept close() method to clean up the Kerberos renewal executor
    boolean isCloseMethod = "close".equals(method.getName());
    try {
      Object result = invokeWithUgi(methodProxy, objects);
      // Close the Kerberos renewal executor after FileSystem.close()
      if (isCloseMethod) {
        close();
      }
      return result;
    } catch (Throwable e) {
      if (isCloseMethod) {
        close();
      }
      throw e;
    }
  }

  protected UserGroupInformation getRequestUser() {
    return initUgi;
  }

  /**
   * Returns the settings that decide how a proxy created with the given configurations logs in: the
   * authentication type, whether impersonation is enabled, and with Kerberos authentication the
   * principal, keytab URI and krb5.conf. Values from {@code config.resources} files and variable
   * expansion count.
   *
   * @param config the configuration map of Gravitino
   * @param configuration the configuration {@link #hadoopConfiguration(Map)} built from {@code
   *     config}
   * @return the login settings, keyed by their configuration keys
   */
  public static Map<String, String> loginSettings(
      Map<String, String> config, Configuration configuration) {
    AuthenticationConfig authenticationConfig = new AuthenticationConfig(config, configuration);
    Map<String, String> settings = new HashMap<>();
    settings.put(AuthenticationConfig.AUTH_TYPE_KEY, authenticationConfig.getAuthType());
    settings.put(
        AuthenticationConfig.IMPERSONATION_ENABLE_KEY,
        String.valueOf(authenticationConfig.isImpersonationEnabled()));
    if (AUTH_KERBEROS.equalsIgnoreCase(authenticationConfig.getAuthType())) {
      // Raw values: a blank principal fails the login, not the lookup of these settings.
      KerberosConfig kerberosConfig = new KerberosConfig(config, configuration);
      settings.put(
          KerberosConfig.PRINCIPAL_KEY, kerberosConfig.getRawString(KerberosConfig.PRINCIPAL_KEY));
      settings.put(
          KerberosConfig.KEY_TAB_URI_KEY,
          kerberosConfig.getRawString(KerberosConfig.KEY_TAB_URI_KEY));
      settings.put(Constants.HADOOP_KRB5_CONF, configuration.get(Constants.HADOOP_KRB5_CONF, ""));
    }
    return Collections.unmodifiableMap(settings);
  }

  /**
   * Returns whether the login settings of the given configuration may expand Hadoop variables. Such
   * settings may depend on any other property, for example the identifier of a fileset.
   *
   * @param configuration the configuration {@link #hadoopConfiguration(Map)} built
   * @return true if a Hadoop property that {@link #loginSettings(Map, Configuration)} may read
   *     contains a variable
   */
  public static boolean loginSettingsExpandVariables(Configuration configuration) {
    for (String key : LOGIN_HADOOP_KEYS) {
      String value = configuration.getRaw(key);
      if (value != null && value.contains("${")) {
        return true;
      }
    }
    String[] sources = configuration.getPropertySources(HADOOP_SECURITY_AUTHENTICATION);
    return sources != null && Arrays.asList(sources).contains(EXPANDED_AUTHENTICATION_SOURCE);
  }

  /**
   * Builds the Hadoop configuration a proxy logs in and connects with. It reads the {@code
   * config.resources} files.
   *
   * @param config the configuration map of Gravitino
   * @return the Hadoop configuration
   */
  public static Configuration hadoopConfiguration(Map<String, String> config) {
    Configuration conf =
        FileSystemUtils.createCompatibleConfiguration(
            GRAVITINO_BYPASS, additionalHDFSConfig(config));
    conf.setBoolean(FS_DISABLE_CACHE, true);
    conf.setBoolean(IPC_FALLBACK_TO_SIMPLE_AUTH_ALLOWED, true);
    String kerberos =
        UserGroupInformation.AuthenticationMethod.KERBEROS.name().toLowerCase(Locale.ROOT);
    if (AUTH_KERBEROS.equalsIgnoreCase(new AuthenticationConfig(config, conf).getAuthType())
        && !kerberos.equals(conf.get(HADOOP_SECURITY_AUTHENTICATION))) {
      // Set before the principal is read, as the principal may refer to this property. A value
      // that already expands to "kerberos" is kept, and the source of a replaced value with
      // variables says so, so that loginSettingsExpandVariables still sees them.
      String raw = conf.getRaw(HADOOP_SECURITY_AUTHENTICATION);
      if (raw != null && raw.contains("${")) {
        conf.set(HADOOP_SECURITY_AUTHENTICATION, kerberos, EXPANDED_AUTHENTICATION_SOURCE);
      } else {
        conf.set(HADOOP_SECURITY_AUTHENTICATION, kerberos);
      }
    }
    return conf;
  }

  /** Invoke the method on the underlying FileSystem using ugi.doAs. */
  protected Object invokeWithUgi(MethodProxy methodProxy, Object[] objects) throws Throwable {
    UserGroupInformation currentUgi = getRequestUser();
    return currentUgi.doAs(
        (PrivilegedExceptionAction<Object>)
            () -> {
              try {
                return methodProxy.invoke(fs, objects);
              } catch (IOException e) {
                throw e;
              } catch (Throwable e) {
                if (RuntimeException.class.isAssignableFrom(e.getClass())) {
                  throw (RuntimeException) e;
                }
                throw ExceptionMessages.wrap("Failed to invoke method", e);
              }
            });
  }

  @VisibleForTesting
  @Nullable
  static Thread keytabCleaner() {
    return KEYTAB_CLEANER.get();
  }

  @VisibleForTesting
  static void resetKeytabFilesHandedOver() {
    keytabFilesHandedOver = false;
  }

  @VisibleForTesting
  static boolean isLiveKeytabFile(File file) {
    return LIVE_KEYTAB_FILES.contains(file);
  }

  private synchronized void close() {
    if (kerberosClient != null) {
      try {
        kerberosClient.close();
      } catch (Exception e) {
        LOG.warn("Failed to stop the Kerberos ticket refresh for {}", kerberosRealm, e);
      }
      kerberosClient = null;
    }
    if (keytabFile != null) {
      try {
        Files.deleteIfExists(keytabFile.toPath());
        LIVE_KEYTAB_FILES.remove(keytabFile);
        releaseKeytabSlot();
      } catch (IOException | RuntimeException e) {
        LOG.warn("Failed to delete the keytab file {}", keytabFile, e);
        // Deleted at exit instead. The shutdown hook would hand it over too, but a catalog class
        // loader removes that hook when it is closed. Only failed deletes get here.
        deleteOnExit(keytabFile);
        // Its name stays taken while the file may still be there.
        keytabSlotPrefix = null;
      }
      keytabFile = null;
    }
  }

  private synchronized void recordKeytabFile(File file) {
    File previous = keytabFile;
    keytabFile = file;
    if (KEYTAB_CLEANER.get() == null) {
      Thread cleaner =
          new Thread(HDFSFileSystemProxy::deleteLiveKeytabFilesOnExit, "gravitino-keytab-cleaner");
      // A catalog class loader removes, when it is closed, the hooks whose context class loader is
      // its own.
      cleaner.setContextClassLoader(HDFSFileSystemProxy.class.getClassLoader());
      if (KEYTAB_CLEANER.compareAndSet(null, cleaner)) {
        try {
          Runtime.getRuntime().addShutdownHook(cleaner);
        } catch (IllegalStateException e) {
          // The JVM is shutting down, so no hook will hand the files over.
          deleteLiveKeytabFilesOnExit();
        }
      }
    }
    // The hook sets the flag and goes through the set under the same LIVE_KEYTAB_FILES lock, so
    // either the hook sees the file or this method sees the flag.
    synchronized (LIVE_KEYTAB_FILES) {
      if (previous != null) {
        LIVE_KEYTAB_FILES.remove(previous);
      }
      LIVE_KEYTAB_FILES.add(file);
      if (keytabFilesHandedOver) {
        deleteOnExit(file);
      }
    }
  }

  // Takes the lowest free name for the identifier, the keytab URI and its content, and moves the
  // fetched file there. See KeytabNames for when a name is reused.
  private synchronized File moveToKeytabName(String identifier, String keytabUri, File fetched)
      throws IOException {
    String prefix = keytabNamePrefix(identifier, keytabUri, fetched);
    int slot = KeytabNames.reserve(prefix);
    keytabSlotPrefix = prefix;
    keytabSlot = slot;
    File file =
        new File(
            fetched.getParentFile(),
            new File(String.format(GRAVITINO_KEYTAB_FORMAT, prefix + "-" + slot)).getName());
    // Moves a symbolic link itself, so the file still follows a local keytab.
    Files.move(fetched.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING);
    recordKeytabFile(file);
    return file;
  }

  private static String keytabNamePrefix(String identifier, String keytabUri, File fetched)
      throws IOException {
    if (Files.isRegularFile(fetched.toPath())) {
      try (InputStream in = Files.newInputStream(fetched.toPath())) {
        byte[] content = ByteStreams.toByteArray(ByteStreams.limit(in, MAX_KEYTAB_NAME_BYTES + 1));
        if (content.length <= MAX_KEYTAB_NAME_BYTES) {
          return KeytabNames.prefix(identifier, keytabUri, content);
        }
      }
    }
    return identifier + "-" + KeytabNames.token() + "-" + UUID.randomUUID();
  }

  private synchronized void releaseKeytabSlot() {
    if (keytabSlotPrefix == null) {
      return;
    }
    KeytabNames.release(keytabSlotPrefix, keytabSlot);
    keytabSlotPrefix = null;
  }

  private static void deleteOnExit(File file) {
    try {
      file.deleteOnExit();
    } catch (RuntimeException e) {
      LOG.warn("Failed to delete the keytab file {} on exit", file, e);
    }
  }

  // Shutdown hooks run concurrently, and the server's own hook still closes file systems that may
  // log in again. File.deleteOnExit deletes the files after all shutdown hooks finish.
  private static void deleteLiveKeytabFilesOnExit() {
    synchronized (LIVE_KEYTAB_FILES) {
      keytabFilesHandedOver = true;
      LIVE_KEYTAB_FILES.forEach(HDFSFileSystemProxy::deleteOnExit);
    }
  }

  private UserGroupInformation initKerberosUgi(
      Map<String, String> properties, Configuration configuration) {
    try {
      KerberosConfig kerberosConfig = new KerberosConfig(properties, configuration);
      KerberosClient client =
          KerberosClient.builder(kerberosConfig.getPrincipalName(), configuration)
              .loginMode(KerberosAuthUtils.LoginMode.RETURN_UGI)
              .refreshCredentials(true)
              .checkIntervalSec(kerberosConfig.getCheckIntervalSec())
              .krb5Conf(Constants.HADOOP_KRB5_CONF, Constants.SECURITY_KRB5_ENV)
              .build();
      // One file for each open proxy: another proxy for the same entity would rewrite a shared
      // file while this one still renews its ticket from it. close() deletes it.
      String identifier = properties.getOrDefault(GRAVITINO_ID_KEY, "");
      // The JDK never reads the file fetched into, so its name is not reused. It is recorded
      // before the fetch, so that close() also deletes what a failed fetch leaves behind.
      File destination =
          new File(
              String.format(
                  GRAVITINO_KEYTAB_FORMAT,
                  identifier + "-" + KeytabNames.token() + "-fetch-" + UUID.randomUUID()));
      recordKeytabFile(destination);
      String keytabUri = kerberosConfig.getKeytab();
      File fetched =
          KerberosAuthUtils.fetchKeytabFromUri(
              keytabUri, destination, kerberosConfig.getFetchTimeoutSec(), false, configuration);
      recordKeytabFile(fetched);
      File keytabFile = moveToKeytabName(identifier, keytabUri, fetched);
      UserGroupInformation ugi = client.login(keytabFile.getAbsolutePath());
      // login() starts the ticket refresh, so record the client before anything else can fail.
      this.kerberosClient = client;
      this.kerberosRealm = client.getRealm();
      return ugi;
    } catch (IOException e) {
      throw ExceptionMessages.wrap("Failed to login with Kerberos", e);
    }
  }
}
