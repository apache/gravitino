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
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.HashMap;
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

  // Keytab files of the proxies not closed yet, deleted when the JVM exits. File.deleteOnExit
  // would keep the path of every proxy ever created.
  private static final Set<File> LIVE_KEYTAB_FILES = ConcurrentHashMap.newKeySet();
  private static final AtomicReference<Thread> KEYTAB_CLEANER = new AtomicReference<>();

  @Nullable private KerberosClient kerberosClient;
  @Nullable private File keytabFile;
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
      } catch (IOException | RuntimeException e) {
        LOG.warn("Failed to delete the keytab file {}", keytabFile, e);
      }
      LIVE_KEYTAB_FILES.remove(keytabFile);
      keytabFile = null;
    }
  }

  private synchronized void recordKeytabFile(File file) {
    if (keytabFile != null) {
      LIVE_KEYTAB_FILES.remove(keytabFile);
    }
    keytabFile = file;
    if (KEYTAB_CLEANER.get() == null) {
      Thread cleaner =
          new Thread(HDFSFileSystemProxy::deleteLiveKeytabFiles, "gravitino-keytab-cleaner");
      // A catalog class loader removes, when it is closed, the hooks whose context class loader is
      // its own.
      cleaner.setContextClassLoader(HDFSFileSystemProxy.class.getClassLoader());
      if (KEYTAB_CLEANER.compareAndSet(null, cleaner)) {
        try {
          Runtime.getRuntime().addShutdownHook(cleaner);
        } catch (IllegalStateException e) {
          LOG.warn("The JVM is shutting down, {} is not deleted on exit", file);
        }
      }
    }
    LIVE_KEYTAB_FILES.add(file);
  }

  @VisibleForTesting
  @Nullable
  static Thread keytabCleaner() {
    return KEYTAB_CLEANER.get();
  }

  private static void deleteLiveKeytabFiles() {
    LIVE_KEYTAB_FILES.forEach(File::delete);
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
    if (AUTH_KERBEROS.equalsIgnoreCase(new AuthenticationConfig(config, conf).getAuthType())) {
      // Set before the principal is read, as its value may refer to this one.
      conf.set(
          HADOOP_SECURITY_AUTHENTICATION,
          UserGroupInformation.AuthenticationMethod.KERBEROS.name().toLowerCase(Locale.ROOT));
    }
    return conf;
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
      // One file for each proxy: another proxy for the same entity would rewrite a shared file
      // while this one still renews its ticket from it. close() deletes it; File.deleteOnExit
      // would keep the path of every proxy ever created until the JVM exits.
      String keytabPath =
          String.format(
              GRAVITINO_KEYTAB_FORMAT,
              properties.getOrDefault(GRAVITINO_ID_KEY, "") + "-" + UUID.randomUUID());
      File destination = new File(keytabPath);
      // Recorded before the fetch, so that close() also deletes what a failed fetch leaves behind.
      recordKeytabFile(destination);
      File keytabFile =
          KerberosAuthUtils.fetchKeytabFromUri(
              kerberosConfig.getKeytab(),
              destination,
              kerberosConfig.getFetchTimeoutSec(),
              false,
              configuration);
      recordKeytabFile(keytabFile);
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
