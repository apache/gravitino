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
import static org.apache.gravitino.catalog.hadoop.fs.HDFSFileSystemProvider.IPC_FALLBACK_TO_SIMPLE_AUTH_ALLOWED;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;
import java.util.Locale;
import java.util.Map;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.apache.gravitino.catalog.hadoop.fs.kerberos.AuthenticationConfig;
import org.apache.gravitino.catalog.hadoop.fs.kerberos.KerberosClient;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
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
   * @param conf the Hadoop configuration
   * @param config the configuration map of Gravitino
   */
  public HDFSFileSystemProxy(Path path, Configuration conf, Map<String, String> config) {
    initFileSystem(path, conf, config);
  }

  protected void initFileSystem(Path path, Configuration conf, Map<String, String> config) {
    try {
      conf.setBoolean(FS_DISABLE_CACHE, true);
      conf.setBoolean(IPC_FALLBACK_TO_SIMPLE_AUTH_ALLOWED, true);
      this.configuration = conf;

      AuthenticationConfig authenticationConfig = new AuthenticationConfig(config, configuration);
      this.impersonationEnabled = authenticationConfig.isImpersonationEnabled();
      String authType = authenticationConfig.getAuthType();
      if (AUTH_KERBEROS.equalsIgnoreCase(authType)) {
        this.configuration.set(
            HADOOP_SECURITY_AUTHENTICATION,
            UserGroupInformation.AuthenticationMethod.KERBEROS.name().toLowerCase(Locale.ROOT));
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
      throw e;
    } catch (Exception e) {
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
    Enhancer e = new Enhancer();
    ClassLoader enhancerClassLoader = Enhancer.class.getClassLoader();
    e.setClassLoader(enhancerClassLoader);
    e.setSuperclass(fs.getClass());
    e.setCallback(this);
    FileSystem proxyFs = (FileSystem) e.create();
    fs.setConf(configuration);
    return proxyFs;
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
                throw new RuntimeException("Failed to invoke method", e);
              }
            });
  }

  private void close() {}

  private UserGroupInformation initKerberosUgi(
      Map<String, String> properties, Configuration configuration) {
    try {
      KerberosClient client = new KerberosClient(properties, configuration, true);
      String keytabPath =
          String.format(GRAVITINO_KEYTAB_FORMAT, properties.getOrDefault(GRAVITINO_ID_KEY, ""));
      File keytabFile = client.saveKeyTabFileFromUri(keytabPath);
      UserGroupInformation ugi = client.login(keytabFile.getAbsolutePath());
      this.kerberosRealm = client.getKerberosRealm();
      return ugi;
    } catch (IOException e) {
      throw new RuntimeException("Failed to login with Kerberos", e);
    }
  }
}
