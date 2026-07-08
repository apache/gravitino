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
package org.apache.gravitino.iceberg.common.authentication.kerberos;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.catalog.hadoop.auth.KerberosAuthUtils;
import org.apache.gravitino.catalog.hadoop.auth.KerberosClient;
import org.apache.gravitino.iceberg.common.authentication.AuthenticationConfig;
import org.apache.gravitino.iceberg.common.authentication.SupportsKerberos;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;

/** Shared Kerberos initialization and execution helpers for Iceberg closable catalogs. */
public final class KerberosCatalogUtils {

  private KerberosCatalogUtils() {}

  /**
   * Initializes and logs in a {@link KerberosClient} for the given catalog.
   *
   * @param properties Catalog properties containing Kerberos configuration.
   * @param hadoopConf Hadoop configuration used for keytab download and login.
   * @param catalogName Catalog name used to derive the local keytab path.
   * @return The initialized and logged-in Kerberos client.
   */
  public static KerberosClient initKerberosClient(
      Map<String, String> properties, Configuration hadoopConf, String catalogName) {
    try {
      KerberosConfig kerberosConfig = new KerberosConfig(properties);
      KerberosClient kerberosClient =
          KerberosClient.builder(kerberosConfig.getPrincipalName(), hadoopConf)
              .loginMode(KerberosAuthUtils.LoginMode.CURRENT_USER)
              .checkIntervalSec(kerberosConfig.getCheckIntervalSec())
              .build();
      File keytabFile =
          new File(String.format(KerberosConfig.GRAVITINO_KEYTAB_FORMAT, catalogName));
      KerberosAuthUtils.saveKeytabFromUri(
          kerberosConfig.getKeytab(),
          keytabFile,
          kerberosConfig.getFetchTimeoutSec(),
          false,
          hadoopConf);
      kerberosClient.login(keytabFile.getAbsolutePath());
      return kerberosClient;
    } catch (IOException e) {
      throw new RuntimeException("Failed to login with kerberos", e);
    }
  }

  /**
   * Closes a Kerberos client and logs failures without rethrowing.
   *
   * @param kerberosClient The Kerberos client to close, or null.
   * @param logger Logger used to record close failures.
   */
  public static void closeKerberosClient(@Nullable KerberosClient kerberosClient, Logger logger) {
    if (kerberosClient != null) {
      try {
        kerberosClient.close();
      } catch (Exception e) {
        logger.warn("Failed to close KerberosClient", e);
      }
    }
  }

  /**
   * Executes catalog operations under Kerberos when configured, otherwise runs directly.
   *
   * @param properties Catalog properties.
   * @param kerberosClient Initialized Kerberos client, or null when Kerberos is not configured.
   * @param executable Operation to execute.
   * @param <R> Result type.
   * @return Operation result.
   * @throws Throwable If the operation fails.
   */
  public static <R> R doKerberosOperations(
      Map<String, String> properties,
      @Nullable KerberosClient kerberosClient,
      SupportsKerberos.Executable<R> executable)
      throws Throwable {
    AuthenticationConfig authenticationConfig = new AuthenticationConfig(properties);
    if (!authenticationConfig.isKerberosAuth()) {
      return executable.execute();
    }
    if (kerberosClient == null) {
      throw new IllegalStateException(
          "Kerberos is configured but KerberosClient is not initialized");
    }

    String finalPrincipalName = resolveProxyPrincipal(kerberosClient);
    UserGroupInformation realUser =
        createRealUser(authenticationConfig, kerberosClient, finalPrincipalName);
    return executeAs(realUser, executable);
  }

  /**
   * Resolves the proxy Kerberos principal for the current subject.
   *
   * @param kerberosClient Kerberos client used to obtain the realm when needed.
   * @return Fully qualified Kerberos principal name.
   */
  public static String resolveProxyPrincipal(KerberosClient kerberosClient) {
    String proxyKerberosPrincipalName = PrincipalUtils.getCurrentPrincipal().getName();
    if (!proxyKerberosPrincipalName.contains("@")) {
      return String.format("%s@%s", proxyKerberosPrincipalName, kerberosClient.getRealm());
    }
    return proxyKerberosPrincipalName;
  }

  /**
   * Creates the effective UGI for catalog operations, optionally as a proxy user.
   *
   * @param authenticationConfig Authentication configuration.
   * @param kerberosClient Kerberos client providing the login user.
   * @param finalPrincipalName Proxy principal name when impersonation is enabled.
   * @return UGI used to execute catalog operations.
   */
  public static UserGroupInformation createRealUser(
      AuthenticationConfig authenticationConfig,
      KerberosClient kerberosClient,
      String finalPrincipalName) {
    return authenticationConfig.isImpersonationEnabled()
        ? UserGroupInformation.createProxyUser(finalPrincipalName, kerberosClient.getLoginUser())
        : kerberosClient.getLoginUser();
  }

  /**
   * Executes an operation as the given UGI.
   *
   * @param userGroupInformation UGI to run as.
   * @param executable Operation to execute.
   * @param <R> Result type.
   * @return Operation result.
   * @throws Throwable If the operation fails.
   */
  public static <R> R executeAs(
      UserGroupInformation userGroupInformation, SupportsKerberos.Executable<R> executable)
      throws Throwable {
    return userGroupInformation.doAs(
        (PrivilegedExceptionAction<R>)
            () -> {
              try {
                return executable.execute();
              } catch (Throwable e) {
                if (RuntimeException.class.isAssignableFrom(e.getClass())) {
                  throw (RuntimeException) e;
                }
                throw new RuntimeException("Failed to invoke method", e);
              }
            });
  }
}
