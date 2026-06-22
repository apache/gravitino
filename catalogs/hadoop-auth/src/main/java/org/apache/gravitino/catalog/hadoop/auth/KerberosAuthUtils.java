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

package org.apache.gravitino.catalog.hadoop.auth;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.gravitino.utils.FileFetcher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;

/** Shared Kerberos helpers for Hadoop-aware Gravitino modules. */
public final class KerberosAuthUtils {

  private KerberosAuthUtils() {}

  /** Login behavior used by Hadoop-backed Kerberos clients. */
  public enum LoginMode {
    /** Log in as the process login user and return {@link UserGroupInformation#getLoginUser()}. */
    LOGIN_USER,

    /**
     * Log in as the process login user and return {@link UserGroupInformation#getCurrentUser()}.
     */
    CURRENT_USER,

    /** Return an isolated UGI from {@link UserGroupInformation#loginUserFromKeytabAndReturnUGI}. */
    RETURN_UGI
  }

  /**
   * Validates a Kerberos principal and returns its realm.
   *
   * @param principal Kerberos principal in {@code name@REALM} format
   * @return the realm portion of the principal
   */
  public static String checkPrincipalAndGetRealm(String principal) {
    if (principal == null || principal.trim().isEmpty()) {
      throw new IllegalArgumentException("The principal can't be blank");
    }

    String[] principalComponents = principal.split("@", -1);
    if (principalComponents.length != 2
        || principalComponents[0].isEmpty()
        || principalComponents[1].isEmpty()) {
      throw new IllegalArgumentException("The principal has the wrong format");
    }

    return principalComponents[1];
  }

  /**
   * Fetches a Kerberos keytab URI to a local file.
   *
   * @param keytabUri source keytab URI
   * @param keytabFile local destination file
   * @param timeoutSec fetch timeout in seconds
   * @param allowHdfsKeytabUri whether {@code hdfs://} keytab URIs are allowed
   * @param hadoopConf Hadoop configuration, required only when fetching an HDFS URI
   * @return the fetched local keytab file
   * @throws IOException if the file cannot be fetched
   */
  public static File fetchKeytabFromUri(
      String keytabUri,
      File keytabFile,
      int timeoutSec,
      boolean allowHdfsKeytabUri,
      @Nullable Configuration hadoopConf)
      throws IOException {
    if (keytabUri == null || keytabUri.trim().isEmpty()) {
      throw new IllegalArgumentException("Keytab uri can't be blank");
    }

    if (!allowHdfsKeytabUri && isHdfsUri(keytabUri)) {
      throw new IllegalArgumentException("HDFS URIs are not supported for keytab files");
    }

    File parentFile = keytabFile.getParentFile();
    if (parentFile != null && !parentFile.exists() && !parentFile.mkdirs()) {
      throw new IOException(
          String.format("Failed to create keytab directory %s", parentFile.getAbsolutePath()));
    }

    FileFetcher.get().fetchFileFromUri(keytabUri, keytabFile, timeoutSec * 1000, hadoopConf);
    return keytabFile;
  }

  /**
   * Configures the JVM Kerberos configuration path from a Hadoop configuration key.
   *
   * @param hadoopConf Hadoop configuration
   * @param hadoopKrb5ConfKey Hadoop configuration key that stores the krb5.conf path
   * @param systemKrb5ConfKey JVM system property key for the krb5.conf path
   */
  public static void configureKrb5Conf(
      Configuration hadoopConf, String hadoopKrb5ConfKey, String systemKrb5ConfKey) {
    String krb5Config = hadoopConf.get(hadoopKrb5ConfKey);
    if (krb5Config != null) {
      System.setProperty(systemKrb5ConfKey, krb5Config);
    }
  }

  /**
   * Logs in with a Kerberos keytab.
   *
   * @param principal Kerberos principal
   * @param keytabFilePath local keytab file path
   * @param hadoopConf Hadoop configuration
   * @param loginMode login behavior to use
   * @return the login UGI
   * @throws IOException if login fails
   */
  public static UserGroupInformation login(
      String principal, String keytabFilePath, Configuration hadoopConf, LoginMode loginMode)
      throws IOException {
    checkPrincipalAndGetRealm(principal);
    UserGroupInformation.setConfiguration(hadoopConf);

    if (loginMode == LoginMode.RETURN_UGI) {
      return UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytabFilePath);
    }

    UserGroupInformation.loginUserFromKeytab(principal, keytabFilePath);
    if (loginMode == LoginMode.CURRENT_USER) {
      return UserGroupInformation.getCurrentUser();
    }

    return UserGroupInformation.getLoginUser();
  }

  /**
   * Starts a daemon executor that periodically refreshes a keytab-backed TGT.
   *
   * @param loginUgi login user to refresh
   * @param checkIntervalSec refresh interval in seconds
   * @param threadNameFormat thread name format, for example {@code check-tgt-%d}
   * @param log logger used for refresh failures
   * @return the scheduled refresh executor
   */
  public static ScheduledThreadPoolExecutor startTicketRefresh(
      UserGroupInformation loginUgi, int checkIntervalSec, String threadNameFormat, Logger log) {
    if (checkIntervalSec <= 0) {
      throw new IllegalArgumentException("The check interval must be positive");
    }

    ScheduledThreadPoolExecutor executor =
        new ScheduledThreadPoolExecutor(1, daemonThreadFactory(threadNameFormat));
    executor.scheduleAtFixedRate(
        () -> {
          try {
            loginUgi.checkTGTAndReloginFromKeytab();
          } catch (Exception e) {
            log.error("Fail to refresh ugi token: ", e);
          }
        },
        checkIntervalSec,
        checkIntervalSec,
        TimeUnit.SECONDS);
    return executor;
  }

  private static boolean isHdfsUri(String keytabUri) {
    try {
      URI uri = new URI(keytabUri.trim());
      return "hdfs".equalsIgnoreCase(uri.getScheme());
    } catch (URISyntaxException e) {
      return keytabUri.trim().startsWith("hdfs");
    }
  }

  private static ThreadFactory daemonThreadFactory(String threadNameFormat) {
    AtomicInteger threadId = new AtomicInteger(0);
    return runnable -> {
      Thread thread = new Thread(runnable);
      thread.setDaemon(true);
      thread.setName(String.format(threadNameFormat, threadId.getAndIncrement()));
      return thread;
    };
  }
}
