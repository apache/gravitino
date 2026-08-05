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

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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

  private static final String TICKET_REFRESH_THREAD_NAME_PREFIX = "kerberos-ticket-refresh-";
  private static final AtomicInteger TICKET_REFRESH_THREAD_ID = new AtomicInteger(0);

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
    Preconditions.checkArgument(principal != null, "The principal can't be blank");

    String normalizedPrincipal = principal.trim();
    Preconditions.checkArgument(!normalizedPrincipal.isEmpty(), "The principal can't be blank");

    String[] principalComponents = normalizedPrincipal.split("@", -1);
    Preconditions.checkArgument(
        principalComponents.length == 2
            && !principalComponents[0].isEmpty()
            && !principalComponents[1].isEmpty(),
        "The principal has the wrong format");

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
    Preconditions.checkArgument(
        keytabUri != null && !keytabUri.trim().isEmpty(), "Keytab uri can't be blank");
    Preconditions.checkArgument(
        allowHdfsKeytabUri || !isHdfsUri(keytabUri),
        "HDFS URIs are not supported for keytab files");

    File parentFile = keytabFile.getParentFile();
    if (parentFile != null) {
      Files.createDirectories(parentFile.toPath());
    }

    FileFetcher.get().fetchFileFromUri(keytabUri, keytabFile, timeoutSec * 1000, hadoopConf);
    return keytabFile;
  }

  /**
   * Fetches a keytab URI into {@code keytabFile}, replacing any stale file first.
   *
   * <p>Deletes a pre-existing destination, marks the new file for deletion on JVM exit, and then
   * fetches the keytab. The parent directory is created if needed.
   *
   * @param keytabUri source keytab URI
   * @param keytabFile local destination file
   * @param timeoutSec fetch timeout in seconds
   * @param allowHdfsKeytabUri whether {@code hdfs://} keytab URIs are allowed
   * @param hadoopConf Hadoop configuration, required only when fetching an HDFS URI
   * @return the fetched local keytab file
   * @throws IOException if the file cannot be fetched
   */
  public static File saveKeytabFromUri(
      String keytabUri,
      File keytabFile,
      int timeoutSec,
      boolean allowHdfsKeytabUri,
      @Nullable Configuration hadoopConf)
      throws IOException {
    keytabFile.deleteOnExit();
    if (keytabFile.exists() && !keytabFile.delete()) {
      throw new IllegalStateException(
          String.format("Failed to delete keytab file %s", keytabFile.getAbsolutePath()));
    }
    return fetchKeytabFromUri(keytabUri, keytabFile, timeoutSec, allowHdfsKeytabUri, hadoopConf);
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
    Preconditions.checkArgument(hadoopConf != null, "Hadoop configuration can't be null");
    Preconditions.checkArgument(
        hadoopKrb5ConfKey != null && !hadoopKrb5ConfKey.trim().isEmpty(),
        "Hadoop krb5.conf key can't be blank");
    Preconditions.checkArgument(
        systemKrb5ConfKey != null && !systemKrb5ConfKey.trim().isEmpty(),
        "System krb5.conf key can't be blank");

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
    Preconditions.checkArgument(loginMode != null, "loginMode can't be null");
    UserGroupInformation.setConfiguration(hadoopConf);

    switch (loginMode) {
      case RETURN_UGI:
        return UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytabFilePath);
      case CURRENT_USER:
        UserGroupInformation.loginUserFromKeytab(principal, keytabFilePath);
        return UserGroupInformation.getCurrentUser();
      case LOGIN_USER:
        UserGroupInformation.loginUserFromKeytab(principal, keytabFilePath);
        return UserGroupInformation.getLoginUser();
      default:
        throw new IllegalArgumentException(String.format("Unsupported loginMode: %s", loginMode));
    }
  }

  /**
   * Starts a dedicated single-thread daemon scheduler that periodically refreshes a keytab-backed
   * TGT.
   *
   * <p>Each caller gets its own executor so a slow or unreachable KDC only blocks that caller's
   * refresh. The caller owns the returned executor and must shut it down (via {@link
   * ScheduledExecutorService#shutdown()}) when it is no longer needed.
   *
   * @param loginUgi login user to refresh
   * @param checkIntervalSec refresh interval in seconds
   * @param log logger used for refresh failures
   * @return the scheduler running the refresh task, owned by the caller
   */
  public static ScheduledExecutorService startTicketRefresh(
      UserGroupInformation loginUgi, int checkIntervalSec, Logger log) {
    Preconditions.checkArgument(checkIntervalSec > 0, "The check interval must be positive");

    ScheduledExecutorService executor =
        Executors.newSingleThreadScheduledExecutor(
            daemonThreadFactory(TICKET_REFRESH_THREAD_NAME_PREFIX));
    executor.scheduleAtFixedRate(
        () -> refreshTicket(loginUgi, log), checkIntervalSec, checkIntervalSec, TimeUnit.SECONDS);
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

  private static void refreshTicket(UserGroupInformation loginUgi, Logger log) {
    try {
      loginUgi.checkTGTAndReloginFromKeytab();
    } catch (Exception e) {
      log.error("Failed to refresh UGI token: ", e);
    }
  }

  private static ThreadFactory daemonThreadFactory(String threadNamePrefix) {
    return runnable -> {
      Thread thread = new Thread(runnable);
      thread.setDaemon(true);
      thread.setName(threadNamePrefix + TICKET_REFRESH_THREAD_ID.getAndIncrement());
      return thread;
    };
  }
}
