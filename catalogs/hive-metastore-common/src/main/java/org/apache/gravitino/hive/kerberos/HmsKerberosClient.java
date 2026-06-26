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

package org.apache.gravitino.hive.kerberos;

import static org.apache.gravitino.catalog.hive.HiveConstants.HIVE_METASTORE_TOKEN_SIGNATURE;
import static org.apache.gravitino.hive.kerberos.KerberosConfig.PRINCIPAL_KEY;

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.ScheduledFuture;
import org.apache.gravitino.catalog.hadoop.auth.KerberosAuthUtils;
import org.apache.gravitino.hive.client.HiveClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.thrift.DelegationTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kerberos client for Hive Metastore.
 *
 * <p>This class keeps HMS-specific behavior on top of the shared Hadoop Kerberos helpers, including
 * proxy user creation, delegation-token retrieval, Hive client wiring, and local keytab lifecycle.
 */
public class HmsKerberosClient implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(HmsKerberosClient.class);

  private ScheduledFuture<?> checkTgtRefreshTask;
  private final Properties conf;
  private final Configuration hadoopConf;
  private final boolean refreshCredentials;
  private UserGroupInformation realLoginUgi;
  private final String keytabFilePath;
  private HiveClient hiveClient = null;

  public HmsKerberosClient(
      Properties properties,
      Configuration hadoopConf,
      boolean refreshCredentials,
      String keytabFilePath)
      throws IOException {
    this.conf = properties;
    this.hadoopConf = hadoopConf;
    this.refreshCredentials = refreshCredentials;
    File keyTabFile = saveKeyTabFileFromUri(keytabFilePath);
    this.keytabFilePath = keyTabFile.getAbsolutePath();
  }

  /**
   * Login proxy user for the given user name.
   *
   * @param currentUser The user name to login
   * @return The UserGroupInformation for the proxy user
   */
  public UserGroupInformation loginProxyUser(String currentUser) {
    try {
      // hiveClient is null in case the initial the kerbers client with the login user
      if (currentUser.equals(realLoginUgi.getUserName()) || hiveClient == null) {
        return realLoginUgi;
      }

      String tokenSignature = conf.getProperty(HIVE_METASTORE_TOKEN_SIGNATURE, "");
      String principal = conf.getProperty(PRINCIPAL_KEY, "");
      String kerberosRealm = KerberosAuthUtils.checkPrincipalAndGetRealm(principal);

      UserGroupInformation proxyUser;
      final String finalPrincipalName;
      if (!currentUser.contains("@")) {
        finalPrincipalName = String.format("%s@%s", currentUser, kerberosRealm);
      } else {
        finalPrincipalName = currentUser;
      }

      proxyUser = UserGroupInformation.createProxyUser(finalPrincipalName, realLoginUgi);

      // Acquire HMS delegation token for the proxy user and attach it to UGI
      String tokenStr =
          hiveClient.getDelegationToken(finalPrincipalName, realLoginUgi.getUserName());

      Token<DelegationTokenIdentifier> delegationToken = new Token<>();
      delegationToken.decodeFromUrlString(tokenStr);
      delegationToken.setService(new Text(tokenSignature));
      proxyUser.addToken(delegationToken);

      return proxyUser;
    } catch (Exception e) {
      throw new RuntimeException("Failed to create proxy user for Kerberos Hive client", e);
    }
  }

  /**
   * Login the Kerberos user from the principal and keytab file.
   *
   * @return
   * @throws Exception
   */
  public UserGroupInformation login() throws Exception {
    KerberosConfig kerberosConfig = new KerberosConfig(conf, hadoopConf);

    String catalogPrincipal = kerberosConfig.getPrincipalName();

    // Login
    UserGroupInformation loginUgi =
        KerberosAuthUtils.login(
            catalogPrincipal, keytabFilePath, hadoopConf, KerberosAuthUtils.LoginMode.LOGIN_USER);
    realLoginUgi = loginUgi;

    // Refresh the cache if it's out of date.
    if (refreshCredentials) {
      cancelTicketRefreshTask();
      int checkInterval = kerberosConfig.getCheckIntervalSec();
      checkTgtRefreshTask = KerberosAuthUtils.startTicketRefresh(loginUgi, checkInterval, LOG);
    }

    return loginUgi;
  }

  public File saveKeyTabFileFromUri(String path) throws IOException {
    KerberosConfig kerberosConfig = new KerberosConfig(conf, hadoopConf);

    String keyTabUri = kerberosConfig.getKeytab();

    File keytabsDir = new File("keytabs");
    if (!keytabsDir.exists()) {
      keytabsDir.mkdir();
    }
    File keytabFile = new File(path);
    int fetchKeytabFileTimeout = kerberosConfig.getFetchTimeoutSec();
    KerberosAuthUtils.fetchKeytabFromUri(
        keyTabUri, keytabFile, fetchKeytabFileTimeout, false /* allowHdfsKeytabUri */, hadoopConf);
    return keytabFile;
  }

  @Override
  public void close() {
    try {
      cancelTicketRefreshTask();

      Files.deleteIfExists(Paths.get(keytabFilePath));
    } catch (IOException e) {
      LOG.warn("Failed to delete keytab file: {}", keytabFilePath, e);
    }
  }

  private void cancelTicketRefreshTask() {
    if (checkTgtRefreshTask != null) {
      checkTgtRefreshTask.cancel(true);
      checkTgtRefreshTask = null;
    }
  }

  public void setHiveClient(HiveClient client) {
    this.hiveClient = client;
  }

  /**
   * Returns the real (non-proxy) {@link UserGroupInformation} that was obtained after Kerberos
   * login via keytab. Used by callers that need to bind the JAAS Subject to the current thread
   * (e.g., via {@code ugi.doAs(...)}) before performing a Kerberos-protected RPC call.
   *
   * @return the real login UGI.
   * @throws IllegalStateException if {@link #login()} has not been called yet.
   */
  public UserGroupInformation getRealLoginUgi() {
    Preconditions.checkState(realLoginUgi != null, "HmsKerberosClient.login() has not been called");
    return realLoginUgi;
  }
}
