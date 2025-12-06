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
import com.google.common.base.Splitter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.hive.client.HiveClient;
import org.apache.gravitino.hive.client.HiveClientClassLoader;
import org.apache.gravitino.hive.client.HiveClientFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.thrift.DelegationTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KerberosClient implements java.io.Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(KerberosClient.class);

  private ScheduledThreadPoolExecutor checkTgtExecutor;
  private final Properties conf;
  private final Configuration hadoopConf;
  private final boolean refreshCredentials;
  private UserGroupInformation realLoginUgi;
  private String keytabFilePath;
  private HiveClient hiveClient = null;
  private HiveClientClassLoader.HiveVersion version;

  public KerberosClient(
      HiveClientClassLoader.HiveVersion version,
      Properties properties,
      Configuration hadoopConf,
      boolean refreshCredentials,
      String keytabFilePath) {
    this.conf = properties;
    this.hadoopConf = hadoopConf;
    this.refreshCredentials = refreshCredentials;
    this.keytabFilePath = keytabFilePath;
    this.version = version;
  }

  public UserGroupInformation login(String userName) throws Exception {
    if (realLoginUgi == null) {
      return loginRealUser(userName);
    } else {
      if (userName.equals(realLoginUgi.getUserName())) {
        return realLoginUgi;
      } else {
        return loginProxyUser(userName);
      }
    }
  }

  private UserGroupInformation loginProxyUser(String currentUser) {
    try {
      String tokenSignature = conf.getProperty(HIVE_METASTORE_TOKEN_SIGNATURE, "");
      String principal = conf.getProperty(PRINCIPAL_KEY, "");
      List<String> principalComponents = Splitter.on('@').splitToList(principal);
      Preconditions.checkArgument(
          principalComponents.size() == 2, "The principal has the wrong format");
      String kerberosRealm = principalComponents.get(1);

      UserGroupInformation proxyUser;
      if (UserGroupInformation.isSecurityEnabled()) {
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

      } else {
        proxyUser = UserGroupInformation.createProxyUser(currentUser, realLoginUgi);
      }

      return proxyUser;
    } catch (Exception e) {
      throw new RuntimeException("Failed to create proxy user for Kerberos Hive client", e);
    }
  }

  public UserGroupInformation loginRealUser(String userName) throws Exception {
    KerberosConfig kerberosConfig = new KerberosConfig(conf, hadoopConf);

    // Check the principal and keytab file
    String catalogPrincipal = kerberosConfig.getPrincipalName();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(catalogPrincipal), "The principal can't be blank");
    @SuppressWarnings("null")
    java.util.List<String> principalComponents = Splitter.on('@').splitToList(catalogPrincipal);
    Preconditions.checkArgument(
        principalComponents.size() == 2, "The principal has the wrong format");

    // Login
    UserGroupInformation.setConfiguration(hadoopConf);
    UserGroupInformation.loginUserFromKeytab(catalogPrincipal, keytabFilePath);
    realLoginUgi = UserGroupInformation.getLoginUser();
    hiveClient =
        HiveClientFactory.createHiveClientImpl(
            version, conf, Thread.currentThread().getContextClassLoader());

    // Refresh the cache if it's out of date.
    if (refreshCredentials) {
      this.checkTgtExecutor = new ScheduledThreadPoolExecutor(1, getThreadFactory("check-tgt"));
      int checkInterval = kerberosConfig.getCheckIntervalSec();
      checkTgtExecutor.scheduleAtFixedRate(
          () -> {
            try {
              realLoginUgi.checkTGTAndReloginFromKeytab();
            } catch (Exception e) {
              LOG.error("Fail to refresh ugi token: ", e);
            }
          },
          checkInterval,
          checkInterval,
          TimeUnit.SECONDS);
    }

    if (!realLoginUgi.getUserName().equals(userName)) {
      return loginProxyUser(userName);
    }

    return realLoginUgi;
  }

  public File saveKeyTabFileFromUri() throws IOException {
    KerberosConfig kerberosConfig = new KerberosConfig(conf, hadoopConf);

    String keyTabUri = kerberosConfig.getKeytab();
    Preconditions.checkArgument(StringUtils.isNotBlank(keyTabUri), "Keytab uri can't be blank");
    Preconditions.checkArgument(
        !keyTabUri.trim().startsWith("hdfs"), "Keytab uri doesn't support to use HDFS");

    File keytabsDir = new File("keytabs");
    if (!keytabsDir.exists()) {
      keytabsDir.mkdir();
    }
    File keytabFile = new File(keytabFilePath);
    keytabFile.deleteOnExit();
    if (keytabFile.exists() && !keytabFile.delete()) {
      throw new IllegalStateException(
          String.format("Fail to delete keytab file %s", keytabFile.getAbsolutePath()));
    }
    int fetchKeytabFileTimeout = kerberosConfig.getFetchTimeoutSec();
    FetchFileUtils.fetchFileFromUri(keyTabUri, keytabFile, fetchKeytabFileTimeout, hadoopConf);
    return keytabFile;
  }

  private static ThreadFactory getThreadFactory(String factoryName) {
    return new ThreadFactoryBuilder().setDaemon(true).setNameFormat(factoryName + "-%d").build();
  }

  @Override
  public void close() {
    if (checkTgtExecutor != null) {
      checkTgtExecutor.shutdown();
    }
  }
}
