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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import lombok.Getter;
import org.apache.gravitino.catalog.hadoop.auth.KerberosAuthUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KerberosClient implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(KerberosClient.class);

  private ScheduledThreadPoolExecutor checkTgtExecutor;
  private final Map<String, String> conf;
  private final Configuration hadoopConf;
  @Getter private UserGroupInformation loginUser;
  private String realm;

  public KerberosClient(Map<String, String> conf, Configuration hadoopConf) {
    this.conf = conf;
    this.hadoopConf = hadoopConf;
  }

  public String getRealm() {
    return realm;
  }

  public void login(String keytabFilePath) throws IOException {
    KerberosConfig kerberosConfig = new KerberosConfig(conf);

    // Check the principal and keytab file
    String catalogPrincipal = kerberosConfig.getPrincipalName();
    this.realm = KerberosAuthUtils.checkPrincipalAndGetRealm(catalogPrincipal);

    // Login
    loginUser =
        KerberosAuthUtils.login(
            catalogPrincipal, keytabFilePath, hadoopConf, KerberosAuthUtils.LoginMode.CURRENT_USER);

    // Refresh the cache if it's out of date.
    int checkInterval = kerberosConfig.getCheckIntervalSec();
    checkTgtExecutor =
        KerberosAuthUtils.startTicketRefresh(
            loginUser, checkInterval, "check-Iceberg-Hive-tgt-%d", LOG);
  }

  public File saveKeyTabFileFromUri(Long catalogId) throws IOException {

    KerberosConfig kerberosConfig = new KerberosConfig(conf);

    String keyTabUri = kerberosConfig.getKeytab();

    File keytabsDir = new File("keytabs");
    if (!keytabsDir.exists()) {
      // Ignore the return value, because there exists many Hive catalog operations making
      // this directory.
      keytabsDir.mkdir();
    }

    File keytabFile = new File(String.format(KerberosConfig.GRAVITINO_KEYTAB_FORMAT, catalogId));
    keytabFile.deleteOnExit();
    if (keytabFile.exists() && !keytabFile.delete()) {
      throw new IllegalStateException(
          String.format("Fail to delete keytab file %s", keytabFile.getAbsolutePath()));
    }

    int fetchKeytabFileTimeout = kerberosConfig.getFetchTimeoutSec();
    KerberosAuthUtils.fetchKeytabFromUri(
        keyTabUri, keytabFile, fetchKeytabFileTimeout, false /* allowHdfsKeytabUri */, hadoopConf);

    return keytabFile;
  }

  @Override
  public void close() throws IOException {
    if (checkTgtExecutor != null) {
      checkTgtExecutor.shutdownNow();
    }
  }
}
