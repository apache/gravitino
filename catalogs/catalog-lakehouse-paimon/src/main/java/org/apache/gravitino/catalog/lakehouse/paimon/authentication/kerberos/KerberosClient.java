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
package org.apache.gravitino.catalog.lakehouse.paimon.authentication.kerberos;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KerberosClient implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(KerberosClient.class);

  public static final String GRAVITINO_KEYTAB_FORMAT =
      "keytabs/gravitino-lakehouse-paimon-%s-keytab";

  private final ScheduledThreadPoolExecutor checkTgtExecutor;
  private final Map<String, String> conf;
  private final Configuration hadoopConf;

  public KerberosClient(Map<String, String> conf, Configuration hadoopConf) {
    this.conf = conf;
    this.hadoopConf = hadoopConf;
    this.checkTgtExecutor = new ScheduledThreadPoolExecutor(1, getThreadFactory("check-tgt"));
  }

  /**
   * performing kerberos authentication based on the keytab file.
   *
   * @param keytabFilePath local keytab file path
   * @throws IOException
   */
  public void login(String keytabFilePath) throws IOException {
    KerberosConfig kerberosConfig = new KerberosConfig(conf);

    // Check the principal and keytab file
    String catalogPrincipal = kerberosConfig.getPrincipalName();

    // Login
    UserGroupInformation.setConfiguration(hadoopConf);
    UserGroupInformation.loginUserFromKeytab(catalogPrincipal, keytabFilePath);
    UserGroupInformation kerberosLoginUgi = UserGroupInformation.getCurrentUser();

    // Refresh the cache if it's out of date.
    int checkInterval = kerberosConfig.getCheckIntervalSec();
    checkTgtExecutor.scheduleAtFixedRate(
        () -> {
          try {
            kerberosLoginUgi.checkTGTAndReloginFromKeytab();
          } catch (Exception e) {
            LOG.error("Fail to refresh ugi token: ", e);
          }
        },
        checkInterval,
        checkInterval,
        TimeUnit.SECONDS);
  }

  public File saveKeyTabFileFromUri(String catalogId) throws IOException {

    KerberosConfig kerberosConfig = new KerberosConfig(conf);

    String keyTabUri = kerberosConfig.getKeytab();

    File keytabsDir = new File("keytabs");
    if (!keytabsDir.exists()) {
      // Ignore the return value, because there exists many Paimon catalog operations making
      // this directory.
      keytabsDir.mkdir();
    }

    File keytabFile = new File(String.format(GRAVITINO_KEYTAB_FORMAT, catalogId));
    keytabFile.deleteOnExit();
    if (keytabFile.exists() && !keytabFile.delete()) {
      throw new IllegalStateException(
          String.format("Fail to delete keytab file %s", keytabFile.getAbsolutePath()));
    }

    int fetchKeytabFileTimeout = kerberosConfig.getFetchTimeoutSec();
    FetchFileUtils.fetchFileFromUri(keyTabUri, keytabFile, fetchKeytabFileTimeout);

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
