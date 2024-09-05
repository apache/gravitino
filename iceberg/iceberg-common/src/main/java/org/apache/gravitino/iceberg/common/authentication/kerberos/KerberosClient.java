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

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KerberosClient implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(KerberosClient.class);

  private final ScheduledThreadPoolExecutor checkTgtExecutor;
  private final Map<String, String> conf;
  private final Configuration hadoopConf;
  private String realm;

  public KerberosClient(Map<String, String> conf, Configuration hadoopConf) {
    this.conf = conf;
    this.hadoopConf = hadoopConf;
    this.checkTgtExecutor =
        new ScheduledThreadPoolExecutor(
            1,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("check-Iceberg-Hive-tgt-%d")
                .build());
  }

  public String getRealm() {
    return realm;
  }

  public void login(String keytabFilePath) throws IOException {
    KerberosConfig kerberosConfig = new KerberosConfig(conf);

    // Check the principal and keytab file
    String catalogPrincipal = kerberosConfig.getPrincipalName();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(catalogPrincipal), "The principal can't be blank");
    @SuppressWarnings("null")
    List<String> principalComponents = Splitter.on('@').splitToList(catalogPrincipal);
    Preconditions.checkArgument(
        principalComponents.size() == 2, "The principal has the wrong format");

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

    this.realm = principalComponents.get(1);
  }

  public File saveKeyTabFileFromUri(Long catalogId) throws IOException {

    KerberosConfig kerberosConfig = new KerberosConfig(conf);

    String keyTabUri = kerberosConfig.getKeytab();
    Preconditions.checkArgument(StringUtils.isNotBlank(keyTabUri), "Keytab uri can't be blank");
    // TODO: Support to download the file from Kerberos HDFS
    Preconditions.checkArgument(
        !keyTabUri.trim().startsWith("hdfs"), "Keytab uri doesn't support to use HDFS");

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

    // TODO: Make the configuration
    int fetchKeytabFileTimeout = kerberosConfig.getFetchTimeoutSec();
    FetchFileUtils.fetchFileFromUri(keyTabUri, keytabFile, fetchKeytabFileTimeout, hadoopConf);

    return keytabFile;
  }

  @Override
  public void close() throws IOException {
    if (checkTgtExecutor != null) {
      checkTgtExecutor.shutdownNow();
    }
  }
}
