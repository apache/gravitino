/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.hadoop.kerberos;

import static com.datastrato.gravitino.catalog.hadoop.kerberos.KerberosConfig.GRAVITINO_KEYTAB_FORMAT;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KerberosClient {
  private static final Logger LOG = LoggerFactory.getLogger(KerberosClient.class);

  private final ScheduledThreadPoolExecutor checkTgtExecutor;
  private final Map<String, String> conf;
  private final Configuration hadoopConf;

  public KerberosClient(Map<String, String> conf, Configuration hadoopConf) {
    this.conf = conf;
    this.hadoopConf = hadoopConf;
    this.checkTgtExecutor = new ScheduledThreadPoolExecutor(1, getThreadFactory("check-tgt"));
  }

  public String login(String keytabFilePath) throws IOException {
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

    return principalComponents.get(1);
  }

  public File saveKeyTabFileFromUri(Long catalogId) throws IOException {

    KerberosConfig kerberosConfig = new KerberosConfig(conf);

    String keyTabUri = kerberosConfig.getKeytab();
    Preconditions.checkArgument(StringUtils.isNotBlank(keyTabUri), "Keytab uri can't be blank");
    // TODO: Support to download the file from Kerberos HDFS
    Preconditions.checkArgument(
        !keyTabUri.trim().startsWith("hdfs"), "Keytab uri doesn't support to use HDFS");

    java.io.File keytabsDir = new File("keytabs");
    if (!keytabsDir.exists()) {
      // Ignore the return value, because there exists many Hive catalog operations making
      // this directory.
      keytabsDir.mkdir();
    }

    File keytabFile = new File(String.format(GRAVITINO_KEYTAB_FORMAT, catalogId));
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

  private static ThreadFactory getThreadFactory(String factoryName) {
    return new ThreadFactoryBuilder().setDaemon(true).setNameFormat(factoryName + "-%d").build();
  }
}
