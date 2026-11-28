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

package org.apache.gravitino.spark.connector.hive;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.credential.AzureAccountKeyCredential;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialPropertyUtils;
import org.apache.gravitino.credential.OSSSecretKeyCredential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.catalog.BaseCatalog;
import org.apache.gravitino.spark.connector.utils.VendedCredentialOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.kyuubi.spark.connector.hive.HiveTable;
import org.apache.kyuubi.spark.connector.hive.HiveTableCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GravitinoHiveCatalog extends BaseCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoHiveCatalog.class);

  private static final String OSS_ACCESS_KEY = "fs.oss.accessKeyId";
  private static final String OSS_SECRET_KEY = "fs.oss.accessKeySecret";

  @Override
  protected TableCatalog createAndInitSparkCatalog(
      String name, CaseInsensitiveStringMap options, Map<String, String> properties) {
    HiveTableCatalog hiveCatalog = new HiveTableCatalog();
    Map<String, String> all =
        getPropertiesConverter().toSparkCatalogProperties(options, properties);
    Credential[] credentials = CredentialPropertyUtils.getCredentials(gravitinoCatalog());
    Configuration sessionHadoopConf = VendedCredentialOptions.sessionHadoopConf();
    applyOptionCredentials(credentials, all, sessionHadoopConf);
    hiveCatalog.initialize(name, new CaseInsensitiveStringMap(all));
    // The catalog's configuration only exists once the catalog is initialized; it is the one the
    // catalog's scans and writes read.
    applyOSSCredential(credentials, hiveCatalog.hadoopConfiguration(), all, sessionHadoopConf);
    return hiveCatalog;
  }

  /**
   * Places the vended credentials that can travel in the catalog options into them: S3 and Azure,
   * under the keys the object stores read, and only where the key is not configured. The kyuubi
   * catalog built from these options copies them into the Hadoop configuration it uses for data
   * access. The vended OSS pair cannot travel this way; applyOSSCredential places it.
   *
   * <p>A key is withheld only for a configured key. An Azure account that authenticates with SAS or
   * OAuth still gets its vended account key, which hadoop-azure then ignores: it reads the key only
   * when the account authenticates with a shared key.
   *
   * @param credentials the credentials the Gravitino catalog vends
   * @param props the merged Spark catalog options
   * @param sessionHadoopConf the session's Hadoop configuration, in which session-level keys are
   *     configured; null when there is no live Spark session
   */
  static void applyOptionCredentials(
      Credential[] credentials,
      Map<String, String> props,
      @Nullable Configuration sessionHadoopConf) {
    for (Credential credential : credentials) {
      if (credential instanceof OSSSecretKeyCredential) {
        // OSS credentials are placed by applyOSSCredential, once the catalog's configuration
        // exists.
        continue;
      } else if (credential instanceof S3SecretKeyCredential) {
        S3SecretKeyCredential s3 = (S3SecretKeyCredential) credential;
        VendedCredentialOptions.putPairUnlessConfigured(
            props,
            sessionHadoopConf,
            "fs.s3a.access.key",
            s3.accessKeyId(),
            "fs.s3a.secret.key",
            s3.secretAccessKey());
      } else if (credential instanceof AzureAccountKeyCredential) {
        AzureAccountKeyCredential azure = (AzureAccountKeyCredential) credential;
        // hadoop-azure looks the account key up under the host name of the table location, which
        // is <account>.dfs.core.windows.net on the public Azure Data Lake endpoint, and falls back
        // to one key for all accounts. A configured key of either form wins. The key reaches the
        // catalog in lower case, so the session is checked under that name.
        VendedCredentialOptions.putKeyUnlessConfigured(
            props,
            sessionHadoopConf,
            String.format(
                "fs.azure.account.key.%s.dfs.core.windows.net",
                azure.accountName().toLowerCase(Locale.ROOT)),
            azure.accountKey(),
            "fs.azure.account.key");
      } else {
        LOG.warn(
            "Received unrecognized credential type '{}' for Hive catalog, skipping",
            credential.getClass().getName());
      }
    }
  }

  /**
   * Places OSS credentials into the Hadoop configuration of the kyuubi catalog, under the two names
   * the OSS filesystem reads. They cannot go through the catalog options: those reach the catalog
   * in lower case, and these names keep upper-case letters. OSS keys given as catalog options are
   * written as given, over the session's, the way the catalog applies every other option; the
   * vended pair is then not written, since one vended half next to a configured half would not
   * authenticate. Otherwise the vended pair is written unless the session configures an OSS key.
   *
   * @param credentials the credentials the Gravitino catalog vends
   * @param hadoopConf the Hadoop configuration of the kyuubi catalog
   * @param props the merged Spark catalog options
   * @param sessionHadoopConf the session's Hadoop configuration, in which session-level keys are
   *     configured; null when there is no live Spark session
   */
  static void applyOSSCredential(
      Credential[] credentials,
      Configuration hadoopConf,
      Map<String, String> props,
      @Nullable Configuration sessionHadoopConf) {
    String accessKey = VendedCredentialOptions.optionValue(props, OSS_ACCESS_KEY);
    String secretKey = VendedCredentialOptions.optionValue(props, OSS_SECRET_KEY);
    if (accessKey != null || secretKey != null) {
      if (accessKey != null) {
        hadoopConf.set(OSS_ACCESS_KEY, accessKey);
      }
      if (secretKey != null) {
        hadoopConf.set(OSS_SECRET_KEY, secretKey);
      }
      LOG.info("OSS credentials given as catalog options are used");
      return;
    }
    if (VendedCredentialOptions.isConfiguredInSession(
        sessionHadoopConf, OSS_ACCESS_KEY, OSS_SECRET_KEY)) {
      if (Arrays.stream(credentials).anyMatch(OSSSecretKeyCredential.class::isInstance)) {
        LOG.info(
            "The vended OSS credentials are not used: an OSS key is configured on the session");
      }
      return;
    }
    for (Credential credential : credentials) {
      if (credential instanceof OSSSecretKeyCredential) {
        OSSSecretKeyCredential oss = (OSSSecretKeyCredential) credential;
        hadoopConf.set(OSS_ACCESS_KEY, oss.accessKeyId());
        hadoopConf.set(OSS_SECRET_KEY, oss.secretAccessKey());
        LOG.info(
            "Vended OSS credentials are written under keys '{}' and '{}'",
            OSS_ACCESS_KEY,
            OSS_SECRET_KEY);
      }
    }
  }

  @Override
  protected org.apache.spark.sql.connector.catalog.Table createSparkTable(
      Identifier identifier,
      Table gravitinoTable,
      org.apache.spark.sql.connector.catalog.Table sparkTable,
      TableCatalog sparkHiveCatalog,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter sparkTransformConverter,
      SparkTypeConverter sparkTypeConverter) {
    return new SparkHiveTable(
        identifier,
        gravitinoTable,
        (HiveTable) sparkTable,
        (HiveTableCatalog) sparkHiveCatalog,
        propertiesConverter,
        sparkTransformConverter,
        sparkTypeConverter);
  }

  @Override
  protected PropertiesConverter getPropertiesConverter() {
    return HivePropertiesConverter.getInstance();
  }

  @Override
  protected SparkTransformConverter getSparkTransformConverter() {
    return new SparkTransformConverter(false);
  }

  @Override
  protected SparkTypeConverter getSparkTypeConverter() {
    return new SparkHiveTypeConverter();
  }

  @Override
  protected boolean supportsSparkViews() {
    return true;
  }

  @Override
  protected org.apache.spark.sql.connector.catalog.Table createSparkView(
      Identifier ident,
      View gravitinoView,
      org.apache.spark.sql.connector.catalog.Table sparkTable) {
    return new SparkHiveView(
        gravitinoView,
        name(),
        getDatabase(ident),
        (HiveTable) sparkTable,
        (HiveTableCatalog) sparkCatalog,
        getSparkTypeConverter());
  }
}
