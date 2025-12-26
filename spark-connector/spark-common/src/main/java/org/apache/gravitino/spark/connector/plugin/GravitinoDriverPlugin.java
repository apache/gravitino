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

package org.apache.gravitino.spark.connector.plugin;

import static org.apache.gravitino.spark.connector.ConnectorConstants.COMMA;
import static org.apache.gravitino.spark.connector.GravitinoSparkConfig.GRAVITINO_CLIENT_CONFIG_PREFIX;
import static org.apache.gravitino.spark.connector.utils.ConnectorUtil.removeDuplicateSparkExtensions;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.auth.AuthProperties;
import org.apache.gravitino.client.DefaultOAuth2TokenProvider;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.client.GravitinoClient.ClientBuilder;
import org.apache.gravitino.client.GravitinoClientConfiguration;
import org.apache.gravitino.client.KerberosTokenProvider;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.apache.gravitino.spark.connector.iceberg.extensions.GravitinoIcebergSparkSessionExtensions;
import org.apache.gravitino.spark.connector.version.CatalogNameAdaptor;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.PluginContext;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GravitinoDriverPlugin creates GravitinoCatalogManager to fetch catalogs from Apache Gravitino and
 * register Gravitino catalogs to Apache Spark.
 */
public class GravitinoDriverPlugin implements DriverPlugin {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoDriverPlugin.class);

  @VisibleForTesting
  static final String PAIMON_SPARK_EXTENSIONS =
      "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions";

  @VisibleForTesting
  static final String ICEBERG_SPARK_EXTENSIONS =
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions";

  private GravitinoCatalogManager catalogManager;
  private final List<String> gravitinoIcebergExtensions =
      Arrays.asList(
          GravitinoIcebergSparkSessionExtensions.class.getName(), ICEBERG_SPARK_EXTENSIONS);
  private final List<String> gravitinoPaimonExtensions = Arrays.asList(PAIMON_SPARK_EXTENSIONS);

  private final List<String> gravitinoDriverExtensions = new ArrayList<>();
  private boolean enableIcebergSupport = false;
  private boolean enablePaimonSupport = false;

  @Override
  public Map<String, String> init(SparkContext sc, PluginContext pluginContext) {
    SparkConf conf = sc.conf();
    String gravitinoUri = conf.get(GravitinoSparkConfig.GRAVITINO_URI);
    String metalake = conf.get(GravitinoSparkConfig.GRAVITINO_METALAKE);
    Map<String, String> gravitinoClientConfig = extractGravitinoClientConfig(conf);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(gravitinoUri),
        String.format(
            "%s:%s, should not be empty", GravitinoSparkConfig.GRAVITINO_URI, gravitinoUri));
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metalake),
        String.format(
            "%s:%s, should not be empty", GravitinoSparkConfig.GRAVITINO_METALAKE, metalake));

    this.enableIcebergSupport =
        conf.getBoolean(GravitinoSparkConfig.GRAVITINO_ENABLE_ICEBERG_SUPPORT, false);
    this.enablePaimonSupport =
        conf.getBoolean(GravitinoSparkConfig.GRAVITINO_ENABLE_PAIMON_SUPPORT, false);
    if (enablePaimonSupport) {
      gravitinoDriverExtensions.addAll(gravitinoPaimonExtensions);
    }
    if (enableIcebergSupport) {
      gravitinoDriverExtensions.addAll(gravitinoIcebergExtensions);
    }

    this.catalogManager =
        GravitinoCatalogManager.create(
            () ->
                createGravitinoClient(
                    gravitinoUri, metalake, conf, sc.sparkUser(), gravitinoClientConfig));
    catalogManager.loadRelationalCatalogs();
    registerGravitinoCatalogs(conf, catalogManager.getCatalogs());
    registerSqlExtensions(conf);
    return Collections.emptyMap();
  }

  @Override
  public void shutdown() {
    if (catalogManager != null) {
      catalogManager.close();
    }
  }

  private void registerGravitinoCatalogs(
      SparkConf sparkConf, Map<String, Catalog> gravitinoCatalogs) {
    gravitinoCatalogs
        .entrySet()
        .forEach(
            entry -> {
              String catalogName = entry.getKey();
              Catalog gravitinoCatalog = entry.getValue();
              String provider = gravitinoCatalog.provider();
              if ("lakehouse-iceberg".equals(provider.toLowerCase(Locale.ROOT))
                  && !enableIcebergSupport) {
                return;
              }
              if ("lakehouse-paimon".equals(provider.toLowerCase(Locale.ROOT))
                  && !enablePaimonSupport) {
                return;
              }
              try {
                registerCatalog(sparkConf, catalogName, provider);
              } catch (Exception e) {
                LOG.warn("Register catalog {} failed.", catalogName, e);
              }
            });
  }

  private void registerCatalog(SparkConf sparkConf, String catalogName, String provider) {
    if (StringUtils.isBlank(provider)) {
      LOG.warn("Skip registering {} because catalog provider is empty.", catalogName);
      return;
    }

    String catalogClassName = CatalogNameAdaptor.getCatalogName(provider);
    if (StringUtils.isBlank(catalogClassName)) {
      LOG.warn("Skip registering {} because {} is not supported yet.", catalogName, provider);
      return;
    }

    String sparkCatalogConfigName = "spark.sql.catalog." + catalogName;
    Preconditions.checkArgument(
        !sparkConf.contains(sparkCatalogConfigName),
        catalogName + " is already registered to SparkCatalogManager");
    sparkConf.set(sparkCatalogConfigName, catalogClassName);
    LOG.info("Register {} catalog to Spark catalog manager.", catalogName);
  }

  private void registerSqlExtensions(SparkConf conf) {
    String extensionString = String.join(COMMA, gravitinoDriverExtensions);
    if (conf.contains(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key())) {
      String sparkSessionExtensions = conf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key());
      if (StringUtils.isNotBlank(sparkSessionExtensions)) {
        conf.set(
            StaticSQLConf.SPARK_SESSION_EXTENSIONS().key(),
            removeDuplicateSparkExtensions(
                gravitinoDriverExtensions.toArray(new String[0]),
                sparkSessionExtensions.split(COMMA)));
      } else {
        conf.set(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key(), extensionString);
      }
    } else {
      conf.set(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key(), extensionString);
    }
  }

  private static GravitinoClient createGravitinoClient(
      String uri,
      String metalake,
      SparkConf sparkConf,
      String sparkUser,
      Map<String, String> clientConfig) {
    ClientBuilder builder = GravitinoClient.builder(uri).withMetalake(metalake);
    builder.withClientConfig(clientConfig);
    String authType =
        sparkConf.get(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, AuthProperties.SIMPLE_AUTH_TYPE);
    if (AuthProperties.isSimple(authType)) {
      Preconditions.checkArgument(
          !UserGroupInformation.isSecurityEnabled(),
          "Spark simple auth mode doesn't support setting kerberos configurations");
      builder.withSimpleAuth(sparkUser);
    } else if (AuthProperties.isOAuth2(authType)) {
      String oAuthUri = getRequiredConfig(sparkConf, GravitinoSparkConfig.GRAVITINO_OAUTH2_URI);
      String credential =
          getRequiredConfig(sparkConf, GravitinoSparkConfig.GRAVITINO_OAUTH2_CREDENTIAL);
      String path = getRequiredConfig(sparkConf, GravitinoSparkConfig.GRAVITINO_OAUTH2_PATH);
      String scope = getRequiredConfig(sparkConf, GravitinoSparkConfig.GRAVITINO_OAUTH2_SCOPE);
      DefaultOAuth2TokenProvider oAuth2TokenProvider =
          DefaultOAuth2TokenProvider.builder()
              .withUri(oAuthUri)
              .withCredential(credential)
              .withPath(path)
              .withScope(scope)
              .build();
      builder.withOAuth(oAuth2TokenProvider);
    } else if (AuthProperties.isKerberos(authType)) {
      String principal =
          getRequiredConfig(sparkConf, GravitinoSparkConfig.GRAVITINO_KERBEROS_PRINCIPAL);
      String keyTabFile =
          getRequiredConfig(sparkConf, GravitinoSparkConfig.GRAVITINO_KERBEROS_KEYTAB_FILE_PATH);
      KerberosTokenProvider kerberosTokenProvider =
          KerberosTokenProvider.builder()
              .withClientPrincipal(principal)
              .withKeyTabFile(new File(keyTabFile))
              .build();
      builder.withKerberosAuth(kerberosTokenProvider);
    } else {
      throw new UnsupportedOperationException("Unsupported auth type: " + authType);
    }
    return builder.build();
  }

  private static String getRequiredConfig(SparkConf sparkConf, String configKey) {
    String configValue = sparkConf.get(configKey, null);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(configValue), configKey + " should not be empty");
    return configValue;
  }

  @Nullable
  private static String getOptionalConfig(SparkConf sparkConf, String configKey) {
    return sparkConf.get(configKey, null);
  }

  @VisibleForTesting
  public static Map<String, String> extractGravitinoClientConfig(SparkConf conf) {
    return Optional.ofNullable(conf.getAllWithPrefix(GRAVITINO_CLIENT_CONFIG_PREFIX))
        .map(
            arr ->
                Stream.of(arr)
                    .collect(
                        Collectors.toMap(
                            t -> GravitinoClientConfiguration.GRAVITINO_CLIENT_CONFIG_PREFIX + t._1,
                            t -> t._2,
                            (oldVal, newVal) -> newVal)))
        .orElse(ImmutableMap.of());
  }
}
