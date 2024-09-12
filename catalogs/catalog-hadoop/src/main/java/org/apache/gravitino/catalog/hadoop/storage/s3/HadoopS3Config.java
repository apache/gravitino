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
package org.apache.gravitino.catalog.hadoop.storage.s3;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.catalog.hadoop.authentication.AuthenticationConfig;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;
import org.apache.gravitino.connector.PropertyEntry;

public class HadoopS3Config extends AuthenticationConfig {
  public static final String ACCESS_KEY = "s3-access-key-id";
  public static final String SECRET_KEY = "s3-secret-access-key";
  public static final String SESSION_TOKEN = "s3-session-token";
  public static final String CREDENTIALS_PROVIDER = "s3-credentials-provider";
  public static final String ENDPOINT = "s3-endpoint";
  public static final String REGION = "s3-region";
  public static final String SSL_ENABLED = "s3-ssl-enabled";
  public static final String PATH_STYLE_ENABLED = "s3-path-style-enabled";

  public static final ConfigEntry<String> ACCESS_KEY_ENTRY =
      new ConfigBuilder(ACCESS_KEY)
          .doc("The access key of S3")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .create();
  public static final ConfigEntry<String> SECRET_KEY_ENTRY =
      new ConfigBuilder(SECRET_KEY)
          .doc("The secret key of S3")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .create();

  public static final ConfigEntry<String> SESSION_TOKEN_ENTRY =
      new ConfigBuilder(SESSION_TOKEN)
          .doc("The session token of S3")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .create();

  public static final ConfigEntry<String> CREDENTIALS_PROVIDER_ENTRY =
      new ConfigBuilder(CREDENTIALS_PROVIDER)
          .doc("The credentials provider of S3, only anonymous, token and simple are supported")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .createWithDefault("simple");

  public static final ConfigEntry<String> ENDPOINT_ENTRY =
      new ConfigBuilder(ENDPOINT)
          .doc("The endpoint of S3")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .create();

  public static final ConfigEntry<String> REGION_ENTRY =
      new ConfigBuilder(REGION)
          .doc("The region of AWS S3")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .create();

  public static final ConfigEntry<Boolean> SSL_ENABLED_ENTRY =
      new ConfigBuilder(SSL_ENABLED)
          .doc("Enables or disables SSL connections to S3")
          .version(ConfigConstants.VERSION_0_7_0)
          .booleanConf()
          .createWithDefault(false);

  public static final ConfigEntry<Boolean> PATH_STYLE_ENABLED_ENTRY =
      new ConfigBuilder(PATH_STYLE_ENABLED)
          .doc("Enable S3 path style access ie disabling the default virtual hosting behaviour")
          .version(ConfigConstants.VERSION_0_7_0)
          .booleanConf()
          .createWithDefault(false);

  public HadoopS3Config(Map<String, String> properties) {
    super(properties);
  }

  public String getAccessKey() {
    return get(ACCESS_KEY_ENTRY);
  }

  public String getSecretKey() {
    return get(SECRET_KEY_ENTRY);
  }

  public String getSessionToken() {
    return get(SESSION_TOKEN_ENTRY);
  }

  public String getCredentialsProvider() {
    return get(CREDENTIALS_PROVIDER_ENTRY);
  }

  public String getEndpoint() {
    return get(ENDPOINT_ENTRY);
  }

  public String getRegion() {
    return get(REGION_ENTRY);
  }

  public Boolean isSSLEnabled() {
    return get(SSL_ENABLED_ENTRY);
  }

  public Boolean isPathStyleEnabled() {
    return get(PATH_STYLE_ENABLED_ENTRY);
  }

  public static final Map<String, PropertyEntry<?>> S3_PROPERTY_ENTRIES =
      new ImmutableMap.Builder<String, PropertyEntry<?>>()
          .put(
              ACCESS_KEY,
              PropertyEntry.stringOptionalPropertyEntry(
                  ACCESS_KEY,
                  "The access key of S3",
                  false /* immutable */,
                  null /* default value */,
                  false /* hidden */))
          .put(
              SECRET_KEY,
              PropertyEntry.stringOptionalPropertyEntry(
                  SECRET_KEY,
                  "The secret key of S3",
                  false /* immutable */,
                  null /* defaultValue */,
                  true /* hidden */))
          .put(
              SESSION_TOKEN,
              PropertyEntry.stringOptionalPropertyEntry(
                  SESSION_TOKEN,
                  "The session token of S3",
                  false /* immutable */,
                  null /* defaultValue */,
                  true /* hidden */))
          .put(
              CREDENTIALS_PROVIDER,
              PropertyEntry.stringOptionalPropertyEntry(
                  CREDENTIALS_PROVIDER,
                  "The credentials provider of S3, only anonymous, token and simple are supported",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              ENDPOINT,
              PropertyEntry.stringOptionalPropertyEntry(
                  ENDPOINT,
                  "The endpoint of S3",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              REGION,
              PropertyEntry.stringOptionalPropertyEntry(
                  REGION,
                  "The region of AWS S3",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              SSL_ENABLED,
              PropertyEntry.booleanPropertyEntry(
                  SSL_ENABLED,
                  "Enables or disables SSL connections to S3",
                  false /* required */,
                  false /* immutable */,
                  false /* defaultValue */,
                  false /* hidden */,
                  false /*reserved */))
          .put(
              PATH_STYLE_ENABLED,
              PropertyEntry.booleanPropertyEntry(
                  PATH_STYLE_ENABLED,
                  "Enable S3 path style access ie disabling the default virtual hosting behaviour",
                  false /* required */,
                  false /* immutable */,
                  false /* defaultValue */,
                  false /* hidden */,
                  false /*reserved */))
          .build();
}
