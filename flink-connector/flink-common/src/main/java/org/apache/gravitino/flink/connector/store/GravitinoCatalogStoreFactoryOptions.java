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

package org.apache.gravitino.flink.connector.store;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class GravitinoCatalogStoreFactoryOptions {

  private GravitinoCatalogStoreFactoryOptions() {}

  public static final String GRAVITINO = "gravitino";

  public static final ConfigOption<String> GRAVITINO_URI =
      ConfigOptions.key("gravitino.uri")
          .stringType()
          .noDefaultValue()
          .withDescription("The uri of Gravitino server");
  public static final ConfigOption<String> GRAVITINO_METALAKE =
      ConfigOptions.key("gravitino.metalake")
          .stringType()
          .noDefaultValue()
          .withDescription("The name of Gravitino metalake");
  public static final ConfigOption<Boolean> GRAVITINO_ENABLE_SESSION_CATALOG_SUPPORT =
      ConfigOptions.key("gravitino.enableSessionCatalogSupport")
          .booleanType()
          .defaultValue(false)
          .withDescription(
              "Whether to enable support for Flink's session catalog in the Gravitino catalog store");

  /**
   * Catalog names that are never resolved against Gravitino in the current Flink session. A session
   * catalog can then be created under the same name as an existing Gravitino catalog without
   * loading, modifying, or dropping the Gravitino catalog. Only valid when {@link
   * #GRAVITINO_ENABLE_SESSION_CATALOG_SUPPORT} is {@code true}.
   */
  public static final ConfigOption<List<String>> GRAVITINO_EXCLUDED_CATALOGS =
      ConfigOptions.key("gravitino.excludedCatalogs")
          .stringType()
          .asList()
          .defaultValues()
          .withDescription(
              "Semicolon-separated catalog names that are never resolved against Gravitino in "
                  + "this Flink session. Session catalogs can be created under these names even "
                  + "if Gravitino has catalogs with the same names, and the Gravitino catalogs "
                  + "are never loaded, modified, or dropped by this session. Only valid when "
                  + "'gravitino.enableSessionCatalogSupport' is true.");

  public static final ConfigOption<Map<String, String>> GRAVITINO_CLIENT_CONFIG =
      ConfigOptions.key("gravitino.client")
          .mapType()
          .defaultValue(ImmutableMap.of())
          .withDescription("The config of Gravitino client");

  public static final String AUTH_TYPE = "gravitino.client.auth.type";
  public static final String OAUTH2 = "oauth2";
  public static final String BASIC = "basic";

  // Basic auth config keys
  public static final String BASIC_USERNAME = "gravitino.client.basic.username";
  public static final String BASIC_PASSWORD = "gravitino.client.basic.password";

  // OAuth2 config keys
  public static final String OAUTH2_SERVER_URI = "gravitino.client.oauth2.serverUri";
  public static final String OAUTH2_CREDENTIAL = "gravitino.client.oauth2.credential";
  public static final String OAUTH2_TOKEN_PATH = "gravitino.client.oauth2.tokenPath";
  public static final String OAUTH2_SCOPE = "gravitino.client.oauth2.scope";
}
