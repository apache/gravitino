/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.credential.config;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Config;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;
import org.apache.gravitino.connector.PropertyEntry;
import org.apache.gravitino.credential.CredentialConstants;

public class CredentialConfig extends Config {

  private static final long DEFAULT_CREDENTIAL_CACHE_MAX_SIZE = 10_000L;
  private static final double DEFAULT_CREDENTIAL_CACHE_EXPIRE_RATIO = 0.15d;

  public static final Map<String, PropertyEntry<?>> CREDENTIAL_PROPERTY_ENTRIES =
      new ImmutableMap.Builder<String, PropertyEntry<?>>()
          .put(
              CredentialConstants.CREDENTIAL_PROVIDERS,
              PropertyEntry.stringPropertyEntry(
                  CredentialConstants.CREDENTIAL_PROVIDERS,
                  "Credential providers for the Gravitino catalog, schema, fileset, table, etc.",
                  false /* required */,
                  false /* immutable */,
                  null /* default value */,
                  false /* hidden */,
                  false /* reserved */))
          .put(
              CredentialConstants.CREDENTIAL_CACHE_EXPIRE_RATIO,
              PropertyEntry.doublePropertyEntry(
                  CredentialConstants.CREDENTIAL_CACHE_EXPIRE_RATIO,
                  "Ratio of the credential's expiration time when Gravitino remove credential from the cache.",
                  false /* required */,
                  false /* immutable */,
                  DEFAULT_CREDENTIAL_CACHE_EXPIRE_RATIO /* default value */,
                  false /* hidden */,
                  false /* reserved */))
          .put(
              CredentialConstants.CREDENTIAL_CACHE_MAX_SIZE,
              PropertyEntry.longPropertyEntry(
                  CredentialConstants.CREDENTIAL_CACHE_MAX_SIZE,
                  "Max size for the credential cache.",
                  false /* required */,
                  false /* immutable */,
                  DEFAULT_CREDENTIAL_CACHE_MAX_SIZE /* default value */,
                  false /* hidden */,
                  false /* reserved */))
          .build();

  public static final ConfigEntry<List<String>> CREDENTIAL_PROVIDERS =
      new ConfigBuilder(CredentialConstants.CREDENTIAL_PROVIDERS)
          .doc("Credential providers, separated by comma.")
          .version(ConfigConstants.VERSION_0_8_0)
          .stringConf()
          .toSequence()
          .createWithDefault(Collections.emptyList());

  public static final ConfigEntry<Double> CREDENTIAL_CACHE_EXPIRE_RATIO =
      new ConfigBuilder(CredentialConstants.CREDENTIAL_CACHE_EXPIRE_RATIO)
          .doc(
              "Ratio of the credential's expiration time when Gravitino remove credential from the "
                  + "cache.")
          .version(ConfigConstants.VERSION_0_8_0)
          .doubleConf()
          .checkValue(
              ratio -> ratio >= 0 && ratio < 1,
              "Ratio of the credential's expiration time should greater than or equal to 0 "
                  + "and less than 1.")
          .createWithDefault(DEFAULT_CREDENTIAL_CACHE_EXPIRE_RATIO);

  public static final ConfigEntry<Long> CREDENTIAL_CACHE_MAX_SIZE =
      new ConfigBuilder(CredentialConstants.CREDENTIAL_CACHE_MAX_SIZE)
          .doc("Max cache size for the credential.")
          .version(ConfigConstants.VERSION_0_8_0)
          .longConf()
          .createWithDefault(DEFAULT_CREDENTIAL_CACHE_MAX_SIZE);

  public CredentialConfig(Map<String, String> properties) {
    super(false);
    loadFromMap(properties, k -> true);
  }
}
