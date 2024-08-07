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
package org.apache.gravitino.catalog.lakehouse.paimon.authentication;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.Config;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;
import org.apache.gravitino.connector.PropertyEntry;

public class AuthenticationConfig extends Config {

  // The key for the authentication type, currently we support Kerberos and simple
  public static final String AUTH_TYPE_KEY = "authentication.type";

  enum AuthenticationType {
    SIMPLE,
    KERBEROS
  }

  public AuthenticationConfig(Map<String, String> properties) {
    super(false);
    loadFromMap(properties, k -> true);
  }

  public static final ConfigEntry<String> AUTH_TYPE_ENTRY =
      new ConfigBuilder(AUTH_TYPE_KEY)
          .doc(
              "The type of authentication for Paimon catalog, currently we only support simple and Kerberos")
          .version(ConfigConstants.VERSION_0_6_0)
          .stringConf()
          .createWithDefault("simple");

  public String getAuthType() {
    return get(AUTH_TYPE_ENTRY);
  }

  public boolean isSimpleAuth() {
    return AuthenticationType.SIMPLE.name().equalsIgnoreCase(getAuthType());
  }

  public boolean isKerberosAuth() {
    return AuthenticationType.KERBEROS.name().equalsIgnoreCase(getAuthType());
  }

  public static final Map<String, PropertyEntry<?>> AUTHENTICATION_PROPERTY_ENTRIES =
      new ImmutableMap.Builder<String, PropertyEntry<?>>()
          .put(
              AUTH_TYPE_KEY,
              PropertyEntry.stringOptionalPropertyEntry(
                  AUTH_TYPE_KEY,
                  "The type of authentication for Paimon catalog, currently we only support simple and Kerberos",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .build();
}
