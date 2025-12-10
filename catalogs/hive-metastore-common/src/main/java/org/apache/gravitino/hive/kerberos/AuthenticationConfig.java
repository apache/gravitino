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

package org.apache.gravitino.hive.kerberos;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;

import java.util.Map;
import java.util.Properties;
import org.apache.gravitino.Config;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;
import org.apache.hadoop.conf.Configuration;

public class AuthenticationConfig extends Config {

  // The key for the authentication type, currently we support Kerberos and simple
  public static final String AUTH_TYPE_KEY = "authentication.type";

  public static final String IMPERSONATION_ENABLE_KEY = "authentication.impersonation-enable";

  enum AuthenticationType {
    SIMPLE,
    KERBEROS;
  }

  public static final boolean KERBEROS_DEFAULT_IMPERSONATION_ENABLE = false;

  public AuthenticationConfig(Properties properties, Configuration configuration) {
    super(false);
    loadFromHdfsConfiguration(configuration);
    loadFromMap((Map) properties, k -> true);
  }

  private void loadFromHdfsConfiguration(Configuration configuration) {
    String authType = configuration.get(HADOOP_SECURITY_AUTHENTICATION, "simple");
    configMap.put(AUTH_TYPE_KEY, authType);
  }

  public static final ConfigEntry<String> AUTH_TYPE_ENTRY =
      new ConfigBuilder(AUTH_TYPE_KEY)
          .doc(
              "The type of authentication for Hudi catalog, currently we only support simple and Kerberos")
          .version(ConfigConstants.VERSION_1_0_0)
          .stringConf()
          .createWithDefault("simple");

  public static final ConfigEntry<Boolean> ENABLE_IMPERSONATION_ENTRY =
      new ConfigBuilder(IMPERSONATION_ENABLE_KEY)
          .doc("Whether to enable impersonation for the Hudi catalog")
          .version(ConfigConstants.VERSION_1_0_0)
          .booleanConf()
          .createWithDefault(KERBEROS_DEFAULT_IMPERSONATION_ENABLE);

  public String getAuthType() {
    return get(AUTH_TYPE_ENTRY);
  }

  public boolean isKerberosAuth() {
    return AuthenticationConfig.AuthenticationType.KERBEROS.name().equalsIgnoreCase(getAuthType());
  }

  public boolean isImpersonationEnable() {
    return get(ENABLE_IMPERSONATION_ENTRY);
  }
}
