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
package org.apache.gravitino.server.authentication;

import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;

public interface KerberosConfig {

  String KERBEROS_CONFIG_PREFIX = "gravitino.authenticator.kerberos.";

  ConfigEntry<String> PRINCIPAL =
      new ConfigBuilder(KERBEROS_CONFIG_PREFIX + "principal")
          .doc("Indicates the Kerberos principal to be used for HTTP endpoint")
          .version(ConfigConstants.VERSION_0_4_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  ConfigEntry<String> KEYTAB =
      new ConfigBuilder(KERBEROS_CONFIG_PREFIX + "keytab")
          .doc("Location of the keytab file with the credentials for the principal")
          .version(ConfigConstants.VERSION_0_4_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();
}
