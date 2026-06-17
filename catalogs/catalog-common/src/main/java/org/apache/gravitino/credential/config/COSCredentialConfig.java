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

import java.util.Map;
import javax.validation.constraints.NotNull;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;
import org.apache.gravitino.storage.COSProperties;

/**
 * Slim credential config for Tencent Cloud COS, covering only the static secret-key path. STS /
 * token-related entries (role arn, token expire) will be added by a follow-up PR that introduces
 * the dynamic credential vending support.
 */
public class COSCredentialConfig extends Config {

  public static final ConfigEntry<String> COS_REGION =
      new ConfigBuilder(COSProperties.GRAVITINO_COS_REGION)
          .doc("The region of the Tencent Cloud COS service")
          .version(ConfigConstants.VERSION_1_4_0)
          .stringConf()
          .create();

  public static final ConfigEntry<String> COS_ACCESS_KEY_ID =
      new ConfigBuilder(COSProperties.GRAVITINO_COS_ACCESS_KEY_ID)
          .doc("The static access key ID (Tencent Cloud SecretId) used to access COS data")
          .version(ConfigConstants.VERSION_1_4_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> COS_SECRET_ACCESS_KEY =
      new ConfigBuilder(COSProperties.GRAVITINO_COS_ACCESS_KEY_SECRET)
          .doc("The static secret access key (Tencent Cloud SecretKey) used to access COS data")
          .version(ConfigConstants.VERSION_1_4_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public COSCredentialConfig(Map<String, String> properties) {
    super(false);
    loadFromMap(properties, k -> true);
  }

  public String region() {
    return this.get(COS_REGION);
  }

  @NotNull
  public String accessKeyID() {
    return this.get(COS_ACCESS_KEY_ID);
  }

  @NotNull
  public String secretAccessKey() {
    return this.get(COS_SECRET_ACCESS_KEY);
  }
}
