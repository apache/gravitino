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
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.storage.OSSProperties;

public class OSSCredentialConfig extends Config {

  public static final ConfigEntry<String> OSS_REGION =
      new ConfigBuilder(OSSProperties.GRAVITINO_OSS_REGION)
          .doc("The region of the OSS service")
          .version(ConfigConstants.VERSION_0_8_0)
          .stringConf()
          .create();

  public static final ConfigEntry<String> OSS_ACCESS_KEY_ID =
      new ConfigBuilder(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID)
          .doc("The static access key ID used to access OSS data")
          .version(ConfigConstants.VERSION_0_8_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> OSS_SECRET_ACCESS_KEY =
      new ConfigBuilder(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET)
          .doc("The static secret access key used to access OSS data")
          .version(ConfigConstants.VERSION_0_8_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> OSS_ROLE_ARN =
      new ConfigBuilder(OSSProperties.GRAVITINO_OSS_ROLE_ARN)
          .doc("OSS role arn")
          .version(ConfigConstants.VERSION_0_8_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> OSS_EXTERNAL_ID =
      new ConfigBuilder(OSSProperties.GRAVITINO_OSS_EXTERNAL_ID)
          .doc("OSS external ID")
          .version(ConfigConstants.VERSION_0_8_0)
          .stringConf()
          .create();

  public static final ConfigEntry<Integer> OSS_TOKEN_EXPIRE_IN_SECS =
      new ConfigBuilder(CredentialConstants.OSS_TOKEN_EXPIRE_IN_SECS)
          .doc("OSS token expire in seconds")
          .version(ConfigConstants.VERSION_0_8_0)
          .intConf()
          .createWithDefault(3600);

  public OSSCredentialConfig(Map<String, String> properties) {
    super(false);
    loadFromMap(properties, k -> true);
  }

  public String region() {
    return this.get(OSS_REGION);
  }

  @NotNull
  public String ossRoleArn() {
    return this.get(OSS_ROLE_ARN);
  }

  @NotNull
  public String accessKeyID() {
    return this.get(OSS_ACCESS_KEY_ID);
  }

  @NotNull
  public String secretAccessKey() {
    return this.get(OSS_SECRET_ACCESS_KEY);
  }

  public String externalID() {
    return this.get(OSS_EXTERNAL_ID);
  }

  public Integer tokenExpireInSecs() {
    return this.get(OSS_TOKEN_EXPIRE_IN_SECS);
  }
}
