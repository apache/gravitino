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
import org.apache.gravitino.storage.S3Properties;

public class S3CredentialConfig extends Config {

  public static final ConfigEntry<String> S3_REGION =
      new ConfigBuilder(S3Properties.GRAVITINO_S3_REGION)
          .doc("The region of the S3 service")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .create();

  public static final ConfigEntry<String> S3_ACCESS_KEY_ID =
      new ConfigBuilder(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID)
          .doc("The static access key ID used to access S3 data")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> S3_SECRET_ACCESS_KEY =
      new ConfigBuilder(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY)
          .doc("The static secret access key used to access S3 data")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> S3_ROLE_ARN =
      new ConfigBuilder(S3Properties.GRAVITINO_S3_ROLE_ARN)
          .doc("S3 role arn")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> S3_EXTERNAL_ID =
      new ConfigBuilder(S3Properties.GRAVITINO_S3_EXTERNAL_ID)
          .doc("S3 external ID")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .create();

  public static final ConfigEntry<Integer> S3_TOKEN_EXPIRE_IN_SECS =
      new ConfigBuilder(CredentialConstants.S3_TOKEN_EXPIRE_IN_SECS)
          .doc("S3 token expire in seconds")
          .version(ConfigConstants.VERSION_0_7_0)
          .intConf()
          .createWithDefault(3600);

  public static final ConfigEntry<String> S3_STS_ENDPOINT =
      new ConfigBuilder(S3Properties.GRAVITINO_S3_STS_ENDPOINT)
          .doc("S3 STS endpoint")
          .version(ConfigConstants.VERSION_0_8_0)
          .stringConf()
          .create();

  public S3CredentialConfig(Map<String, String> properties) {
    super(false);
    loadFromMap(properties, k -> true);
  }

  @NotNull
  public String s3RoleArn() {
    return this.get(S3_ROLE_ARN);
  }

  @NotNull
  public String accessKeyID() {
    return this.get(S3_ACCESS_KEY_ID);
  }

  @NotNull
  public String secretAccessKey() {
    return this.get(S3_SECRET_ACCESS_KEY);
  }

  public String region() {
    return this.get(S3_REGION);
  }

  public String externalID() {
    return this.get(S3_EXTERNAL_ID);
  }

  public Integer tokenExpireInSecs() {
    return this.get(S3_TOKEN_EXPIRE_IN_SECS);
  }

  public String stsEndpoint() {
    return this.get(S3_STS_ENDPOINT);
  }
}
