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
import org.apache.gravitino.storage.COSProperties;

/**
 * Credential config for Tencent Cloud COS. Covers both the static secret-key path and the dynamic
 * STS (Security Token Service) path used by credential vending.
 */
public class COSCredentialConfig extends Config {

  public static final ConfigEntry<String> COS_REGION =
      new ConfigBuilder(COSProperties.GRAVITINO_COS_REGION)
          .doc("The region of the Tencent Cloud COS service")
          .version(ConfigConstants.VERSION_2_0_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> COS_ACCESS_KEY_ID =
      new ConfigBuilder(COSProperties.GRAVITINO_COS_ACCESS_KEY_ID)
          .doc("The static access key ID (Tencent Cloud SecretId) used to access COS data")
          .version(ConfigConstants.VERSION_2_0_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> COS_SECRET_ACCESS_KEY =
      new ConfigBuilder(COSProperties.GRAVITINO_COS_ACCESS_KEY_SECRET)
          .doc("The static secret access key (Tencent Cloud SecretKey) used to access COS data")
          .version(ConfigConstants.VERSION_2_0_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> COS_ROLE_ARN =
      new ConfigBuilder(COSProperties.GRAVITINO_COS_ROLE_ARN)
          .doc(
              "The Cloud Access Management (CAM) role ARN that the server assumes when issuing"
                  + " STS temporary credentials")
          .version(ConfigConstants.VERSION_2_0_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> COS_EXTERNAL_ID =
      new ConfigBuilder(COSProperties.GRAVITINO_COS_EXTERNAL_ID)
          .doc("Optional external ID for cross-account assume-role")
          .version(ConfigConstants.VERSION_2_0_0)
          .stringConf()
          .create();

  public static final ConfigEntry<String> COS_APP_ID =
      new ConfigBuilder(COSProperties.GRAVITINO_COS_APP_ID)
          .doc("The Tencent Cloud APPID that owns the COS buckets, used to build resource ARNs")
          .version(ConfigConstants.VERSION_2_0_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  /**
   * The maximum {@code DurationSeconds} accepted by Tencent Cloud CAM {@code AssumeRole}, in
   * seconds. Sourced from the {@code tencentcloud-sdk-java-sts} v3.1.1239 model {@code
   * AssumeRoleRequest#DurationSeconds}: default 7200 seconds, maximum 43200 seconds (12 hours). If
   * a future SDK/API bump raises this ceiling, update the constant below and the accompanying error
   * message together.
   */
  private static final int COS_TOKEN_EXPIRE_IN_SECS_MAX = 43200;

  private static final String COS_TOKEN_EXPIRE_IN_SECS_ERROR_MSG =
      "cos-token-expire-in-secs must be a positive integer no greater than "
          + COS_TOKEN_EXPIRE_IN_SECS_MAX
          + " seconds (Tencent Cloud CAM AssumeRole DurationSeconds hard limit)";

  public static final ConfigEntry<Integer> COS_TOKEN_EXPIRE_IN_SECS =
      new ConfigBuilder(CredentialConstants.COS_TOKEN_EXPIRE_IN_SECS)
          .doc(
              "COS STS token expire time in seconds. Must be within Tencent Cloud CAM"
                  + " AssumeRole limits: (0, 43200]. The effective upper bound may be further"
                  + " reduced by the CAM role's MaxSessionDuration setting; the STS API rejects"
                  + " values above the role's limit at call time.")
          .version(ConfigConstants.VERSION_2_0_0)
          .intConf()
          .checkValue(
              v -> v != null && v > 0 && v <= COS_TOKEN_EXPIRE_IN_SECS_MAX,
              COS_TOKEN_EXPIRE_IN_SECS_ERROR_MSG)
          .createWithDefault(3600);

  public COSCredentialConfig(Map<String, String> properties) {
    super(false);
    loadFromMap(properties, k -> true);
  }

  @NotNull
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

  @NotNull
  public String cosRoleArn() {
    return this.get(COS_ROLE_ARN);
  }

  public String externalID() {
    return this.get(COS_EXTERNAL_ID);
  }

  @NotNull
  public String appID() {
    return this.get(COS_APP_ID);
  }

  public Integer tokenExpireInSecs() {
    return this.get(COS_TOKEN_EXPIRE_IN_SECS);
  }
}
