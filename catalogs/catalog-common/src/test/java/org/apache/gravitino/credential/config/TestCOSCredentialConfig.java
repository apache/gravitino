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
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.storage.COSProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCOSCredentialConfig {

  /**
   * Baseline properties for a COS STS credential config; individual tests override the field under
   * test. Kept minimal (all required-for-STS fields present) so blank-value tests exercise only the
   * specific field they target.
   */
  private static Map<String, String> baseProps() {
    Map<String, String> props = new HashMap<>();
    props.put(COSProperties.GRAVITINO_COS_REGION, "ap-shanghai");
    props.put(COSProperties.GRAVITINO_COS_ACCESS_KEY_ID, "ak");
    props.put(COSProperties.GRAVITINO_COS_ACCESS_KEY_SECRET, "sk");
    props.put(COSProperties.GRAVITINO_COS_ROLE_ARN, "qcs::cam::uin/100:roleName/role");
    props.put(COSProperties.GRAVITINO_COS_APP_ID, "1259000000");
    return props;
  }

  @Test
  void testTokenExpireDefaultsTo3600WhenAbsent() {
    // Sanity check: when the caller does not set cos-token-expire-in-secs at all, the default
    // (3600s) is applied and the range check does not reject the default.
    COSCredentialConfig config = new COSCredentialConfig(baseProps());
    Assertions.assertEquals(3600, config.tokenExpireInSecs());
  }

  @Test
  void testTokenExpireAcceptsBoundaryValues() {
    // 1s and 43200s are the extreme values the range check is expected to allow.
    // 43200s (12h) is the documented maximum for Tencent Cloud CAM AssumeRole DurationSeconds
    // per tencentcloud-sdk-java-sts v3.1.1239.
    for (int valid : new int[] {1, 3600, 43200}) {
      Map<String, String> props =
          ImmutableMap.<String, String>builder()
              .putAll(baseProps())
              .put(CredentialConstants.COS_TOKEN_EXPIRE_IN_SECS, String.valueOf(valid))
              .build();
      COSCredentialConfig config = new COSCredentialConfig(props);
      Assertions.assertEquals(valid, config.tokenExpireInSecs(), "value=" + valid);
    }
  }

  @Test
  void testTokenExpireRejectsNonPositiveValues() {
    // Zero and negative values are meaningless for an STS token TTL. Fail fast at read time
    // rather than surfacing a Tencent Cloud STS API error at runtime.
    //
    // Note: Config#loadFromMap only records raw strings; ConfigEntry validators fire lazily on
    // Config#get. That's why the assertion wraps tokenExpireInSecs() rather than the constructor.
    for (int invalid : new int[] {0, -1, -3600}) {
      Map<String, String> props =
          ImmutableMap.<String, String>builder()
              .putAll(baseProps())
              .put(CredentialConstants.COS_TOKEN_EXPIRE_IN_SECS, String.valueOf(invalid))
              .build();
      COSCredentialConfig config = new COSCredentialConfig(props);
      Assertions.assertThrows(
          IllegalArgumentException.class, config::tokenExpireInSecs, "value=" + invalid);
    }
  }

  @Test
  void testTokenExpireRejectsValuesAboveTencentCloudLimit() {
    // 43201s and above are guaranteed to be rejected by Tencent Cloud STS at call time; catching
    // them here gives a clearer error message tied to the SDK version we depend on.
    for (int invalid : new int[] {43201, 86400, Integer.MAX_VALUE}) {
      Map<String, String> props =
          ImmutableMap.<String, String>builder()
              .putAll(baseProps())
              .put(CredentialConstants.COS_TOKEN_EXPIRE_IN_SECS, String.valueOf(invalid))
              .build();
      COSCredentialConfig config = new COSCredentialConfig(props);
      Assertions.assertThrows(
          IllegalArgumentException.class, config::tokenExpireInSecs, "value=" + invalid);
    }
  }

  @Test
  void testAccessorsReadBaselineProperties() {
    // Sanity check that the getters return exactly what was fed in via loadFromMap. Guards
    // against future refactors accidentally aliasing property keys.
    COSCredentialConfig config = new COSCredentialConfig(baseProps());
    Assertions.assertEquals("ap-shanghai", config.region());
    Assertions.assertEquals("ak", config.accessKeyID());
    Assertions.assertEquals("sk", config.secretAccessKey());
    Assertions.assertEquals("qcs::cam::uin/100:roleName/role", config.cosRoleArn());
    Assertions.assertEquals("1259000000", config.appID());
  }

  @Test
  void testExternalIdIsOptional() {
    // cos-external-id has no NotBlank check because most tenants do not use cross-account
    // AssumeRole. When omitted, externalID() must return null so COSTokenGenerator can skip
    // setting ExternalId on the AssumeRole request.
    COSCredentialConfig config = new COSCredentialConfig(baseProps());
    Assertions.assertNull(config.externalID());

    Map<String, String> withExtId =
        ImmutableMap.<String, String>builder()
            .putAll(baseProps())
            .put(COSProperties.GRAVITINO_COS_EXTERNAL_ID, "ext-42")
            .build();
    COSCredentialConfig withExt = new COSCredentialConfig(withExtId);
    Assertions.assertEquals("ext-42", withExt.externalID());
  }

  @Test
  void testRegionIsRequired() {
    // A blank / missing cos-region weakens the STS session policy (region ARN would degrade to
    // a wildcard). ConfigEntry.checkValue should reject it.
    for (String blank : new String[] {"", "   "}) {
      Map<String, String> props = new HashMap<>(baseProps());
      props.put(COSProperties.GRAVITINO_COS_REGION, blank);
      COSCredentialConfig config = new COSCredentialConfig(props);
      Assertions.assertThrows(
          IllegalArgumentException.class, config::region, "region=[" + blank + "]");
    }
    // Missing altogether: reading a required entry with no default must throw as well.
    Map<String, String> missing = new HashMap<>(baseProps());
    missing.remove(COSProperties.GRAVITINO_COS_REGION);
    COSCredentialConfig config = new COSCredentialConfig(missing);
    Assertions.assertThrows(RuntimeException.class, config::region);
  }

  @Test
  void testRoleArnIsRequiredForStsPath() {
    // cos-role-arn is what tells the server which CAM role to AssumeRole into. Without it the
    // STS provider would call AssumeRole with a null RoleArn and the API would 4xx. Catching it
    // in config validation gives a clearer error tied to the property name.
    for (String blank : new String[] {"", "   "}) {
      Map<String, String> props = new HashMap<>(baseProps());
      props.put(COSProperties.GRAVITINO_COS_ROLE_ARN, blank);
      COSCredentialConfig config = new COSCredentialConfig(props);
      Assertions.assertThrows(
          IllegalArgumentException.class, config::cosRoleArn, "role-arn=[" + blank + "]");
    }
  }

  @Test
  void testAppIdIsRequiredForStsPath() {
    // cos-app-id is required to build the resource ARN "<bucket>-<APPID>" that scopes the STS
    // session policy. Without it the policy would allow the wrong bucket, so we hard-fail early.
    for (String blank : new String[] {"", "   "}) {
      Map<String, String> props = new HashMap<>(baseProps());
      props.put(COSProperties.GRAVITINO_COS_APP_ID, blank);
      COSCredentialConfig config = new COSCredentialConfig(props);
      Assertions.assertThrows(
          IllegalArgumentException.class, config::appID, "app-id=[" + blank + "]");
    }
  }
}
