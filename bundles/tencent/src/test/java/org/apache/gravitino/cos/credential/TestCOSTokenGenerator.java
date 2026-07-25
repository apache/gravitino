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

package org.apache.gravitino.cos.credential;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCOSTokenGenerator {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private COSTokenGenerator newGenerator() {
    COSTokenGenerator generator = new COSTokenGenerator();
    generator.initializeForTest(
        "ak", "sk", "qcs::cam::uin/100:roleName/role", null, "ap-shanghai", "1259000000", 3600);
    return generator;
  }

  @Test
  void testPolicyContainsBucketAppIdSuffix() throws Exception {
    COSTokenGenerator generator = newGenerator();
    String json =
        generator.buildPolicyForTest(
            ImmutableSet.of("cosn://my-bucket/dataset/foo/"), ImmutableSet.of());
    JsonNode root = MAPPER.readTree(json);
    Assertions.assertEquals("2.0", root.get("version").asText());
    JsonNode statements = root.get("statement");
    Assertions.assertTrue(statements.isArray() && statements.size() > 0);

    boolean foundBucketAppIdInResource = false;
    for (JsonNode stmt : statements) {
      JsonNode resources = stmt.get("resource");
      if (resources != null && resources.isArray()) {
        for (JsonNode r : resources) {
          if (r.asText().contains("my-bucket-1259000000")) {
            foundBucketAppIdInResource = true;
            break;
          }
        }
      }
    }
    Assertions.assertTrue(
        foundBucketAppIdInResource,
        "Resource ARN should append the APPID suffix to the bucket name. Policy: " + json);
  }

  @Test
  void testPolicyOmitsWriteStatementWhenWriteEmpty() throws Exception {
    COSTokenGenerator generator = newGenerator();
    String json =
        generator.buildPolicyForTest(ImmutableSet.of("cosn://my-bucket/read/"), ImmutableSet.of());
    Assertions.assertFalse(json.contains("cos:PutObject"), "Policy: " + json);
    Assertions.assertFalse(json.contains("cos:DeleteObject"), "Policy: " + json);
  }

  @Test
  void testPolicyIncludesWriteStatementWhenWriteSet() throws Exception {
    COSTokenGenerator generator = newGenerator();
    String json =
        generator.buildPolicyForTest(
            ImmutableSet.of("cosn://my-bucket/read/"), ImmutableSet.of("cosn://my-bucket/write/"));
    Assertions.assertTrue(json.contains("cos:PutObject"), "Policy: " + json);
    Assertions.assertTrue(json.contains("cos:DeleteObject"), "Policy: " + json);
    Assertions.assertTrue(json.contains("cos:CompleteMultipartUpload"), "Policy: " + json);
  }

  @Test
  void testPolicyUsesLowerCaseEffectAllow() throws Exception {
    COSTokenGenerator generator = newGenerator();
    String json =
        generator.buildPolicyForTest(ImmutableSet.of("cosn://my-bucket/data/"), ImmutableSet.of());
    Assertions.assertTrue(json.contains("\"effect\":\"allow\""), "Policy: " + json);
    Assertions.assertFalse(json.contains("\"Effect\":\"Allow\""), "Policy: " + json);
  }

  @Test
  void testPolicyUsesCosPrefixCondition() throws Exception {
    COSTokenGenerator generator = newGenerator();
    String json =
        generator.buildPolicyForTest(ImmutableSet.of("cosn://my-bucket/data/"), ImmutableSet.of());
    Assertions.assertTrue(json.contains("string_like"), "Policy: " + json);
    Assertions.assertTrue(json.contains("cos:prefix"), "Policy: " + json);
    Assertions.assertTrue(json.contains("data/*"), "Policy: " + json);
  }

  @Test
  void testBucketWithExistingAppIdSuffixIsKept() throws Exception {
    COSTokenGenerator generator = newGenerator();
    String json =
        generator.buildPolicyForTest(
            ImmutableSet.of("cosn://already-1259000000/data/"), ImmutableSet.of());
    Assertions.assertTrue(json.contains("already-1259000000"), "Policy: " + json);
    // Ensure we did not double-suffix.
    Assertions.assertFalse(json.contains("already-1259000000-1259000000"), "Policy: " + json);
  }

  @Test
  void testBuildPolicyRejectsEmptyLocations() {
    COSTokenGenerator generator = newGenerator();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> generator.buildPolicyForTest(ImmutableSet.of(), ImmutableSet.of()));
  }

  @Test
  void testPolicyAppendsWildcardWhenLocationHasNoTrailingSlash() throws Exception {
    COSTokenGenerator generator = newGenerator();
    String json =
        generator.buildPolicyForTest(ImmutableSet.of("cosn://my-bucket/data"), ImmutableSet.of());
    // A location without a trailing slash is still allowed: the object resource ARN keeps the
    // {@code data/*} form, and the {@code cos:prefix} condition uses {@code data*} (no slash) so
    // it matches the raw prefix that hadoop-cos may pass in list requests.
    Assertions.assertTrue(
        json.contains("data/*"), "object resource missing 'data/*'. Policy: " + json);
    Assertions.assertTrue(
        json.contains("\"data*\""), "cos:prefix pattern 'data*' missing. Policy: " + json);
  }

  @Test
  void testInitializeForTestRejectsBlankRegion() {
    // A blank region would degrade the resource ARN region segment to a wildcard, weakening
    // the STS session policy. The generator must reject it fast.
    COSTokenGenerator generator = new COSTokenGenerator();
    for (String blank : new String[] {null, "", "   "}) {
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              generator.initializeForTest(
                  "ak", "sk", "qcs::cam::uin/100:roleName/role", null, blank, "1259000000", 3600),
          "Blank region should be rejected: [" + blank + "]");
    }
  }

  @Test
  void testPolicyResourceArnCarriesConfiguredRegion() throws Exception {
    // Sanity check that the configured region flows into the resource ARN region segment
    // instead of being replaced by a wildcard.
    COSTokenGenerator generator = newGenerator();
    String json =
        generator.buildPolicyForTest(ImmutableSet.of("cosn://my-bucket/data/"), ImmutableSet.of());
    Assertions.assertTrue(json.contains("qcs::cos:ap-shanghai:"), "Policy: " + json);
    Assertions.assertFalse(json.contains("qcs::cos:*:"), "Policy: " + json);
  }

  @Test
  void testPolicyIncludesHeadBucketAction() throws Exception {
    // hadoop-cos calls headBucket during FileSystem.initialize(); if the session policy is
    // missing cos:HeadBucket, the vended credentials return 403 Forbidden on that call.
    COSTokenGenerator generator = newGenerator();
    String json =
        generator.buildPolicyForTest(ImmutableSet.of("cosn://my-bucket/data/"), ImmutableSet.of());
    Assertions.assertTrue(json.contains("cos:HeadBucket"), "Policy: " + json);
  }

  @Test
  void testGetBucketUsesWildcardResource() throws Exception {
    // Tencent Cloud CAM requires distinct resource forms for bucket-level actions:
    //   - cos:GetBucket needs the wildcard form (bucket/*).
    //   - cos:HeadBucket / cos:GetBucketLocation need the plain form (bucket/).
    // Locking both behaviours prevents accidental regression.
    COSTokenGenerator generator = newGenerator();
    String json =
        generator.buildPolicyForTest(ImmutableSet.of("cosn://my-bucket/data/"), ImmutableSet.of());
    Assertions.assertTrue(
        json.contains(":my-bucket-1259000000/*"),
        "GetBucket must use bucket/* resource. Policy: " + json);
    Assertions.assertTrue(
        json.contains(":my-bucket-1259000000/\""),
        "HeadBucket must use bucket/ resource. Policy: " + json);
  }

  @Test
  void testPrefixConditionOmitsTrailingSlashInWildcard() throws Exception {
    // The cos:prefix condition must use the pattern {@code data*} (no slash) so that hadoop-cos
    // list requests with prefix "data/" match. Guard against a double slash regression that
    // would happen if the wildcard were appended to a prefix that already ends in "/".
    COSTokenGenerator generator = newGenerator();
    String json =
        generator.buildPolicyForTest(ImmutableSet.of("cosn://my-bucket/data/"), ImmutableSet.of());
    Assertions.assertTrue(
        json.contains("\"data/*\""),
        "cos:prefix pattern must be 'data/*' when input is 'data/'. Policy: " + json);
    Assertions.assertFalse(
        json.contains("data//*"), "Double slash in prefix pattern. Policy: " + json);
  }
}
