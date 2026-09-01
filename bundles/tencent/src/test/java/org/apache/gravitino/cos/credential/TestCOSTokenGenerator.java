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
    Assertions.assertTrue(json.contains("\"data/\""), "Policy: " + json);
    Assertions.assertTrue(json.contains("\"data/*\""), "Policy: " + json);
  }

  @Test
  void testBucketWithExistingAppIdSuffixIsKept() throws Exception {
    COSTokenGenerator generator = newGenerator();
    String json =
        generator.buildPolicyForTest(
            ImmutableSet.of("cosn://already-1259000000/data/"), ImmutableSet.of());
    Assertions.assertTrue(json.contains("already-1259000000"), "Policy: " + json);
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
    // A location without a trailing slash is normalized to end with '/' before the cos:prefix
    // patterns are built; a bare 'data*' pattern MUST NOT be emitted, otherwise sibling
    // prefixes such as 'data_backup/' would be accidentally covered.
    COSTokenGenerator generator = newGenerator();
    String json =
        generator.buildPolicyForTest(ImmutableSet.of("cosn://my-bucket/data"), ImmutableSet.of());
    Assertions.assertTrue(json.contains("data/*"), "Policy: " + json);
    Assertions.assertTrue(json.contains("\"data/\""), "Policy: " + json);
    Assertions.assertTrue(json.contains("\"data/*\""), "Policy: " + json);
    Assertions.assertFalse(json.contains("\"data*\""), "Policy: " + json);
  }

  @Test
  void testInitializeForTestRejectsBlankRegion() {
    // A blank region degrades the resource ARN region segment to '*', weakening the policy.
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
    COSTokenGenerator generator = newGenerator();
    String json =
        generator.buildPolicyForTest(ImmutableSet.of("cosn://my-bucket/data/"), ImmutableSet.of());
    Assertions.assertTrue(json.contains("qcs::cos:ap-shanghai:"), "Policy: " + json);
    Assertions.assertFalse(json.contains("qcs::cos:*:"), "Policy: " + json);
  }

  @Test
  void testPolicyIncludesHeadBucketAction() throws Exception {
    // hadoop-cos calls headBucket during FileSystem.initialize(); without cos:HeadBucket the
    // vended credentials return 403.
    COSTokenGenerator generator = newGenerator();
    String json =
        generator.buildPolicyForTest(ImmutableSet.of("cosn://my-bucket/data/"), ImmutableSet.of());
    Assertions.assertTrue(json.contains("cos:HeadBucket"), "Policy: " + json);
  }

  @Test
  void testGetBucketUsesWildcardResource() throws Exception {
    // CAM needs bucket/* for cos:GetBucket and bucket/ for cos:HeadBucket / cos:GetBucketLocation.
    COSTokenGenerator generator = newGenerator();
    String json =
        generator.buildPolicyForTest(ImmutableSet.of("cosn://my-bucket/data/"), ImmutableSet.of());
    Assertions.assertTrue(json.contains(":my-bucket-1259000000/*"), "Policy: " + json);
    Assertions.assertTrue(json.contains(":my-bucket-1259000000/\""), "Policy: " + json);
  }

  @Test
  void testPrefixConditionKeepsTrailingSlashBoundary() throws Exception {
    // Guard against dropping the '/' before '*' (would over-grant siblings) or a double
    // slash ('data//*') from double-appending the wildcard.
    COSTokenGenerator generator = newGenerator();
    String json =
        generator.buildPolicyForTest(ImmutableSet.of("cosn://my-bucket/data/"), ImmutableSet.of());
    Assertions.assertTrue(json.contains("\"data/*\""), "Policy: " + json);
    Assertions.assertFalse(json.contains("data//*"), "Policy: " + json);
    Assertions.assertFalse(json.contains("\"data*\""), "Policy: " + json);
  }

  @Test
  void testPrefixConditionDoesNotOverGrantSiblingPaths() throws Exception {
    // Regression guard: 'orders' and 'orders/' must both normalize to 'orders/' + 'orders/*',
    // never leaking to sibling prefixes like 'orders_backup/'.
    COSTokenGenerator generator = newGenerator();
    for (String location : new String[] {"cosn://my-bucket/orders", "cosn://my-bucket/orders/"}) {
      String json = generator.buildPolicyForTest(ImmutableSet.of(location), ImmutableSet.of());
      Assertions.assertTrue(json.contains("\"orders/\""), location + " Policy: " + json);
      Assertions.assertTrue(json.contains("\"orders/*\""), location + " Policy: " + json);
      Assertions.assertFalse(json.contains("\"orders*\""), location + " Policy: " + json);
    }
  }

  @Test
  void testPrefixConditionForBucketRootLocation() throws Exception {
    // Bucket-root fileset: cos:prefix "/" would never match real COS keys (they carry no
    // leading slash), so both cosn://bucket/ and cosn://bucket must emit a bare "*".
    COSTokenGenerator generator = newGenerator();
    for (String location : new String[] {"cosn://my-bucket/", "cosn://my-bucket"}) {
      String json = generator.buildPolicyForTest(ImmutableSet.of(location), ImmutableSet.of());
      Assertions.assertTrue(json.contains("\"*\""), location + " Policy: " + json);
      Assertions.assertFalse(json.contains("\"/\""), location + " Policy: " + json);
      Assertions.assertFalse(json.contains("\"/*\""), location + " Policy: " + json);
    }
  }

  @Test
  void testWritePolicyIncludesFullMultipartActionSet() throws Exception {
    // hadoop-cos calls ListParts on UploadPart 409 to reconcile part state; missing any of
    // these actions turns a recoverable multipart upload into a hard 403.
    COSTokenGenerator generator = newGenerator();
    String json =
        generator.buildPolicyForTest(ImmutableSet.of(), ImmutableSet.of("cosn://my-bucket/data/"));
    for (String action :
        new String[] {
          "cos:PutObject",
          "cos:DeleteObject",
          "cos:InitiateMultipartUpload",
          "cos:UploadPart",
          "cos:ListParts",
          "cos:CompleteMultipartUpload",
          "cos:AbortMultipartUpload"
        }) {
      Assertions.assertTrue(json.contains(action), "missing " + action + " in policy: " + json);
    }
  }

  @Test
  void testGetBucketConditionCoversAllPrefixesForSameBucket() throws Exception {
    // CredentialOperationDispatcher.mergeContexts collects multiple PathContexts into one
    // PathBasedCredentialContext, so the GetBucket statement must authorise every path.
    COSTokenGenerator generator = newGenerator();
    String json =
        generator.buildPolicyForTest(
            ImmutableSet.of("cosn://my-bucket/path-a/"),
            ImmutableSet.of("cosn://my-bucket/path-b/"));
    Assertions.assertTrue(json.contains("\"path-a/\""), "Policy: " + json);
    Assertions.assertTrue(json.contains("\"path-a/*\""), "Policy: " + json);
    Assertions.assertTrue(json.contains("\"path-b/\""), "Policy: " + json);
    Assertions.assertTrue(json.contains("\"path-b/*\""), "Policy: " + json);
  }
}
