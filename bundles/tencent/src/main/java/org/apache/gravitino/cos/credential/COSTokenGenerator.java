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

package org.apache.gravitino.cos.credential;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.tencentcloudapi.common.Credential;
import com.tencentcloudapi.common.exception.TencentCloudSDKException;
import com.tencentcloudapi.sts.v20180813.StsClient;
import com.tencentcloudapi.sts.v20180813.models.AssumeRoleRequest;
import com.tencentcloudapi.sts.v20180813.models.AssumeRoleResponse;
import java.io.IOException;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.cos.credential.policy.Effect;
import org.apache.gravitino.cos.credential.policy.Policy;
import org.apache.gravitino.cos.credential.policy.Statement;
import org.apache.gravitino.credential.COSTokenCredential;
import org.apache.gravitino.credential.CredentialContext;
import org.apache.gravitino.credential.CredentialGenerator;
import org.apache.gravitino.credential.PathBasedCredentialContext;
import org.apache.gravitino.credential.config.COSCredentialConfig;

/** Generates Tencent Cloud COS STS tokens scoped to the requested fileset paths. */
public class COSTokenGenerator implements CredentialGenerator<COSTokenCredential> {

  private static final String POLICY_VERSION = "2.0";

  private final ObjectMapper objectMapper = new ObjectMapper();

  private String accessKeyId;
  private String secretAccessKey;
  private String roleArn;
  private String externalId;
  private String region;
  private String appId;
  private int tokenExpireSecs;

  @Override
  public void initialize(Map<String, String> properties) {
    COSCredentialConfig config = new COSCredentialConfig(properties);
    this.accessKeyId = config.accessKeyID();
    this.secretAccessKey = config.secretAccessKey();
    this.roleArn = config.cosRoleArn();
    this.externalId = config.externalID();
    this.region = config.region();
    this.appId = config.appID();
    this.tokenExpireSecs = config.tokenExpireInSecs();
  }

  @Override
  public COSTokenCredential generate(CredentialContext context) throws Exception {
    if (!(context instanceof PathBasedCredentialContext)) {
      return null;
    }

    PathBasedCredentialContext pathContext = (PathBasedCredentialContext) context;

    AssumeRoleResponse response =
        callAssumeRole(
            pathContext.getReadPaths(), pathContext.getWritePaths(), pathContext.getUserName());

    com.tencentcloudapi.sts.v20180813.models.Credentials credentials = response.getCredentials();
    Long expiredTime = response.getExpiredTime();
    Preconditions.checkState(
        credentials != null && expiredTime != null,
        "Tencent STS AssumeRole returned an incomplete response, requestId: %s",
        response.getRequestId());
    // Tencent STS returns ExpiredTime in seconds; the Credential contract uses ms.
    long expireTimeInMs = expiredTime * 1000L;
    return new COSTokenCredential(
        credentials.getTmpSecretId(),
        credentials.getTmpSecretKey(),
        credentials.getToken(),
        expireTimeInMs);
  }

  private AssumeRoleResponse callAssumeRole(
      Set<String> readLocations, Set<String> writeLocations, String userName)
      throws TencentCloudSDKException {
    Credential cred = new Credential(accessKeyId, secretAccessKey);
    StsClient client = new StsClient(cred, region);

    AssumeRoleRequest request = new AssumeRoleRequest();
    request.setRoleArn(roleArn);
    request.setRoleSessionName(getRoleSessionName(userName));
    request.setDurationSeconds((long) tokenExpireSecs);
    if (StringUtils.isNotBlank(externalId)) {
      request.setExternalId(externalId);
    }
    request.setPolicy(buildPolicy(readLocations, writeLocations));

    return client.AssumeRole(request);
  }

  private String buildPolicy(Set<String> readLocations, Set<String> writeLocations) {
    Preconditions.checkArgument(
        !readLocations.isEmpty() || !writeLocations.isEmpty(),
        "COS token generator requires at least one read or write location");
    Policy.Builder policyBuilder = Policy.builder().version(POLICY_VERSION);

    Statement.Builder readObjectStatement =
        Statement.builder()
            .effect(Effect.ALLOW)
            .addAction("cos:GetObject")
            .addAction("cos:HeadObject");

    // LinkedHashMap keeps the emitted statements in a deterministic order.
    Map<String, Statement.Builder> bucketListStatements = new LinkedHashMap<>();
    Map<String, Statement.Builder> bucketMetadataStatements = new LinkedHashMap<>();

    Stream.concat(readLocations.stream(), writeLocations.stream())
        .distinct()
        .forEach(
            location -> {
              URI uri = URI.create(location);
              addObjectResources(readObjectStatement, uri);
              String bucketResource = getBucketResource(uri);
              String bucketWildcardResource = getBucketWildcardResource(uri);
              // CAM requires different resource ARN forms per action: cos:GetBucket needs
              // bucket/*, whereas cos:HeadBucket / cos:GetBucketLocation need bucket/.
              Statement.Builder listStatement =
                  bucketListStatements.computeIfAbsent(
                      bucketWildcardResource,
                      key ->
                          Statement.builder()
                              .effect(Effect.ALLOW)
                              .addAction("cos:GetBucket")
                              .addResource(key));
              // CredentialOperationDispatcher merges multiple PathContexts of the same
              // credential type into one PathBasedCredentialContext, so cos:prefix must be
              // accumulated for every URI here — not just the first.
              addPrefixPatterns(listStatement, uri);
              // hadoop-cos calls headBucket during FileSystem.initialize(); without
              // cos:HeadBucket the vended credentials return 403.
              bucketMetadataStatements.computeIfAbsent(
                  bucketResource,
                  key ->
                      Statement.builder()
                          .effect(Effect.ALLOW)
                          .addAction("cos:GetBucketLocation")
                          .addAction("cos:HeadBucket")
                          .addResource(key));
            });

    if (!writeLocations.isEmpty()) {
      Statement.Builder writeObjectStatement =
          Statement.builder()
              .effect(Effect.ALLOW)
              .addAction("cos:PutObject")
              .addAction("cos:DeleteObject")
              .addAction("cos:InitiateMultipartUpload")
              .addAction("cos:UploadPart")
              .addAction("cos:ListParts")
              .addAction("cos:CompleteMultipartUpload")
              .addAction("cos:AbortMultipartUpload");
      writeLocations.forEach(
          location -> addObjectResources(writeObjectStatement, URI.create(location)));
      policyBuilder.addStatement(writeObjectStatement.build());
    }

    if (!bucketListStatements.isEmpty()) {
      bucketListStatements.values().forEach(builder -> policyBuilder.addStatement(builder.build()));
    }
    bucketMetadataStatements
        .values()
        .forEach(builder -> policyBuilder.addStatement(builder.build()));

    policyBuilder.addStatement(readObjectStatement.build());

    try {
      return objectMapper.writeValueAsString(policyBuilder.build());
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize COS session policy", e);
    }
  }

  /**
   * Emits the {@code cos:prefix} condition patterns for the given fileset location. For non-root
   * paths this is {@code xxx/} and {@code xxx/*}; the trailing slash is essential — a bare {@code
   * xxx*} would also match sibling prefixes like {@code xxx_backup/}. For bucket-root filesets
   * ({@code cosn://bucket/} or {@code cosn://bucket}) the path is empty and a bare {@code *} is
   * emitted, since COS object keys have no leading slash.
   */
  private void addPrefixPatterns(Statement.Builder statement, URI uri) {
    String prefix = trimLeadingSlash(uri.getPath());
    if (prefix.isEmpty()) {
      statement.addStringLikePrefix("*");
      return;
    }
    if (!prefix.endsWith("/")) {
      prefix = prefix + "/";
    }
    statement.addStringLikePrefix(prefix);
    statement.addStringLikePrefix(prefix + "*");
  }

  /** Bucket ARN with trailing slash, used for cos:HeadBucket / cos:GetBucketLocation. */
  private String getBucketResource(URI uri) {
    return getResourcePrefix() + getBucketWithAppId(uri) + "/";
  }

  /** Bucket ARN with trailing wildcard, required by CAM for cos:GetBucket. */
  private String getBucketWildcardResource(URI uri) {
    return getResourcePrefix() + getBucketWithAppId(uri) + "/*";
  }

  /**
   * Emits both {@code bucket/prefix} (matches the fileset key itself, e.g. a HEAD on the root) and
   * {@code bucket/prefix/*} (matches everything under it). CAM treats them as distinct resources.
   */
  private void addObjectResources(Statement.Builder statement, URI uri) {
    String path = trimLeadingSlash(uri.getPath());
    String bucketPrefix = getResourcePrefix() + getBucketWithAppId(uri) + "/";
    String fullPrefix = bucketPrefix + path;
    String prefixArn =
        fullPrefix.endsWith("/") ? fullPrefix.substring(0, fullPrefix.length() - 1) : fullPrefix;
    statement.addResource(prefixArn);
    statement.addResource(appendWildcard(fullPrefix));
  }

  private String getResourcePrefix() {
    return "qcs::cos:" + region + ":uid/" + appId + ":";
  }

  /** Bucket ARNs require the {@code -<APPID>} suffix; append it unless already present. */
  private String getBucketWithAppId(URI uri) {
    String bucket = uri.getHost();
    if (bucket == null) {
      throw new IllegalArgumentException("COS location is missing bucket: " + uri);
    }
    String suffix = "-" + appId;
    if (bucket.endsWith(suffix)) {
      return bucket;
    }
    return bucket + suffix;
  }

  private String trimLeadingSlash(String path) {
    if (path == null) {
      return "";
    }
    return path.startsWith("/") ? path.substring(1) : path;
  }

  /** Appends {@code /*} to a path, avoiding double slashes. */
  private static String appendWildcard(String leftPath) {
    return leftPath.endsWith("/") ? leftPath + "*" : leftPath + "/*";
  }

  private String getRoleSessionName(String userName) {
    String safe = userName == null ? "anonymous" : userName.replaceAll("[^A-Za-z0-9_=.@\\-]", "_");
    // Tencent Cloud caps the role session name at 64 characters.
    String name = "gravitino_" + safe;
    return name.length() > 64 ? name.substring(0, 64) : name;
  }

  // Visible for tests.
  String buildPolicyForTest(Set<String> readLocations, Set<String> writeLocations) {
    return buildPolicy(readLocations, writeLocations);
  }

  // Visible for tests.
  void initializeForTest(
      String accessKeyId,
      String secretAccessKey,
      String roleArn,
      String externalId,
      String region,
      String appId,
      int tokenExpireSecs) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(region),
        "COS token generator requires a non-blank region; got '%s'",
        region);
    this.accessKeyId = accessKeyId;
    this.secretAccessKey = secretAccessKey;
    this.roleArn = roleArn;
    this.externalId = externalId;
    this.region = region;
    this.appId = appId;
    this.tokenExpireSecs = tokenExpireSecs;
  }

  @Override
  public void close() throws IOException {
    // StsClient has no resources to release.
  }
}
