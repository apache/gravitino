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
import org.apache.gravitino.cos.credential.policy.Condition;
import org.apache.gravitino.cos.credential.policy.Effect;
import org.apache.gravitino.cos.credential.policy.Policy;
import org.apache.gravitino.cos.credential.policy.Statement;
import org.apache.gravitino.cos.credential.policy.StringLike;
import org.apache.gravitino.credential.COSTokenCredential;
import org.apache.gravitino.credential.CredentialContext;
import org.apache.gravitino.credential.CredentialGenerator;
import org.apache.gravitino.credential.PathBasedCredentialContext;
import org.apache.gravitino.credential.config.COSCredentialConfig;

/**
 * Generates Tencent Cloud COS STS tokens scoped to the requested fileset paths.
 *
 * <p>The generator calls Tencent Cloud {@code sts:AssumeRole} with a session policy that allows
 * only the read/write paths reported by the {@link PathBasedCredentialContext}, so the temporary
 * credentials handed back to clients cannot reach data outside the requested locations.
 */
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
    // Tencent Cloud returns ExpiredTime in Unix seconds, convert to ms to match the
    // Credential#expireTimeInMs() contract.
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
    // COSCredentialConfig enforces cos-region to be non-blank, so we can pass it directly to
    // the STS client to sign requests against the correct regional endpoint.
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

    // Use LinkedHashMap so the resulting policy JSON has a deterministic statement order; this
    // makes logs easier to diff and avoids spurious cache key churn.
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
              // Tencent Cloud CAM requires distinct resource ARN forms for cos:GetBucket vs
              // the bucket-metadata actions: cos:GetBucket needs the wildcard form (bucket/*),
              // whereas cos:HeadBucket / cos:GetBucketLocation need the plain form (bucket/).
              // The cos:prefix condition uses "xxx*" (no slash) so it matches both the fileset
              // root prefix (e.g. "xxx/") and any sub-path (e.g. "xxx/foo/") that hadoop-cos
              // may pass in a list request.
              bucketListStatements.computeIfAbsent(
                  bucketWildcardResource,
                  key ->
                      Statement.builder()
                          .effect(Effect.ALLOW)
                          .addAction("cos:GetBucket")
                          .addResource(key)
                          .condition(buildPrefixCondition(uri)));
              // hadoop-cos calls headBucket during FileSystem.initialize(); cos:HeadBucket must
              // be granted here or the temporary credentials will fail with 403 Forbidden.
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
   * Build the {@code cos:prefix} condition for a fileset location.
   *
   * <p>hadoop-cos may pass the fileset prefix itself (e.g. {@code xxx/}) or the prefix plus a
   * sub-path (e.g. {@code xxx/foo/}) as the {@code prefix} of a list request. Tencent Cloud CAM's
   * {@code string_like} matcher requires the pattern to be {@code xxx*} (no trailing slash) to
   * cover both forms; the alternative {@code xxx/*} fails to match {@code xxx/} and returns
   * AccessDenied. The pattern is still anchored to the fileset prefix, so it cannot broaden the
   * scope of the token.
   */
  private Condition buildPrefixCondition(URI uri) {
    return Condition.builder()
        .stringLike(StringLike.builder().addPrefix(trimLeadingSlash(uri.getPath()) + "*").build())
        .build();
  }

  /**
   * Build the bucket-level resource ARN for COS, e.g. {@code
   * qcs::cos:ap-shanghai:uid/1259xxx:my-bucket-1259xxx/}.
   *
   * <p>Used for bucket-metadata actions such as {@code cos:HeadBucket} and {@code
   * cos:GetBucketLocation}. For {@code cos:GetBucket} (list objects), use {@link
   * #getBucketWildcardResource(URI)} instead — Tencent Cloud CAM requires the wildcard form.
   */
  private String getBucketResource(URI uri) {
    return getResourcePrefix() + getBucketWithAppId(uri) + "/";
  }

  /**
   * Build the bucket-level resource ARN with a trailing wildcard, e.g. {@code
   * qcs::cos:ap-shanghai:uid/1259xxx:my-bucket-1259xxx/*}.
   *
   * <p>Tencent Cloud CAM requires this wildcard form for {@code cos:GetBucket} (list objects); the
   * plain {@code bucket/} form yields AccessDenied even with a matching {@code cos:prefix}
   * condition.
   */
  private String getBucketWildcardResource(URI uri) {
    return getResourcePrefix() + getBucketWithAppId(uri) + "/*";
  }

  /**
   * Add the object-level resource ARNs for a fileset location to the given object statement.
   *
   * <p>Two ARNs are added:
   *
   * <ul>
   *   <li>{@code bucket/prefix} — matches the fileset key itself (used e.g. by hadoop-cos when it
   *       issues {@code HeadObject} on the fileset root key without any sub-path).
   *   <li>{@code bucket/prefix/*} — matches every object beneath the prefix.
   * </ul>
   *
   * <p>Tencent Cloud CAM treats {@code prefix} and {@code prefix/*} as distinct resources, so both
   * ARNs are needed to cover the {@code HeadObject} on the fileset root as well as reads/writes on
   * child objects.
   */
  private void addObjectResources(Statement.Builder statement, URI uri) {
    String path = trimLeadingSlash(uri.getPath());
    String bucketPrefix = getResourcePrefix() + getBucketWithAppId(uri) + "/";
    String fullPrefix = bucketPrefix + path;
    // Strip the trailing slash so the prefix ARN matches the exact key. Tencent Cloud treats
    // "xxx" and "xxx/" as different object keys; hadoop-cos may issue HEAD against either form.
    String prefixArn =
        fullPrefix.endsWith("/") ? fullPrefix.substring(0, fullPrefix.length() - 1) : fullPrefix;
    statement.addResource(prefixArn);
    statement.addResource(appendWildcard(fullPrefix));
  }

  private String getResourcePrefix() {
    // A non-blank cos-region is enforced at config load time, so the region segment of the
    // resource ARN cannot degrade to the wildcard "*".
    return "qcs::cos:" + region + ":uid/" + appId + ":";
  }

  /**
   * Returns the bucket name including the {@code -<APPID>} suffix that Tencent Cloud expects in
   * resource ARNs. If the bucket in the URI already ends with {@code -<APPID>} we preserve it as
   * is; otherwise append the suffix.
   */
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

  /**
   * Append the COS wildcard suffix {@code /*} to a path, collapsing duplicate slashes so that
   * {@code "data/"} and {@code "data"} both yield {@code "data/*"}.
   */
  private static String appendWildcard(String leftPath) {
    return leftPath.endsWith("/") ? leftPath + "*" : leftPath + "/*";
  }

  private String getRoleSessionName(String userName) {
    String safe = userName == null ? "anonymous" : userName.replaceAll("[^A-Za-z0-9_=.@\\-]", "_");
    // Tencent Cloud limits the role session name to 64 characters.
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
    // Tencent Cloud StsClient does not require explicit cleanup.
  }
}
