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
package org.apache.gravitino.oss.credential;

import com.aliyun.credentials.Client;
import com.aliyun.credentials.models.Config;
import com.aliyun.credentials.models.CredentialModel;
import com.aliyun.credentials.utils.AuthConstant;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialContext;
import org.apache.gravitino.credential.CredentialProvider;
import org.apache.gravitino.credential.OSSTokenCredential;
import org.apache.gravitino.credential.PathBasedCredentialContext;
import org.apache.gravitino.credential.config.OSSCredentialConfig;
import org.apache.gravitino.oss.credential.policy.Condition;
import org.apache.gravitino.oss.credential.policy.Effect;
import org.apache.gravitino.oss.credential.policy.Policy;
import org.apache.gravitino.oss.credential.policy.Statement;
import org.apache.gravitino.oss.credential.policy.StringLike;

/** Generates OSS token to access OSS data. */
public class OSSTokenProvider implements CredentialProvider {
  private final ObjectMapper objectMapper = new ObjectMapper();
  private String accessKeyId;
  private String secretAccessKey;
  private String roleArn;
  private String externalID;
  private int tokenExpireSecs;
  private String region;

  /**
   * Initializes the credential provider with catalog properties.
   *
   * @param properties catalog properties that can be used to configure the provider. The specific
   *     properties required vary by implementation.
   */
  @Override
  public void initialize(Map<String, String> properties) {
    OSSCredentialConfig credentialConfig = new OSSCredentialConfig(properties);
    this.roleArn = credentialConfig.ossRoleArn();
    this.externalID = credentialConfig.externalID();
    this.tokenExpireSecs = credentialConfig.tokenExpireInSecs();
    this.accessKeyId = credentialConfig.accessKeyID();
    this.secretAccessKey = credentialConfig.secretAccessKey();
    this.region = credentialConfig.region();
  }

  /**
   * Returns the type of credential, it should be identical in Gravitino.
   *
   * @return A string identifying the type of credentials.
   */
  @Override
  public String credentialType() {
    return OSSTokenCredential.OSS_TOKEN_CREDENTIAL_TYPE;
  }

  /**
   * Obtains a credential based on the provided context information.
   *
   * @param context A context object providing necessary information for retrieving credentials.
   * @return A Credential object containing the authentication information needed to access a system
   *     or resource. Null will be returned if no credential is available.
   */
  @Nullable
  @Override
  public Credential getCredential(CredentialContext context) {
    if (!(context instanceof PathBasedCredentialContext)) {
      return null;
    }
    PathBasedCredentialContext pathBasedCredentialContext = (PathBasedCredentialContext) context;
    CredentialModel credentialModel =
        createOSSCredentialModel(
            roleArn,
            pathBasedCredentialContext.getReadPaths(),
            pathBasedCredentialContext.getWritePaths(),
            pathBasedCredentialContext.getUserName());
    return new OSSTokenCredential(
        credentialModel.accessKeyId,
        credentialModel.accessKeySecret,
        credentialModel.securityToken,
        credentialModel.expiration);
  }

  private CredentialModel createOSSCredentialModel(
      String roleArn, Set<String> readLocations, Set<String> writeLocations, String userName) {
    Config config = new Config();
    config.setAccessKeyId(accessKeyId);
    config.setAccessKeySecret(secretAccessKey);
    config.setType(AuthConstant.RAM_ROLE_ARN);
    config.setRoleArn(roleArn);
    config.setRoleSessionName(getRoleName(userName));
    if (StringUtils.isNotBlank(externalID)) {
      config.setExternalId(externalID);
    }
    config.setRoleSessionExpiration(tokenExpireSecs);
    config.setPolicy(createPolicy(readLocations, writeLocations));
    // Local object and client is a simple proxy that does not require manual release
    Client client = new Client(config);
    return client.getCredential();
  }

  // reference:
  // https://www.alibabacloud.com/help/en/oss/user-guide/tutorial-use-ram-policies-to-control-access-to-oss?spm=a2c63.p38356.help-menu-31815.d_2_4_5_1.5536471b56XPRQ
  private String createPolicy(Set<String> readLocations, Set<String> writeLocations) {
    Policy.Builder policyBuilder = Policy.builder().version("1");

    // Allow read and write access to the specified locations
    Statement.Builder allowGetObjectStatementBuilder =
        Statement.builder()
            .effect(Effect.ALLOW)
            .addAction("oss:GetObject")
            .addAction("oss:GetObjectVersion");
    // Add support for bucket-level policies
    Map<String, Statement.Builder> bucketListStatementBuilder = new HashMap<>();
    Map<String, Statement.Builder> bucketGetLocationStatementBuilder = new HashMap<>();

    String arnPrefix = getArnPrefix();
    Stream.concat(readLocations.stream(), writeLocations.stream())
        .distinct()
        .forEach(
            location -> {
              URI uri = URI.create(location);
              allowGetObjectStatementBuilder.addResource(getOssUriWithArn(arnPrefix, uri));
              String bucketArn = arnPrefix + getBucketName(uri);
              // ListBucket
              bucketListStatementBuilder.computeIfAbsent(
                  bucketArn,
                  key ->
                      Statement.builder()
                          .effect(Effect.ALLOW)
                          .addAction("oss:ListBucket")
                          .addResource(key)
                          .condition(getCondition(uri)));
              // GetBucketLocation
              bucketGetLocationStatementBuilder.computeIfAbsent(
                  bucketArn,
                  key ->
                      Statement.builder()
                          .effect(Effect.ALLOW)
                          .addAction("oss:GetBucketLocation")
                          .addResource(key));
            });

    if (!writeLocations.isEmpty()) {
      Statement.Builder allowPutObjectStatementBuilder =
          Statement.builder()
              .effect(Effect.ALLOW)
              .addAction("oss:PutObject")
              .addAction("oss:DeleteObject");
      writeLocations.forEach(
          location -> {
            URI uri = URI.create(location);
            allowPutObjectStatementBuilder.addResource(getOssUriWithArn(arnPrefix, uri));
          });
      policyBuilder.addStatement(allowPutObjectStatementBuilder.build());
    }

    if (!bucketListStatementBuilder.isEmpty()) {
      bucketListStatementBuilder
          .values()
          .forEach(statementBuilder -> policyBuilder.addStatement(statementBuilder.build()));
    } else {
      // add list privilege with 0 resources
      policyBuilder.addStatement(
          Statement.builder().effect(Effect.ALLOW).addAction("oss:ListBucket").build());
    }
    bucketGetLocationStatementBuilder
        .values()
        .forEach(statementBuilder -> policyBuilder.addStatement(statementBuilder.build()));

    policyBuilder.addStatement(allowGetObjectStatementBuilder.build());
    try {
      return objectMapper.writeValueAsString(policyBuilder.build());
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private Condition getCondition(URI uri) {
    return Condition.builder()
        .stringLike(
            StringLike.builder()
                .addPrefix(concatPathWithSep(trimLeadingSlash(uri.getPath()), "*", "/"))
                .build())
        .build();
  }

  private String getArnPrefix() {
    if (StringUtils.isNotEmpty(region)) {
      return "acs:oss:" + region + ":*:";
    }
    return "acs:oss:*:*:";
  }

  private String getBucketName(URI uri) {
    return uri.getHost();
  }

  private String getOssUriWithArn(String arnPrefix, URI uri) {
    return arnPrefix + concatPathWithSep(removeSchemaFromOSSUri(uri), "*", "/");
  }

  private static String concatPathWithSep(String leftPath, String rightPath, String fileSep) {
    if (leftPath.endsWith(fileSep) && rightPath.startsWith(fileSep)) {
      return leftPath + rightPath.substring(1);
    } else if (!leftPath.endsWith(fileSep) && !rightPath.startsWith(fileSep)) {
      return leftPath + fileSep + rightPath;
    } else {
      return leftPath + rightPath;
    }
  }

  // Transform 'oss://bucket/path' to /bucket/path
  private String removeSchemaFromOSSUri(URI uri) {
    String bucket = uri.getHost();
    String path = trimLeadingSlash(uri.getPath());
    return String.join(
        "/", Stream.of(bucket, path).filter(Objects::nonNull).toArray(String[]::new));
  }

  private String trimLeadingSlash(String path) {
    return path.startsWith("/") ? path.substring(1) : path;
  }

  private String getRoleName(String userName) {
    return "gravitino_" + userName;
  }

  @Override
  public void close() throws IOException {}
}
