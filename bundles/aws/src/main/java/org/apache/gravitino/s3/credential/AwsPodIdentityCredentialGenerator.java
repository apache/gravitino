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
package org.apache.gravitino.s3.credential;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.credential.AwsPodIdentityCredential;
import org.apache.gravitino.credential.CredentialContext;
import org.apache.gravitino.credential.CredentialGenerator;
import org.apache.gravitino.credential.PathBasedCredentialContext;
import org.apache.gravitino.credential.config.S3CredentialConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.policybuilder.iam.IamConditionOperator;
import software.amazon.awssdk.policybuilder.iam.IamEffect;
import software.amazon.awssdk.policybuilder.iam.IamPolicy;
import software.amazon.awssdk.policybuilder.iam.IamResource;
import software.amazon.awssdk.policybuilder.iam.IamStatement;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

/** Generate AWS EKS Pod Identity credentials according to the read and write paths. */
public class AwsPodIdentityCredentialGenerator
    implements CredentialGenerator<AwsPodIdentityCredential> {

  private ContainerCredentialsProvider baseCredentialsProvider;
  private String roleArn;
  private int tokenExpireSecs;
  private String region;
  private String stsEndpoint;
  private String externalID;

  @Override
  public void initialize(Map<String, String> properties) {
    // Use ContainerCredentialsProvider for base Pod Identity configuration
    this.baseCredentialsProvider = ContainerCredentialsProvider.builder().build();

    S3CredentialConfig s3CredentialConfig = new S3CredentialConfig(properties);
    this.roleArn = s3CredentialConfig.s3RoleArn();
    this.tokenExpireSecs = s3CredentialConfig.tokenExpireInSecs();
    this.region = s3CredentialConfig.region();
    this.stsEndpoint = s3CredentialConfig.stsEndpoint();
    this.externalID = s3CredentialConfig.externalID();
  }

  @Override
  public AwsPodIdentityCredential generate(CredentialContext context) {
    if (!(context instanceof PathBasedCredentialContext)) {
      // Fallback to basic mode for non-path-based contexts
      AwsCredentials creds = baseCredentialsProvider.resolveCredentials();
      if (creds instanceof AwsSessionCredentials) {
        AwsSessionCredentials sessionCreds = (AwsSessionCredentials) creds;
        long expiration =
            sessionCreds.expirationTime().isPresent()
                ? sessionCreds.expirationTime().get().toEpochMilli()
                : 0L;
        return new AwsPodIdentityCredential(
            sessionCreds.accessKeyId(),
            sessionCreds.secretAccessKey(),
            sessionCreds.sessionToken(),
            expiration);
      } else {
        throw new IllegalStateException(
            "AWS Pod Identity credentials must be of type AwsSessionCredentials. "
                + "Check your EKS Pod Identity configuration. Got: "
                + creds.getClass().getName());
      }
    }

    PathBasedCredentialContext pathBasedCredentialContext = (PathBasedCredentialContext) context;

    Credentials s3Token =
        createCredentialsWithSessionPolicy(
            pathBasedCredentialContext.getReadPaths(),
            pathBasedCredentialContext.getWritePaths(),
            pathBasedCredentialContext.getUserName());
    return new AwsPodIdentityCredential(
        s3Token.accessKeyId(),
        s3Token.secretAccessKey(),
        s3Token.sessionToken(),
        s3Token.expiration().toEpochMilli());
  }

  private Credentials createCredentialsWithSessionPolicy(
      Set<String> readLocations, Set<String> writeLocations, String userName) {
    validateInputParameters(readLocations, writeLocations, userName);

    IamPolicy sessionPolicy = createSessionPolicy(readLocations, writeLocations, region);
    String effectiveRoleArn = getValidatedRoleArn(roleArn);

    try {
      return assumeRoleWithSessionPolicy(effectiveRoleArn, userName, sessionPolicy);
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to create credentials with session policy for user: " + userName, e);
    }
  }

  private IamPolicy createSessionPolicy(
      Set<String> readLocations, Set<String> writeLocations, String region) {
    IamPolicy.Builder policyBuilder = IamPolicy.builder();
    String arnPrefix = getArnPrefix(region);

    addReadPermissions(policyBuilder, readLocations, writeLocations, arnPrefix);
    if (!writeLocations.isEmpty()) {
      addWritePermissions(policyBuilder, writeLocations, arnPrefix);
    }
    addBucketPermissions(policyBuilder, readLocations, writeLocations, arnPrefix);

    return policyBuilder.build();
  }

  private void addReadPermissions(
      IamPolicy.Builder policyBuilder,
      Set<String> readLocations,
      Set<String> writeLocations,
      String arnPrefix) {
    IamStatement.Builder allowGetObjectStatementBuilder =
        IamStatement.builder()
            .effect(IamEffect.ALLOW)
            .addAction("s3:GetObject")
            .addAction("s3:GetObjectVersion");

    Stream.concat(readLocations.stream(), writeLocations.stream())
        .distinct()
        .forEach(
            location -> {
              URI uri = URI.create(location);
              allowGetObjectStatementBuilder.addResource(
                  IamResource.create(getS3UriWithArn(arnPrefix, uri)));
            });

    policyBuilder.addStatement(allowGetObjectStatementBuilder.build());
  }

  private void addWritePermissions(
      IamPolicy.Builder policyBuilder, Set<String> writeLocations, String arnPrefix) {
    IamStatement.Builder allowPutObjectStatementBuilder =
        IamStatement.builder()
            .effect(IamEffect.ALLOW)
            .addAction("s3:PutObject")
            .addAction("s3:DeleteObject");

    writeLocations.forEach(
        location -> {
          URI uri = URI.create(location);
          allowPutObjectStatementBuilder.addResource(
              IamResource.create(getS3UriWithArn(arnPrefix, uri)));
        });

    policyBuilder.addStatement(allowPutObjectStatementBuilder.build());
  }

  private void addBucketPermissions(
      IamPolicy.Builder policyBuilder,
      Set<String> readLocations,
      Set<String> writeLocations,
      String arnPrefix) {
    Map<String, IamStatement.Builder> bucketListStatementBuilder = new HashMap<>();
    Map<String, IamStatement.Builder> bucketGetLocationStatementBuilder = new HashMap<>();

    Stream.concat(readLocations.stream(), writeLocations.stream())
        .distinct()
        .forEach(
            location -> {
              URI uri = URI.create(location);
              String bucketArn = arnPrefix + getBucketName(uri);
              String rawPath = trimLeadingSlash(uri.getPath());

              bucketListStatementBuilder
                  .computeIfAbsent(
                      bucketArn,
                      key ->
                          IamStatement.builder()
                              .effect(IamEffect.ALLOW)
                              .addAction("s3:ListBucket")
                              .addResource(key))
                  .addConditions(
                      IamConditionOperator.STRING_LIKE,
                      "s3:prefix",
                      Arrays.asList(rawPath, addWildcardToPath(rawPath)));

              bucketGetLocationStatementBuilder.computeIfAbsent(
                  bucketArn,
                  key ->
                      IamStatement.builder()
                          .effect(IamEffect.ALLOW)
                          .addAction("s3:GetBucketLocation")
                          .addResource(key));
            });

    addStatementsToPolicy(policyBuilder, bucketListStatementBuilder);
    addStatementsToPolicy(policyBuilder, bucketGetLocationStatementBuilder);
  }

  private void addStatementsToPolicy(
      IamPolicy.Builder policyBuilder, Map<String, IamStatement.Builder> statementBuilders) {
    statementBuilders.values().forEach(builder -> policyBuilder.addStatement(builder.build()));
  }

  private String getS3UriWithArn(String arnPrefix, URI uri) {
    return arnPrefix + addWildcardToPath(removeSchemaFromS3Uri(uri));
  }

  private String getArnPrefix(String region) {
    if (StringUtils.isNotBlank(region)) {
      if (region.contains("cn-")) {
        return "arn:aws-cn:s3:::";
      } else if (region.contains("us-gov-")) {
        return "arn:aws-us-gov:s3:::";
      }
    }
    return "arn:aws:s3:::";
  }

  private static String addWildcardToPath(String path) {
    return path.endsWith("/") ? path + "*" : path + "/*";
  }

  private static String removeSchemaFromS3Uri(URI uri) {
    String bucket = uri.getHost();
    String path = trimLeadingSlash(uri.getPath());
    return String.join(
        "/", Stream.of(bucket, path).filter(Objects::nonNull).toArray(String[]::new));
  }

  private static String trimLeadingSlash(String path) {
    return path.startsWith("/") ? path.substring(1) : path;
  }

  private static String getBucketName(URI uri) {
    return uri.getHost();
  }

  private void validateInputParameters(
      Set<String> readLocations, Set<String> writeLocations, String userName) {
    if (StringUtils.isBlank(userName)) {
      throw new IllegalArgumentException("userName cannot be null or empty");
    }
    if ((readLocations == null || readLocations.isEmpty())
        && (writeLocations == null || writeLocations.isEmpty())) {
      throw new IllegalArgumentException("At least one read or write location must be specified");
    }
  }

  private String getValidatedRoleArn(String configRoleArn) {
    if (StringUtils.isBlank(configRoleArn)) {
      throw new IllegalStateException(
          "No role ARN available. Please configure s3-role-arn for fine-grained access control.");
    }
    if (!configRoleArn.startsWith("arn:aws")) {
      throw new IllegalArgumentException("Invalid role ARN format: " + configRoleArn);
    }
    return configRoleArn;
  }

  private Credentials assumeRoleWithSessionPolicy(
      String roleArn, String userName, IamPolicy sessionPolicy) {
    StsClientBuilder stsBuilder =
        StsClient.builder().credentialsProvider(this.baseCredentialsProvider);
    if (StringUtils.isNotBlank(region)) {
      stsBuilder.region(Region.of(region));
    }
    if (StringUtils.isNotBlank(stsEndpoint)) {
      stsBuilder.endpointOverride(URI.create(stsEndpoint));
    }

    try (StsClient stsClient = stsBuilder.build()) {
      AssumeRoleRequest.Builder requestBuilder =
          AssumeRoleRequest.builder()
              .roleArn(roleArn)
              .roleSessionName("gravitino_pod_identity_session_" + userName)
              .durationSeconds(tokenExpireSecs)
              .policy(sessionPolicy.toJson());

      if (StringUtils.isNotBlank(externalID)) {
        requestBuilder.externalId(externalID);
      }

      AssumeRoleResponse response = stsClient.assumeRole(requestBuilder.build());
      return response.credentials();
    }
  }

  @Override
  public void close() throws IOException {
    if (baseCredentialsProvider != null) {
      baseCredentialsProvider.close();
    }
  }
}
