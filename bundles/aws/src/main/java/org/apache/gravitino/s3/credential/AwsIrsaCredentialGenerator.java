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

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.credential.AwsIrsaCredential;
import org.apache.gravitino.credential.CredentialContext;
import org.apache.gravitino.credential.CredentialGenerator;
import org.apache.gravitino.credential.PathBasedCredentialContext;
import org.apache.gravitino.credential.config.S3CredentialConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.policybuilder.iam.IamConditionOperator;
import software.amazon.awssdk.policybuilder.iam.IamEffect;
import software.amazon.awssdk.policybuilder.iam.IamPolicy;
import software.amazon.awssdk.policybuilder.iam.IamResource;
import software.amazon.awssdk.policybuilder.iam.IamStatement;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

/** Generate AWS IRSA credentials according to the read and write paths. */
public class AwsIrsaCredentialGenerator implements CredentialGenerator<AwsIrsaCredential> {

  @Override
  public AwsIrsaCredential generate(Map<String, String> properties, CredentialContext context) {
    S3CredentialConfig s3CredentialConfig = new S3CredentialConfig(properties);

    if (!(context instanceof PathBasedCredentialContext)) {
      // Fallback to original behavior for non-path-based contexts
      try (WebIdentityTokenFileCredentialsProvider baseCredentialsProvider =
          WebIdentityTokenFileCredentialsProvider.create()) {
        AwsCredentials creds = baseCredentialsProvider.resolveCredentials();
        if (creds instanceof AwsSessionCredentials) {
          AwsSessionCredentials sessionCreds = (AwsSessionCredentials) creds;
          long expiration =
              sessionCreds.expirationTime().isPresent()
                  ? sessionCreds.expirationTime().get().toEpochMilli()
                  : 0L;
          return new AwsIrsaCredential(
              sessionCreds.accessKeyId(),
              sessionCreds.secretAccessKey(),
              sessionCreds.sessionToken(),
              expiration);
        } else {
          throw new IllegalStateException(
              "AWS IRSA credentials must be of type AwsSessionCredentials. "
                  + "Check your EKS/IRSA configuration. Got: "
                  + creds.getClass().getName());
        }
      }
    }

    PathBasedCredentialContext pathBasedCredentialContext = (PathBasedCredentialContext) context;

    Credentials s3Token =
        createCredentialsWithSessionPolicy(
            pathBasedCredentialContext.getReadPaths(),
            pathBasedCredentialContext.getWritePaths(),
            pathBasedCredentialContext.getUserName(),
            s3CredentialConfig);
    return new AwsIrsaCredential(
        s3Token.accessKeyId(),
        s3Token.secretAccessKey(),
        s3Token.sessionToken(),
        s3Token.expiration().toEpochMilli());
  }

  private Credentials createCredentialsWithSessionPolicy(
      Set<String> readLocations,
      Set<String> writeLocations,
      String userName,
      S3CredentialConfig config) {
    validateInputParameters(readLocations, writeLocations, userName);

    IamPolicy sessionPolicy = createSessionPolicy(readLocations, writeLocations, config.region());
    String webIdentityTokenFile = getValidatedWebIdentityTokenFile();
    String effectiveRoleArn = getValidatedRoleArn(config.s3RoleArn());

    try {
      String tokenContent =
          new String(Files.readAllBytes(Paths.get(webIdentityTokenFile)), StandardCharsets.UTF_8);
      if (StringUtils.isBlank(tokenContent)) {
        throw new IllegalStateException(
            "Web identity token file is empty: " + webIdentityTokenFile);
      }

      return assumeRoleWithSessionPolicy(
          effectiveRoleArn, userName, tokenContent, sessionPolicy, config);
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

  private String getValidatedWebIdentityTokenFile() {
    String webIdentityTokenFile = System.getenv("AWS_WEB_IDENTITY_TOKEN_FILE");
    if (StringUtils.isBlank(webIdentityTokenFile)) {
      throw new IllegalStateException(
          "AWS_WEB_IDENTITY_TOKEN_FILE environment variable is not set. "
              + "Ensure IRSA is properly configured in your EKS cluster.");
    }
    if (!Files.exists(Paths.get(webIdentityTokenFile))) {
      throw new IllegalStateException(
          "Web identity token file does not exist: " + webIdentityTokenFile);
    }
    return webIdentityTokenFile;
  }

  private String getValidatedRoleArn(String configRoleArn) {
    String effectiveRoleArn =
        StringUtils.isNotBlank(configRoleArn) ? configRoleArn : System.getenv("AWS_ROLE_ARN");
    if (StringUtils.isBlank(effectiveRoleArn)) {
      throw new IllegalStateException(
          "No role ARN available. Either configure s3-role-arn or ensure AWS_ROLE_ARN environment variable is set.");
    }
    if (!effectiveRoleArn.startsWith("arn:aws")) {
      throw new IllegalArgumentException("Invalid role ARN format: " + effectiveRoleArn);
    }
    return effectiveRoleArn;
  }

  private Credentials assumeRoleWithSessionPolicy(
      String roleArn,
      String userName,
      String webIdentityToken,
      IamPolicy sessionPolicy,
      S3CredentialConfig config) {
    StsClientBuilder stsBuilder = StsClient.builder();
    if (StringUtils.isNotBlank(config.region())) {
      stsBuilder.region(Region.of(config.region()));
    }
    if (StringUtils.isNotBlank(config.stsEndpoint())) {
      stsBuilder.endpointOverride(URI.create(config.stsEndpoint()));
    }

    try (StsClient stsClient = stsBuilder.build()) {
      AssumeRoleWithWebIdentityRequest request =
          AssumeRoleWithWebIdentityRequest.builder()
              .roleArn(roleArn)
              .roleSessionName("gravitino_irsa_session_" + userName)
              .durationSeconds(config.tokenExpireInSecs())
              .webIdentityToken(webIdentityToken)
              .policy(sessionPolicy.toJson())
              .build();

      AssumeRoleWithWebIdentityResponse response = stsClient.assumeRoleWithWebIdentity(request);
      return response.credentials();
    }
  }
}
