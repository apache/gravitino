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
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialContext;
import org.apache.gravitino.credential.CredentialProvider;
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

/**
 * AWS IRSA credential provider that supports both basic IRSA credentials and fine-grained
 * path-based access control.
 *
 * <p>This provider operates in two modes:
 *
 * <ul>
 *   <li><b>Basic IRSA mode</b>: For non-path-based credential contexts, returns credentials with
 *       full permissions of the associated IAM role (backward compatibility)
 *   <li><b>Fine-grained mode</b>: For path-based credential contexts (e.g., table access with
 *       vended credentials), generates temporary credentials with IAM policies scoped to specific
 *       S3 paths including table location, metadata location, and write data locations
 * </ul>
 *
 * <p>The fine-grained mode uses STS AssumeRoleWithWebIdentity to create temporary credentials with
 * inline IAM policies that grant minimal required permissions:
 *
 * <ul>
 *   <li>s3:GetObject, s3:GetObjectVersion for read access to table paths
 *   <li>s3:ListBucket with s3:prefix conditions limiting to table directories
 *   <li>s3:PutObject, s3:DeleteObject for write operations (when write paths are present)
 *   <li>s3:GetBucketLocation for bucket metadata access
 * </ul>
 *
 * <p>Prerequisites for fine-grained mode:
 *
 * <ul>
 *   <li>EKS cluster with IRSA properly configured
 *   <li>AWS_WEB_IDENTITY_TOKEN_FILE environment variable pointing to service account token
 *   <li>IAM role with permissions to assume the target role specified in s3-role-arn
 *   <li>Target IAM role with necessary S3 permissions for data locations
 * </ul>
 */
public class AwsIrsaCredentialProvider implements CredentialProvider {

  private WebIdentityTokenFileCredentialsProvider baseCredentialsProvider;
  private StsClient stsClient;
  private String roleArn;
  private int tokenExpireSecs;

  @Override
  public void initialize(Map<String, String> properties) {
    // Use WebIdentityTokenFileCredentialsProvider for base IRSA configuration
    this.baseCredentialsProvider = WebIdentityTokenFileCredentialsProvider.create();

    S3CredentialConfig s3CredentialConfig = new S3CredentialConfig(properties);
    this.roleArn = s3CredentialConfig.s3RoleArn();
    this.tokenExpireSecs = s3CredentialConfig.tokenExpireInSecs();
    this.stsClient = createStsClient(s3CredentialConfig);
  }

  @Override
  public void close() {
    if (stsClient != null) {
      stsClient.close();
    }
  }

  @Override
  public String credentialType() {
    return AwsIrsaCredential.AWS_IRSA_CREDENTIAL_TYPE;
  }

  @Override
  public Credential getCredential(CredentialContext context) {
    if (!(context instanceof PathBasedCredentialContext)) {
      // Fallback to original behavior for non-path-based contexts
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

    PathBasedCredentialContext pathBasedCredentialContext = (PathBasedCredentialContext) context;
    Credentials s3Token =
        createS3TokenWithWebIdentity(
            roleArn,
            pathBasedCredentialContext.getReadPaths(),
            pathBasedCredentialContext.getWritePaths(),
            pathBasedCredentialContext.getUserName());
    return new AwsIrsaCredential(
        s3Token.accessKeyId(),
        s3Token.secretAccessKey(),
        s3Token.sessionToken(),
        s3Token.expiration().toEpochMilli());
  }

  private StsClient createStsClient(S3CredentialConfig s3CredentialConfig) {
    StsClientBuilder builder = StsClient.builder();
    String region = s3CredentialConfig.region();
    if (StringUtils.isNotBlank(region)) {
      builder.region(Region.of(region));
    }
    String stsEndpoint = s3CredentialConfig.stsEndpoint();
    if (StringUtils.isNotBlank(stsEndpoint)) {
      builder.endpointOverride(URI.create(stsEndpoint));
    }
    return builder.build();
  }

  private IamPolicy createPolicy(
      String roleArn, Set<String> readLocations, Set<String> writeLocations) {
    IamPolicy.Builder policyBuilder = IamPolicy.builder();
    IamStatement.Builder allowGetObjectStatementBuilder =
        IamStatement.builder()
            .effect(IamEffect.ALLOW)
            .addAction("s3:GetObject")
            .addAction("s3:GetObjectVersion");
    Map<String, IamStatement.Builder> bucketListStatmentBuilder = new HashMap<>();
    Map<String, IamStatement.Builder> bucketGetLocationStatmentBuilder = new HashMap<>();

    String arnPrefix = getArnPrefix(roleArn);
    Stream.concat(readLocations.stream(), writeLocations.stream())
        .distinct()
        .forEach(
            location -> {
              URI uri = URI.create(location);
              allowGetObjectStatementBuilder.addResource(
                  IamResource.create(getS3UriWithArn(arnPrefix, uri)));
              String bucketArn = arnPrefix + getBucketName(uri);
              String rawPath = trimLeadingSlash(uri.getPath());
              bucketListStatmentBuilder
                  .computeIfAbsent(
                      bucketArn,
                      (String key) ->
                          IamStatement.builder()
                              .effect(IamEffect.ALLOW)
                              .addAction("s3:ListBucket")
                              .addResource(key))
                  .addConditions(
                      IamConditionOperator.STRING_LIKE,
                      "s3:prefix",
                      Arrays.asList(
                          // Get raw path metadata information for AWS hadoop connector
                          rawPath,
                          // Listing objects in raw path
                          concatPathWithSep(rawPath, "*", "/")));
              bucketGetLocationStatmentBuilder.computeIfAbsent(
                  bucketArn,
                  key ->
                      IamStatement.builder()
                          .effect(IamEffect.ALLOW)
                          .addAction("s3:GetBucketLocation")
                          .addResource(key));
            });

    if (!writeLocations.isEmpty()) {
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
    if (!bucketListStatmentBuilder.isEmpty()) {
      bucketListStatmentBuilder
          .values()
          .forEach(statementBuilder -> policyBuilder.addStatement(statementBuilder.build()));
    } else {
      // add list privilege with 0 resources
      policyBuilder.addStatement(
          IamStatement.builder().effect(IamEffect.ALLOW).addAction("s3:ListBucket").build());
    }

    bucketGetLocationStatmentBuilder
        .values()
        .forEach(statementBuilder -> policyBuilder.addStatement(statementBuilder.build()));
    return policyBuilder.addStatement(allowGetObjectStatementBuilder.build()).build();
  }

  private String getS3UriWithArn(String arnPrefix, URI uri) {
    return arnPrefix + concatPathWithSep(removeSchemaFromS3Uri(uri), "*", "/");
  }

  private String getArnPrefix(String roleArn) {
    if (roleArn.contains("aws-cn")) {
      return "arn:aws-cn:s3:::";
    } else if (roleArn.contains("aws-us-gov")) {
      return "arn:aws-us-gov:s3:::";
    } else {
      return "arn:aws:s3:::";
    }
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

  // Transform 's3://bucket/path' to /bucket/path
  private static String removeSchemaFromS3Uri(URI uri) {
    String bucket = uri.getHost();
    String path = trimLeadingSlash(uri.getPath());
    return String.join(
        "/", Stream.of(bucket, path).filter(Objects::nonNull).toArray(String[]::new));
  }

  private static String trimLeadingSlash(String path) {
    if (path.startsWith("/")) {
      path = path.substring(1);
    }
    return path;
  }

  private static String getBucketName(URI uri) {
    return uri.getHost();
  }

  private Credentials createS3TokenWithWebIdentity(
      String roleArn, Set<String> readLocations, Set<String> writeLocations, String userName) {
    IamPolicy policy = createPolicy(roleArn, readLocations, writeLocations);

    // Get the web identity token from the IRSA provider
    String webIdentityToken = System.getenv("AWS_WEB_IDENTITY_TOKEN_FILE");
    if (StringUtils.isBlank(webIdentityToken)) {
      throw new IllegalStateException(
          "AWS_WEB_IDENTITY_TOKEN_FILE environment variable is not set. "
              + "Ensure IRSA is properly configured in your EKS cluster.");
    }

    try {
      String tokenContent =
          new String(Files.readAllBytes(Paths.get(webIdentityToken)), StandardCharsets.UTF_8);

      AssumeRoleWithWebIdentityRequest.Builder builder =
          AssumeRoleWithWebIdentityRequest.builder()
              .roleArn(roleArn)
              .roleSessionName("gravitino_irsa_" + userName)
              .durationSeconds(tokenExpireSecs)
              .webIdentityToken(tokenContent)
              .policy(policy.toJson());

      AssumeRoleWithWebIdentityResponse response =
          stsClient.assumeRoleWithWebIdentity(builder.build());
      return response.credentials();
    } catch (Exception e) {
      throw new RuntimeException("Failed to read web identity token or assume role", e);
    }
  }
}
