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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialContext;
import org.apache.gravitino.credential.CredentialProvider;
import org.apache.gravitino.credential.PathBasedCredentialContext;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.credential.config.S3CredentialConfig;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
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

/** Generates S3 token to access S3 data. */
public class S3TokenProvider implements CredentialProvider {
  private StsClient stsClient;
  private String roleArn;
  private String externalID;
  private int tokenExpireSecs;

  @Override
  public void initialize(Map<String, String> properties) {
    S3CredentialConfig s3CredentialConfig = new S3CredentialConfig(properties);
    this.roleArn = s3CredentialConfig.s3RoleArn();
    this.externalID = s3CredentialConfig.externalID();
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
    return S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE;
  }

  @Override
  public Credential getCredential(CredentialContext context) {
    if (!(context instanceof PathBasedCredentialContext)) {
      return null;
    }
    PathBasedCredentialContext pathBasedCredentialContext = (PathBasedCredentialContext) context;
    Credentials s3Token =
        createS3Token(
            roleArn,
            pathBasedCredentialContext.getReadPaths(),
            pathBasedCredentialContext.getWritePaths(),
            pathBasedCredentialContext.getUserName());
    return new S3TokenCredential(
        s3Token.accessKeyId(),
        s3Token.secretAccessKey(),
        s3Token.sessionToken(),
        s3Token.expiration().toEpochMilli());
  }

  private StsClient createStsClient(S3CredentialConfig s3CredentialConfig) {
    AwsCredentialsProvider credentialsProvider =
        StaticCredentialsProvider.create(
            AwsBasicCredentials.create(
                s3CredentialConfig.accessKeyID(), s3CredentialConfig.secretAccessKey()));
    StsClientBuilder builder = StsClient.builder().credentialsProvider(credentialsProvider);
    String region = s3CredentialConfig.region();
    if (StringUtils.isNotBlank(region)) {
      builder.region(Region.of(region));
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
              bucketListStatmentBuilder
                  .computeIfAbsent(
                      bucketArn,
                      (String key) ->
                          IamStatement.builder()
                              .effect(IamEffect.ALLOW)
                              .addAction("s3:ListBucket")
                              .addResource(key))
                  .addCondition(
                      IamConditionOperator.STRING_LIKE,
                      "s3:prefix",
                      concatPathWithSep(trimLeadingSlash(uri.getPath()), "*", "/"));
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

  private Credentials createS3Token(
      String roleArn, Set<String> readLocations, Set<String> writeLocations, String userName) {
    IamPolicy policy = createPolicy(roleArn, readLocations, writeLocations);
    AssumeRoleRequest.Builder builder =
        AssumeRoleRequest.builder()
            .roleArn(roleArn)
            .roleSessionName("gravitino_" + userName)
            .durationSeconds(tokenExpireSecs)
            .policy(policy.toJson());
    if (StringUtils.isNotBlank(externalID)) {
      builder.externalId(externalID);
    }
    AssumeRoleResponse response = stsClient.assumeRole(builder.build());
    return response.credentials();
  }
}
