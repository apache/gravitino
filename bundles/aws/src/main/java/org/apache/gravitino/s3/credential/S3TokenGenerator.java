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
import org.apache.gravitino.credential.CredentialContext;
import org.apache.gravitino.credential.CredentialGenerator;
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

/** Generate S3 token credentials according to the read and write paths. */
public class S3TokenGenerator implements CredentialGenerator<S3TokenCredential> {

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
  public S3TokenCredential generate(CredentialContext context) {
    if (!(context instanceof PathBasedCredentialContext)) {
      return null;
    }

    PathBasedCredentialContext pathContext = (PathBasedCredentialContext) context;

    Credentials s3Token =
        createS3Token(
            pathContext.getReadPaths(), pathContext.getWritePaths(), pathContext.getUserName());

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

    if (StringUtils.isNotBlank(s3CredentialConfig.region())) {
      builder.region(Region.of(s3CredentialConfig.region()));
    }
    if (StringUtils.isNotBlank(s3CredentialConfig.stsEndpoint())) {
      builder.endpointOverride(URI.create(s3CredentialConfig.stsEndpoint()));
    }
    return builder.build();
  }

  private Credentials createS3Token(
      Set<String> readLocations, Set<String> writeLocations, String userName) {
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

  private IamPolicy createPolicy(
      String roleArn, Set<String> readLocations, Set<String> writeLocations) {
    IamPolicy.Builder policyBuilder = IamPolicy.builder();
    IamStatement.Builder allowGetObjectStatementBuilder =
        IamStatement.builder()
            .effect(IamEffect.ALLOW)
            .addAction("s3:GetObject")
            .addAction("s3:GetObjectVersion");
    Map<String, IamStatement.Builder> bucketListStatementBuilder = new HashMap<>();
    Map<String, IamStatement.Builder> bucketGetLocationStatementBuilder = new HashMap<>();

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

    bucketListStatementBuilder
        .values()
        .forEach(builder -> policyBuilder.addStatement(builder.build()));
    bucketGetLocationStatementBuilder
        .values()
        .forEach(builder -> policyBuilder.addStatement(builder.build()));
    policyBuilder.addStatement(allowGetObjectStatementBuilder.build());

    return policyBuilder.build();
  }

  private String getS3UriWithArn(String arnPrefix, URI uri) {
    return arnPrefix + addWildcardToPath(removeSchemaFromS3Uri(uri));
  }

  private String getArnPrefix(String roleArn) {
    if (roleArn.contains("aws-cn")) {
      return "arn:aws-cn:s3:::";
    } else if (roleArn.contains("aws-us-gov")) {
      return "arn:aws-us-gov:s3:::";
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

  @Override
  public void close() throws IOException {
    if (stsClient != null) {
      stsClient.close();
    }
  }
}
