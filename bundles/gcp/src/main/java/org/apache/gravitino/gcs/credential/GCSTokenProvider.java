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

package org.apache.gravitino.gcs.credential;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.CredentialAccessBoundary;
import com.google.auth.oauth2.CredentialAccessBoundary.AccessBoundaryRule;
import com.google.auth.oauth2.DownscopedCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialContext;
import org.apache.gravitino.credential.CredentialProvider;
import org.apache.gravitino.credential.GCSTokenCredential;
import org.apache.gravitino.credential.PathBasedCredentialContext;
import org.apache.gravitino.credential.config.GCSCredentialConfig;

/** Generate GCS access token according to the read and write paths. */
public class GCSTokenProvider implements CredentialProvider {

  private static final String INITIAL_SCOPE = "https://www.googleapis.com/auth/cloud-platform";

  private GoogleCredentials sourceCredentials;

  @Override
  public void initialize(Map<String, String> properties) {
    GCSCredentialConfig gcsCredentialConfig = new GCSCredentialConfig(properties);
    try {
      this.sourceCredentials =
          getSourceCredentials(gcsCredentialConfig).createScoped(INITIAL_SCOPE);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {}

  @Override
  public String credentialType() {
    return GCSTokenCredential.GCS_TOKEN_CREDENTIAL_TYPE;
  }

  @Override
  public Credential getCredential(CredentialContext context) {
    if (!(context instanceof PathBasedCredentialContext)) {
      return null;
    }
    PathBasedCredentialContext pathBasedCredentialContext = (PathBasedCredentialContext) context;
    try {
      AccessToken accessToken =
          getToken(
              pathBasedCredentialContext.getReadPaths(),
              pathBasedCredentialContext.getWritePaths());
      String tokenValue = accessToken.getTokenValue();
      long expireTime = accessToken.getExpirationTime().toInstant().toEpochMilli();
      return new GCSTokenCredential(tokenValue, expireTime);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private AccessToken getToken(Set<String> readLocations, Set<String> writeLocations)
      throws IOException {
    DownscopedCredentials downscopedCredentials =
        DownscopedCredentials.newBuilder()
            .setSourceCredential(sourceCredentials)
            .setCredentialAccessBoundary(getAccessBoundary(readLocations, writeLocations))
            .build();
    return downscopedCredentials.refreshAccessToken();
  }

  private List<String> getReadExpressions(String bucketName, String resourcePath) {
    List<String> readExpressions = new ArrayList<>();
    readExpressions.add(
        String.format(
            "resource.name.startsWith('projects/_/buckets/%s/objects/%s')",
            bucketName, resourcePath));
    getAllResources(resourcePath)
        .forEach(
            parentResourcePath ->
                readExpressions.add(
                    String.format(
                        "resource.name == 'projects/_/buckets/%s/objects/%s'",
                        bucketName, parentResourcePath)));
    return readExpressions;
  }

  @VisibleForTesting
  // "a/b/c" will get ["a", "a/", "a/b", "a/b/", "a/b/c"]
  static List<String> getAllResources(String resourcePath) {
    if (resourcePath.endsWith("/")) {
      resourcePath = resourcePath.substring(0, resourcePath.length() - 1);
    }
    if (resourcePath.isEmpty()) {
      return Arrays.asList("");
    }
    Preconditions.checkArgument(
        !resourcePath.startsWith("/"), resourcePath + " should not start with /");
    List<String> parts = Arrays.asList(resourcePath.split("/"));
    List<String> results = new ArrayList<>();
    String parent = "";
    for (int i = 0; i < parts.size() - 1; i++) {
      results.add(parts.get(i));
      parent += parts.get(i) + "/";
      results.add(parent);
    }
    results.add(parent + parts.get(parts.size() - 1));
    return results;
  }

  @VisibleForTesting
  // Remove the first '/', and append `/` if the path does not end with '/'.
  static String normalizeUriPath(String resourcePath) {
    if (resourcePath.isEmpty() || "/".equals(resourcePath)) {
      return "";
    }
    if (resourcePath.startsWith("/")) {
      resourcePath = resourcePath.substring(1);
    }
    if (resourcePath.endsWith("/")) {
      return resourcePath;
    }
    return resourcePath + "/";
  }

  private CredentialAccessBoundary getAccessBoundary(
      Set<String> readLocations, Set<String> writeLocations) {
    // bucketName -> read resource expressions
    Map<String, List<String>> readExpressions = new HashMap<>();
    // bucketName -> write resource expressions
    Map<String, List<String>> writeExpressions = new HashMap<>();

    // Construct read and write resource expressions
    HashSet<String> readBuckets = new HashSet<>();
    HashSet<String> writeBuckets = new HashSet<>();
    Stream.concat(readLocations.stream(), writeLocations.stream())
        .distinct()
        .forEach(
            location -> {
              URI uri = URI.create(location);
              String bucketName = getBucketName(uri);
              readBuckets.add(bucketName);
              String resourcePath = normalizeUriPath(uri.getPath());
              List<String> resourceExpressions =
                  readExpressions.computeIfAbsent(bucketName, key -> new ArrayList<>());
              // add read privilege
              resourceExpressions.addAll(getReadExpressions(bucketName, resourcePath));
              // add list privilege
              resourceExpressions.add(
                  String.format(
                      "api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith('%s')",
                      resourcePath));
              if (writeLocations.contains(location)) {
                writeBuckets.add(bucketName);
                resourceExpressions =
                    writeExpressions.computeIfAbsent(bucketName, key -> new ArrayList<>());
                // add write privilege
                resourceExpressions.add(
                    String.format(
                        "resource.name.startsWith('projects/_/buckets/%s/objects/%s')",
                        bucketName, resourcePath));
              }
            });

    // Construct policy according to the resource expression and privilege.
    CredentialAccessBoundary.Builder credentialAccessBoundaryBuilder =
        CredentialAccessBoundary.newBuilder();
    readBuckets.forEach(
        bucket -> {
          // Hadoop GCS connector needs storage.buckets.get permission, the reason why not use
          // inRole:roles/storage.legacyBucketReader is it provides extra list permission.
          AccessBoundaryRule bucketInfoRule =
              AccessBoundaryRule.newBuilder()
                  .setAvailableResource(toGCSBucketResource(bucket))
                  .setAvailablePermissions(
                      Arrays.asList("inRole:roles/storage.insightsCollectorService"))
                  .build();
          credentialAccessBoundaryBuilder.addRule(bucketInfoRule);
          List<String> readConditions = readExpressions.get(bucket);
          AccessBoundaryRule rule =
              getAccessBoundaryRule(
                  bucket, readConditions, Arrays.asList("inRole:roles/storage.objectViewer"));
          if (rule == null) {
            return;
          }
          credentialAccessBoundaryBuilder.addRule(rule);
        });

    writeBuckets.forEach(
        bucket -> {
          List<String> writeConditions = writeExpressions.get(bucket);
          AccessBoundaryRule rule =
              getAccessBoundaryRule(
                  bucket,
                  writeConditions,
                  Arrays.asList("inRole:roles/storage.legacyBucketWriter"));
          if (rule == null) {
            return;
          }
          credentialAccessBoundaryBuilder.addRule(rule);
        });

    return credentialAccessBoundaryBuilder.build();
  }

  private AccessBoundaryRule getAccessBoundaryRule(
      String bucketName, List<String> resourceExpression, List<String> permissions) {
    if (resourceExpression == null || resourceExpression.isEmpty()) {
      return null;
    }
    CredentialAccessBoundary.AccessBoundaryRule.Builder builder =
        CredentialAccessBoundary.AccessBoundaryRule.newBuilder();
    builder.setAvailableResource(toGCSBucketResource(bucketName));
    builder.setAvailabilityCondition(
        CredentialAccessBoundary.AccessBoundaryRule.AvailabilityCondition.newBuilder()
            .setExpression(String.join(" || ", resourceExpression))
            .build());
    builder.setAvailablePermissions(permissions);
    return builder.build();
  }

  private static String toGCSBucketResource(String bucketName) {
    return "//storage.googleapis.com/projects/_/buckets/" + bucketName;
  }

  private static String getBucketName(URI uri) {
    return uri.getHost();
  }

  private GoogleCredentials getSourceCredentials(GCSCredentialConfig gcsCredentialConfig)
      throws IOException {
    String gcsCredentialFilePath = gcsCredentialConfig.gcsCredentialFilePath();
    if (StringUtils.isBlank(gcsCredentialFilePath)) {
      return GoogleCredentials.getApplicationDefault();
    }
    Path credentialsFilePath = Paths.get(gcsCredentialFilePath);
    try (InputStream fileInputStream = Files.newInputStream(credentialsFilePath)) {
      return GoogleCredentials.fromStream(fileInputStream);
    } catch (NoSuchFileException e) {
      throw new IOException("GCS credential file does not exist." + gcsCredentialFilePath, e);
    }
  }
}
